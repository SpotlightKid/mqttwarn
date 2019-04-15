# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime
from functools import lru_cache
from inspect import isclass

import paho.mqtt.client as paho
import six
import stopit

from .context import RuntimeContext
from .cron import PeriodicThread
from .util import Struct, is_funcspec, load_function

try:
    import json
except ImportError:
    import simplejson as json

try:
    import queue
except ImportError:
    import Queue as queue

HAVE_JINJA = True
try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    HAVE_JINJA = False
else:
    jenv = Environment(loader=FileSystemLoader('templates/', encoding='utf-8'), trim_blocks=True)
    jenv.filters['jsonify'] = json.dumps


log = logging.getLogger(__name__)

# lwt values - may make these configurable later?
LWTALIVE = "1"
LWTDEAD = "0"

# Name of calling program
SCRIPTNAME = 'mqttwarn'

# Global runtime context object
context = None

# Global configuration object
cf = None

# Global handle to MQTT client
mqttc = None

# Initialize processor queue
jobq = queue.Queue(maxsize=0)
exit_flag = False

# Instances of PeriodicThread objects
ptlist = {}

# Instances of loaded service plugins
service_plugins = {}

# Collection of static configuration data for each subscribed topic
topichandlers = {}


# Class with helper functions which is passed to each plugin
# and its global instantiation
class Service(object):
    def __init__(self, mqttc, logger):
        # Reference to MQTT client object
        self.mqttc = mqttc

        # Reference to logging object
        self.log = log

        # Name of self ("mqttwarn", mostly)
        self.SCRIPTNAME = SCRIPTNAME


class Job(object):
    def __init__(self, prio, service, target, handler, msg, data):
        self.data = data
        self.handler = handler
        self.msg = msg
        self.prio = prio
        self.service = service
        self.target = target
        log.debug("New '%s:%s' job for topic '%s'.", service['name'], target, msg.topic)

    def __cmp__(self, other):
        return ((self.prio > other.prio) - (self.prio < other.prio))


class TopicHandler(object):
    def __init__(self, section, subscription, targets, config):
        self.section = section
        self.subscription = subscription
        self.config = config
        self.targets = targets

    def __repr__(self):
        return "<TopicHandler('%s')>" % self.section

    @property
    def qos(self):
        return self.config.getint(self.section, 'qos', fallback=0)

    @lru_cache()
    def filter(self, topic, payload):
        if not hasattr(self, '_filter'):
            _filter = self.config.get(self.section, 'filter', fallback=None)

            if is_funcspec(_filter):
                dottedpath, funcname = _filter.rstrip('()').split(':', 1)

                try:
                    self._filter = load_function(dottedpath, funcname)
                except Exception as exc:
                    log.warn("Could not import filter function '%s' from topic handler '%s': %s",
                             funcname, self.section, exc)
            else:
                self._filter = None

        if self._filter:
            try:
                return self._filter(topic, payload)
            except Exception as exc:
                log.warn("Error invoking filter function for topic handler '%s': %s",
                         self.section, exc)

    def decode_payload(self, msg):
        """Decode message payload through transformation machinery."""
        data = msg.data().copy()

        # Attempt to decode the payload as JSON. If payload decodes to a
        # dictionary, update transformation data dict with it.
        try:
            payload_data = msg.json()
        except Exception as exc:
            log.debug("Cannot decode payload=%r as JSON: %s", msg.payload, exc)
        else:
            if isinstance(payload_data, dict):
                data.update(payload_data)

        # If the topic handler section has a ``datamap`` option, which is set
        # to an importable modulepath/function, it is called with the message
        # topic and the global transformation data as positional arguments.
        # The function may update the transformation data dictionary.
        # The return value is ignored.
        self.xform('datamap', msg.topic, data)

        return data

    def xform(self, field, value, data):
        """Attempt transformation on value using ``value`` of handler section
        option named by ``field`` as formatter.

        If formatter is a dictionary, ``data`` is ignored and ``value`` is
        used as a key to look up the transformed value in it, which is
        returned by this function. If no matching key is present in formatter,
        ``value`` is returned unchanged.

        If formatter is a function, ``value`` and the transformation ``data``
        dict are passed to it as positional arguments. Its return value is
        returned by this function as the transformed value.

        If formatter is a format string, it is formatted with standard
        string formatting (i.e. the ``format`` string method) and ``value``
        is passed as the first and only optional argument to ``format``
        and the ``data`` transformation dict as keyword arguments.

        """
        if value is None:
            return None

        try:
            formatter = self.config.getdict(self.section, field, fallback=None)
        except TypeError:
            pass
        else:
            return formatter.get(value, value)

        formatter = self.config.get(self.section, field, fallback=None)

        if is_funcspec(formatter):
            dottedpath, funcname = formatter.rstrip('()').split(':', 1)

            try:
                func = load_function(dottedpath, funcname)
            except Exception as exc:
                log.warn("Could not import '%s' function '%s' from topic handler '%s': %s",
                         field, funcname, self.section, exc)
            try:
                log.debug("Xform value with '%s' function '%s:%s'", field, dottedpath, funcname)
                return func(value, data)
            except Exception as exc:
                log.warn("Error invoking '%s' function '%s' defined in '%s': %s",
                         field, funcname, self.section, exc)
        elif formatter:
            try:
                value = formatter.format(value, **data)
            except Exception as exc:
                log.warning("Cannot format value: %s", exc)

        if isinstance(value, six.string_types):
            value = value.replace("\\n", "\n")

        return value


class MQTTMessageWrapper(object):
    __slots__ = ('_data', '_decoded', '_json', 'msg')

    def __init__(self, msg):
        self.msg = msg

    def __getattr__(self, name):
        return getattr(self.msg, name)

    def json(self):
        if not hasattr(self, '_json'):
            self._json = json.loads(self.payload.rstrip(b'\0'))

        return self._json

    def payload_string(self, encoding='utf-8', errors='ignore'):
        if not hasattr(self, '_decoded'):
            self._decoded = {}

        if encoding not in self._decoded:
            self._decoded[encoding] = self.payload.decode(encoding, errors)

        return self._decoded[encoding]

    def data(self, encoding='utf-8'):
        """Return a dict with standard transformation data available to all plugins."""
        if not hasattr(self, '_data'):
            dt = datetime.utcnow()
            lt = datetime.now()
            self._data = {
                'topic': self.msg.topic,
                'raw_payload': self.msg.payload,
                'payload': self.payload_string(encoding),
                # datetime.datetime instance for UTC
                '_dt': dt,
                # datetime.datetime instance for local time
                '_lt': lt,
                # Unix timestamp in seconds since the epoch
                '_dtepoch': dt.timestamp(),
                # UTC timestamp, e.g. 2014-02-17T10:38:43.910691Z
                '_dtiso': dt.isoformat(),
                # Local time in iso format
                '_ltiso': lt.isoformat(),
                # Local time in hours and minutes, e.g. 10:16
                '_lthhmm': lt.strftime('%H:%M'),
                # Local time in hours, minutes and seconds, e.g. 10:16:21
                '_lthhmmss': lt.strftime('%H:%M:%S')
            }

        return self._data


# MQTT broker callbacks
def on_connect(mosq, userdata, flags, result_code):
    """Handle connections (or failures) to the broker.

    This is called after the client has received a CONNACK message
    from the broker in response to calling connect().

    The result_code is one of:
    0: Success
    1: Refused - unacceptable protocol version
    2: Refused - identifier rejected
    3: Refused - server unavailable
    4: Refused - bad user name or password (MQTT v3.1 broker only)
    5: Refused - not authorised (MQTT v3.1 broker only)

    """
    try:
        if result_code == 0:
            log.info("Connected to MQTT broker. Subscribing to topics...")

            if not cf.clean_session:
                log.debug("clean_session==False; previous subscriptions for client_id '%s' remain "
                          "active on broker.", cf.client_id)

            subscribed = set()
            for handler in topichandlers.values():
                topic = handler.subscription
                qos = handler.qos

                if topic in subscribed:
                    continue

                log.debug("Subscribing to %s (qos=%d).", topic, qos)
                mqttc.subscribe(topic, qos)
                subscribed.add(topic)

            if cf.lwt is not None:
                mqttc.publish(cf.lwt, LWTALIVE, qos=0, retain=True)
        elif result_code == 1:
            log.error("Connection refused - unacceptable protocol version.")
        elif result_code == 2:
            log.error("Connection refused - identifier rejected.")
        elif result_code == 3:
            log.error("Connection refused - server unavailable.")
        elif result_code == 4:
            log.error("Connection refused - bad user name or password.")
        elif result_code == 5:
            log.error("Connection refused - not authorised.")
        else:
            log.error("Connection failed - result code %d.", result_code)
    except Exception as exc:
        log.exception("Error in 'on_connect' callback: %s", exc)
        cleanup(1)


def on_disconnect(mosq, userdata, result_code):
    """Handle disconnections from the broker."""
    try:
        if result_code == 0:
            log.info("Clean disconnection from broker.")
        else:
            send_failover("brokerdisconnected",
                          "Broker connection lost. Will attempt to reconnect in 5s...")
            time.sleep(5)
    except Exception as exc:
        log.exception("Error in 'on_disconnect' callback: %s", exc)


def on_message(mosq, userdata, msg):
    """Handle message received from the broker."""
    try:
        log.debug("Message received on topic '%s': %r", msg.topic, msg.payload)
        msg = MQTTMessageWrapper(msg)

        if msg.retain == 1:
            if cf.skipretained:
                log.debug("Skipping retained message on topic '%s'.", msg.topic)
                return

        log.debug("Checking handlers...")
        handlers = match_topic_handlers(msg.topic)
        log.debug("Matching handlers: %r", handlers)

        for handler in handlers:
            # Check for any message filters
            if handler.filter(msg.topic, msg.payload):
                log.debug("Filter in section [%s] has skipped message on topic '%s'.",
                          handler.section, msg.topic)
                continue

            # Send the message to any targets specified
            log.debug("Passing message on topic '%s' to handler [%s].", msg.topic, handler.section)
            send_to_targets(handler, msg)
    except Exception as exc:
        log.exception("Error in 'on_message' callback: %s", exc)


# End of MQTT broker callbacks


def make_service(mqttc=None, name=None):
    """Service object factory.

    Prepare service object for plugin.
    Inject appropriate MQTT client and logger objects.

    :param mqttc: Instance of PAHO MQTT client object.
    :param name:  Name used for obtaining a logger instance.
    :return:      Service object ready for being passed to plugin instance.

    """
    name = name or 'unknown'
    logger = logging.getLogger(name)
    service = Service(mqttc, logger)
    return service


@lru_cache()
def match_topic_handlers(topic):
    """Return list of matching handlers for given topic."""
    handlers = []
    for subscription, handler in topichandlers.items():
        if paho.topic_matches_sub(subscription, topic):
            log.debug("Section [%s] matches message on topic '%s'.", handler.section, topic)
            handlers.append(handler)

    return handlers


def render_template(filename, data):
    if HAVE_JINJA:
        template = jenv.get_template(filename)
        return template.render(data)


def send_failover(reason, message):
    # Make sure we dump this event to the log
    log.warn(message)
    # Attempt to send the message to our failover targets
    if 'failover' in topichandlers:
        # create fake MQTTMessage
        msg = MQTTMessageWrapper(msg=Struct(topic=reason, payload=message))
        send_to_targets(topichandlers['failover'], msg)


def send_to_targets(handler, msg):
    section = handler.section
    topic = msg.topic
    payload = msg.payload

    # Decode raw payload into transformation data
    data = handler.decode_payload(msg)

    if callable(handler.targets):
        targetlist = handler.targets(section, topic, data)
    elif isinstance(handler.targets, dict):
        # XXX: Refactor this whole block into a separate function

        def get_key(item):
            # Prefix a key with the number of topic levels and then use reverse alphabetic ordering
            # '+' is after '#' in ascii table
            # Caveat: space is allowed in topic name but will be less specific than '+', '#'
            # so replace '#' with first ascii character and '+' with second ascii character
            # http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a

            # item[0] represents topic. Replace wildcard characters to ensure the right order
            modified_topic = item[0].replace('#', chr(0x01)).replace('+', chr(0x02))
            levels = len(item[0].split('/'))
            # Concatenate levels with leading zeros and modified topic and return as a key
            return "{:03d}{}".format(levels, modified_topic)

        # Produce a sorted list of topic/targets with longest and more specific first
        sorted_dispatcher = sorted(handler.targets.items(), key=get_key, reverse=True)

        for match_topic, targets in sorted_dispatcher:
            if paho.topic_matches_sub(match_topic, topic):
                # hocus pocus, let targets become a list
                targetlist = targets if isinstance(targets, list) else [targets]
                log.debug("Most specific match '%s' dispatched to '%s'.", match_topic, targets)
                # first most specific topic matches then stops processing
                break
        else:
            # Not found then no action. This could be configured intentionally.
            log.debug("Dispatcher definition does not contain matching topic/target pair in "
                      "section [%s].", section)
            return
    else:
        targetlist = handler.targets

    if not isinstance(targetlist, (tuple, list)):
        log.error("Invalid targets definition in section [%s]: %r not a list or tuple.",
                  section, targetlist)
        return

    # Interpolate transformation data values into topic targets.
    # Be graceful if interpolation fails, but log a meaningful message.
    targetlist_transformed = []

    for service, target in targetlist:
        try:
            target = target.format(**data)
            targetlist_transformed.append((service, target))
        except Exception as exc:
            log.exception("Cannot interpolate transformation data into topic handler target '%s' "
                          "of section '%s': %s", target, section, exc)
            log.debug("topic=%s, payload=%r, data=%r", topic, payload, data)

    targetlist = targetlist_transformed
    log.debug("Final target list for topic '%s': %r", topic, targetlist)

    for service, target in targetlist:
        # By now, each target in targetlist is a two-element tuple (service, target)
        # If target is None or emptys, then notify *all* targets of service

        # skip targets with invalid services
        service_inst = service_plugins.get(service)
        if service_inst is None:
            log.error("Invalid configuration: topic handler '%s' points to non-existing service "
                      "'%s'.", topic, service)
            continue

        if target and target not in service_inst['targets']:
            log.error("Invalid configuration: topic handler '%s' points to non-existing "
                      "target '%s' in service '%s'.", section, target, service)
            continue

        for target in (target,) if target else tuple(service_inst['targets']):
            log.debug("Message on topic '%s' routed to service '%s:%s'.", topic, service, target)
            job = Job(prio=1, service=service_inst, target=target, handler=handler, msg=msg,
                      data=data)
            jobq.put(job)


def processor(jobq, worker_id=None, job_timeout=10):
    """Queue runner.

    Pull a job from the queue, find the module in charge
    of handling the service, and invoke the module's plugin to do so.

    """
    while not exit_flag:
        log.debug('Job queue has %s items to process.', jobq.qsize())
        job = jobq.get()

        if job is None:
            break

        service = job.service['name']
        handler = job.handler
        target = job.target
        topic = job.msg.topic
        log.debug("Processor #%s is handling '%s:%s'.", worker_id, service, target)

        data = job.data.copy()
        # It's mportant to keep order of the following three calls, since they
        # all may alter the data dict.
        title = handler.xform('title', SCRIPTNAME, data)
        image = handler.xform('image', '', data)
        message = handler.xform('format', data['payload'], data)

        item = Struct(
            addrs=job.service['targets'][target],
            config=job.service['config'],
            data=data,
            image=image,
            message=message,
            payload=job.msg.payload,
            section=handler.section,
            service=service,
            target=target,
            title=title,
            topic=topic
        )

        try:
            item.priority = int(handler.xform('priority', 0, data))
        except Exception:
            item.priority = 0
            log.debug("Failed to determine the priority, defaulting to zero.")

        template = context.get_config(handler.section, 'template')

        if template is not None:
            if HAVE_JINJA:
                try:
                    text = render_template(template, data)

                    if text is not None:
                        item.message = text
                except Exception as exc:
                    log.warn("Cannot render template '%s': %s", template, exc)
            else:
                log.warn("Templating not possible because Jinja2 is not installed.")

        if item.message or isinstance(item.message, (float, int)):
            # Run the plugin in a separate thread and kill it if it doesn't return in time
            with stopit.ThreadingTimeout(job_timeout):
                try:
                    result = job.service['plugin'](job.service['srv'], item)
                except stopit.TimeoutException:
                    log.warn("Service '%s:%s' for topic '%s' cancelled after %is timeout.",
                             service, target, topic, job_timeout)
                except Exception as exc:
                    log.error("Error invoking service '%s:%s' for topic '%s': exc",
                              service, target, topic, exc)
                else:
                    if isinstance(result, six.string_types):
                        log.info("Service '%s:%s' for topic '%s' result: %s",
                                 service, target, topic, result)
                    elif not result:
                        log.warn("Service '%s:%s' for topic '%s' failed.", service, target, topic)
        else:
            log.warn("Notification of '%s' for '%s' suppressed: empty message.", service, topic)

        jobq.task_done()

    log.debug("Worker thread #%s exiting...", worker_id)


def load_services(services, mqttc):
    for service in services:
        service_config = context.get_service_config(service)

        if service_config is None:
            log.error("Skipping service '%s' with missing config section.", service)
            continue

        service_targets = context.get_service_targets(service)

        if service_targets is None:
            log.error("Skipping service '%s' with no valid targets.", service)
            continue

        modname = context.get_service_module(service)

        extra_pkgs = [] if '.' in modname[1:] else ['mqttwarn.services']

        try:
            plugin_func = load_function(modname, 'plugin', extra_pkgs=extra_pkgs)
            service_logger_name = 'mqttwarn.services.{}'.format(service)
            srv = make_service(mqttc=mqttc, name=service_logger_name)

            if isclass(plugin_func):
                plugin_func = plugin_func(srv, service_config)
        except Exception as exc:
            log.exception("Unable to load plugin module '%s' for service '%s': %s",
                          modname, service, exc)
        else:
            log.info("Successfully loaded plugin module '%s' for service '%s'.", modname, service)
            service_plugins[service] = {
                'name': service,
                'config': service_config,
                'targets': service_targets,
                'plugin': plugin_func,
                'module': modname,
                'srv': srv,
            }


def load_topichandlers(services):
    log.debug("Loading topic handlers configuration...")

    for section in context.get_handler_sections():
        targets = context.get_handler_targets(section)
        service_found = False

        if callable(targets):
            service_found = True
        elif targets:
            if isinstance(targets, dict):
                for targetlist in targets.values():
                    for service, _ in targetlist:
                        if service in services:
                            service_found = True
            elif isinstance(targets, list):
                for service, _ in targets:
                    if service in services:
                        service_found = True

        if service_found:
            subscription = context.get_handler_topic(section)
            topichandlers[subscription] = TopicHandler(
                section=section,
                subscription=subscription,
                targets=targets,
                config=context.config
            )


def connect():
    """Load service plugins, connect to the broker, launch daemon threads and listen forever."""
    # FIXME: Remove global variables
    global mqttc

    services = cf.getlist('defaults', 'launch', fallback=[])

    if not services:
        msg = "No services configured. Aborting."
        log.error(msg)
        sys.exit(msg)

    try:
        os.chdir(cf.directory)
    except Exception as exc:
        msg = "Cannot chdir to %s: %s" % (cf.directory, exc)
        log.error(msg)
        sys.exit(msg)

    # Initialize MQTT broker connection
    mqttc = paho.Client(cf.client_id, clean_session=cf.clean_session, protocol=cf.protocol,
                        transport=cf.transport)

    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect

    # initialize service configurations
    load_services(services, mqttc)
    # and topic handler
    load_topichandlers(services)

    # check for authentication
    if cf.username:
        mqttc.username_pw_set(cf.username, cf.password)

    # set the lwt before connecting
    if cf.lwt is not None:
        log.debug("Setting Last Will and Testament to topic '%s', value %r.", cf.lwt, LWTDEAD)
        mqttc.will_set(cf.lwt, payload=LWTDEAD, qos=0, retain=True)

    # Delays will be: 3, 6, 12, 24, 30, 30, ...
    # mqttc.reconnect_delay_set(delay=3, delay_max=30, exponential_backoff=True)

    if cf.tls:
        mqttc.tls_set(cf.ca_certs, cf.certfile, cf.keyfile, tls_version=cf.tls_version)

    if cf.tls_insecure:
        mqttc.tls_insecure_set(True)

    try:
        log.debug("Attempting connection to MQTT broker %s:%s...", cf.hostname, cf.port)
        mqttc.connect(cf.hostname, int(cf.port), 60)
    except Exception as exc:
        msg = "Cannot connect to MQTT broker at %s:%s: %s" % (cf.hostname, cf.port, exc)
        log.exception(msg)
        sys.exit(msg)

    # Launch worker threads to operate on queue
    log.info('Starting %s worker threads...', cf.num_workers)
    for i in range(cf.num_workers):
        t = threading.Thread(target=processor, args=(jobq,), kwargs={'worker_id': i})
        t.daemon = True
        t.start()

    # If the config file has on ore more [cron:xxx] sections, these define
    # functions, which should be invoked periodically.
    #
    # Each section must have at least two options named 'target' and
    # 'interval'.
    #
    # The 'target' option specifies the function to run. The format of
    # of the value is the dotted package path of the module defining
    # the function and the function name sperated by a colon.
    #
    # The 'interval' option specifies the interval in seconds as an
    # integer or float at which the target function should be invoked.
    #
    # Additionally, the following options are recognized but optional:
    #
    # 'now' (bool, default: False) - whether to run the function
    #     immediately on startup
    #
    # Example section:
    #
    # [cron:publish_ip]
    # ; Define a function for publishing your public ip address to the MQTT bus each minute.
    # target = mymodule.customfuncs:publish_public_ip_address
    # interval = 60
    # now = false

    for section in cf.sections():
        if section.startswith('cron:'):
            name = section.split(':', 1)[1]

            if cf.has_option(section, 'target'):
                funcspec = cf.get(section, 'target')
            else:
                log.error("[cron:%s] section does not specify target function.", name)
                continue

            if cf.has_option(section, 'interval'):
                interval = cf.getfloat(section, 'interval')
            else:
                log.error("[cron:%s] section does not specify execution interval.", name)
                continue

            try:
                dottedpath, funcname = funcspec.rstrip('()').split(':', 1)
                func = load_function(dottedpath, funcname)
            except Exception as exc:
                log.error("[cron:%s] could not load function '%s': %s", name, funcspec, exc)
                continue

            now = cf.getboolean(section, 'now', fallback=False)
            log.debug("Scheduling function '%s' as periodic task to run every %s seconds via "
                      "[cron:%s] section.", funcname, interval, name)
            service = make_service(mqttc=mqttc, name='mqttwarn.cron.' + name)
            ptlist[name] = PeriodicThread(callback=func, period=interval, name=name, srv=service,
                                          now=now)
            ptlist[name].start()

    while not exit_flag:
        reconnect_interval = 5

        try:
            mqttc.loop_forever()
        except socket.error:
            pass
        # FIXME: add logging with trace for any other exceptions

        if not exit_flag:
            log.warning("MQTT server disconnected, trying to reconnect every %s seconds.",
                        reconnect_interval)
            time.sleep(reconnect_interval)


def cleanup(retcode=0, frame=None):
    """Signal handler to ensure we disconnect cleanly in the event of a SIGTERM or SIGINT."""
    for ptname in ptlist:
        log.debug("Cancelling %s timer...", ptname)
        ptlist[ptname].cancel()

    log.debug("Disconnecting from MQTT broker...")
    if cf.lwt is not None:
        mqttc.publish(cf.lwt, LWTDEAD, qos=0, retain=True)

    mqttc.loop_stop()
    mqttc.disconnect()

    log.info("Waiting for queue to drain...")
    jobq.join()

    # Send exit signal to subsystems _after_ queue was drained
    global exit_flag
    exit_flag = True

    if frame:
        log.debug("Exiting on signal %d.", retcode)
        sys.exit(0)
    else:
        log.debug("Exiting with return code %d.", retcode)
        sys.exit(retcode)


def bootstrap(config=None, scriptname=None):
    # FIXME: Remove global variables
    global context, cf, SCRIPTNAME
    context = RuntimeContext(config=config)
    cf = config
    SCRIPTNAME = scriptname


def run_plugin(config=None, name=None, data=None):
    """Run service plugins directly without the dispatching and transformation machinery.

    On the one hand, this might look like a bit of a hack.
    On the other hand, it shows very clearly how some of
    the innards of mqttwarn interact so it might also please
    newcomers as a "learning the guts of mqttwarn" example.

    :param config: The configuration object
    :param name:   The name of the service plugin, e.g. "log" or "file"
    :param data:   The data to be converged into an appropriate item Struct object instance

    """
    # Bootstrap mqttwarn core
    bootstrap(config=config)

    # Load designated service plugins
    load_services([name])
    service_logger_name = 'mqttwarn.services.{}'.format(name)
    srv = make_service(mqttc=None, name=service_logger_name)

    # Build a mimikry item instance for feeding to the service plugin
    item = Struct(**data)
    item.config = config
    item.service = name
    item.target = 'mqttwarn'
    item.data = {}        # FIXME

    # Launch plugin
    plugin = service_plugins[name]['plugin']
    response = plugin(srv, item)
    log.info('Plugin response: %r', response)
    return response
