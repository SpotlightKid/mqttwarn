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

import paho.mqtt.client as paho
import six

from .context import RuntimeContext
from .cron import PeriodicThread
from .util import Struct, is_funcspec, load_function, timeout

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


logger = logging.getLogger(__name__)

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
q_in = queue.Queue(maxsize=0)
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

        # Reference to all mqttwarn globals, for using its machinery from plugins
        self.mwcore = globals()

        # Reference to logging object
        self.logging = logger

        # Name of self ("mqttwarn", mostly)
        self.SCRIPTNAME = SCRIPTNAME


class Job(object):
    def __init__(self, prio, service, handler, msg, data, target):
        self.prio = prio
        self.service = service
        self.handler = handler
        self.msg = msg
        self.data = data
        self.target = target
        logger.debug("New '%s:%s' job: %s", service['name'], target, msg.topic)

    def __cmp__(self, other):
        return ((self.prio > other.prio) - (self.prio < other.prio))


class TopicHandler(object):
    def __init__(self, section, subscription, targets, config):
        self.section = section
        self.subscription = subscription
        self.config = config
        self.targets = targets

    @property
    def qos(self):
        return self.config.getint(self.section, 'qos', fallback=0)

    @lru_cache()
    def filter(self, msg):
        return False

    def decode_payload(self, msg):
        """Decode message payload through transformation machinery."""
        data = msg.data().copy()

        # Attempt to decode the payload from JSON. If payload decodes to a
        # dictionary, update transformation data dict with it.
        try:
            payload_data = msg.json()
        except Exception as exc:
            logger.debug("Cannot decode JSON object, payload=%s: %s", msg.payload, exc)
        else:
            if isinstance(payload_data, dict):
                data.update(payload_data)

        # If the topic handler section has an ``alldata`` option, which is set to
        # an importable modulepath/function, it is called with the handler section
        # name, the message topic and the global transformation_data.
        # It must return a dictionary, with which the transformation data dict is
        # then updated.
        all_data = self.xform(msg.topic, 'alldata', data)

        if all_data is not None and isinstance(all_data, dict):
            data.update(all_data)

        return data

    def xform(self, field, value, data):
        """Attempt transformation on value using value of handler section
        option named by field as formatter.

        The formatter, if it is a function, is passed the value and the
        transformation data dict and should. It's return value is returned
        directly.

        If formatter is a format string, its is formatted with standard
        string formatting (i.e. the ``format`` string method and the value
        is passed as the first and only opistional argument to ``format``
        and the transformation data dict as keyword arguments.

        """
        if value is None:
            return None

        formatter = self.config.get(self.section, field, fallback=None)

        if is_funcspec(formatter):
            dottedpath, funcname = formatter.split(':', 1)

            try:
                func = load_function(dottedpath, funcname)
            except Exception as exc:
                logger.warn("Could not import '%s' function '%s' from in topic handler '%s': %s",
                            field, funcname, self.section, exc)
            try:
                return func(value, data)
            except Exception as exc:
                logger.warn("Error invoking '%s' function '%s' defined in '%s': %s",
                            field, funcname, self.section, exc)
        elif formatter:
            try:
                value = formatter.format(value, **data)
            except Exception as exc:
                logger.warning("Cannot format value: %s", exc)

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

    def data(self):
        """Return a dict with standard transformation data available to all plugins."""
        if not hasattr(self, 'm_data'):
            dt = datetime.now()
            self._data = {
                'topic': self.msg.topic,
                'payload': self.msg.payload,
                # Unix timestamp in seconds since the epoch
                '_dtepoch': int(time.time()),
                # UTC timestamp, e.g. 2014-02-17T10:38:43.910691Z
                '_dtiso': datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                # local time in iso format
                '_ltiso': datetime.now().isoformat(),
                # Local time in hours and minutes, e.g. 10:16
                '_dthhmm': dt.strftime('%H:%M'),
                # Local time in hours, minutes and seconds, e.g. 10:16:21
                '_dthhmmss': dt.strftime('%H:%M:%S')
            }

        return self._data


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


def render_template(filename, data):
    if HAVE_JINJA:
        template = jenv.get_template(filename)
        return template.render(data)


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
    if result_code == 0:
        logger.debug("Connected to MQTT broker, subscribing to topics...")

        if not cf.clean_session:
            logger.debug("clean_session==False; previous subscriptions for client_id '%s' remain "
                         "active on broker", cf.client_id)

        subscribed = set()
        for handler in topichandlers.values():
            topic = handler.subscription
            qos = handler.qos

            if topic in subscribed:
                continue

            logger.debug("Subscribing to %s (qos=%d)", topic, qos)
            mqttc.subscribe(topic, qos)
            subscribed.add(topic)

        if cf.lwt is not None:
            mqttc.publish(cf.lwt, LWTALIVE, qos=0, retain=True)
    elif result_code == 1:
        logger.info("Connection refused - unacceptable protocol version")
    elif result_code == 2:
        logger.info("Connection refused - identifier rejected")
    elif result_code == 3:
        logger.info("Connection refused - server unavailable")
    elif result_code == 4:
        logger.info("Connection refused - bad user name or password")
    elif result_code == 5:
        logger.info("Connection refused - not authorised")
    else:
        logger.warning("Connection failed - result code %d", result_code)


def on_disconnect(mosq, userdata, result_code):
    """Handle disconnections from the broker."""
    if result_code == 0:
        logger.info("Clean disconnection from broker")
    else:
        send_failover("brokerdisconnected",
                      "Broker connection lost. Will attempt to reconnect in 5s...")
        time.sleep(5)


@lru_cache()
def match_topic_handlers(topic):
    for handler in topichandlers.values():
        if paho.topic_matches_sub(handler.subscription, topic):
            logger.debug("Section [%s] matches message on %s.", handler.section, topic)
            yield handler


def on_message(mosq, userdata, msg):
    """Handle message received from the broker."""
    logger.debug("Message received on %s: %r", msg.topic, msg.payload)
    msg = MQTTMessageWrapper(msg)

    if msg.retain == 1:
        if cf.skipretained:
            logger.debug("Skipping retained message on %s", msg.topic)
            return

    # Try to find matching handler for this topic
    for handler in match_topic_handlers(msg.topic):
        # Check for any message filters
        if handler.filter(msg):
            logger.debug("Filter in section [%s] has skipped message on topic '%s'",
                         handler.section, msg.topic)
            continue

        # Send the message to any targets specified
        send_to_targets(handler, msg)

# End of MQTT broker callbacks


def send_failover(reason, message):
    # Make sure we dump this event to the log
    logger.warn(message)
    # Attempt to send the message to our failover targets
    send_to_targets('failover', reason, message)


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
                logger.debug("Most specific match %s dispatched to %s", match_topic, targets)
                # first most specific topic matches then stops processing
                break
        else:
            # Not found then no action. This could be configured intentionally.
            logger.debug("Dispatcher definition does not contain matching topic/target pair in "
                         "section [%s]", section)
            return
    else:
        targetlist = handler.targets

    if not isinstance(targetlist, (tuple, list)):
        logger.error("Invalid targets definition in section [%s]: %r not a liust or tuple",
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
            logger.exception("Cannot interpolate transformation data into topic handler target "
                             "'%s' of section '%s': %s", target, section, exc)
            logger.debug("topic=%s, payload=%r, data=%r", topic, payload, data)

    targetlist = targetlist_transformed

    logger.debug("Final target list for topic '%s': %r", topic, targetlist)

    for service, target in targetlist:
        # By now, each target in targetlist is a two-element tuple (service, target)
        # If target is None or emptys, then notify *all* targets of service

        # skip targets with invalid services
        service_inst = service_plugins.get(service)
        if service_inst is None:
            logger.error("Invalid configuration: topic handler '%s' points to non-existing "
                         "service '%s'", topic, service)
            continue

        if target and target not in service_inst['targets']:
            logger.error("Invalid configuration: topic handler '%s' points to non-existing "
                         "target '%s' in service '%s'.", section, target, service)
            continue

        for target in (target,) if target else tuple(service_inst['targets']):
            logger.debug("Message on topic '%s' routed to service '%s:%s'", topic, service, target)
            job = Job(1, service_inst, handler, msg, data, target)
            q_in.put(job)


def processor(worker_id=None):
    """Queue runner.

    Pull a job from the queue, find the module in charge
    of handling the service, and invoke the module's plugin to do so.

    """
    conf = context.get_config

    while not exit_flag:
        logger.debug('Job queue has %s items to process', q_in.qsize())
        job = q_in.get()

        service = job.service['name']
        handler = job.handler
        target = job.target
        topic = job.msg.topic
        logger.debug("Processor #%s is handling '%s:%s'", worker_id, service, target)

        data = job.data.copy()
        item = {
            'service': service,
            'section': handler.section,
            'target': target,
            'config': job.service['config'],
            'addrs': job.service['targets'][target],
            'topic': topic,
            'payload': job.msg.payload,
            'data': data,
            'title': handler.xform('title', SCRIPTNAME, data),
            'image': handler.xform('image', '', data),
            'message': handler.xform('format', job.msg.payload_string(), data),
        }

        try:
            item['priority'] = int(handler.xform('priority', 0, data))
        except Exception as exc:
            item['priority'] = 0
            logger.debug("Failed to determine the priority, defaulting to zero: %s", exc)

        template = conf(handler.section, 'template')

        if template is not None:
            if HAVE_JINJA:
                try:
                    text = render_template(template, data)

                    if text is not None:
                        item['message'] = text
                except Exception as exc:
                    logger.warn("Cannot render '%s' template: %s", template, exc)
            else:
                logger.warn("Templating not possible because Jinja2 is not installed")

        if item['message'] or isinstance(item['message'], (float, int)):
            st = Struct(**item)
            notified = False

            try:
                # Run the plugin in a separate thread and kill it if it doesn't return in 10s
                plugin = job.service['plugin']
                service_logger_name = 'mqttwarn.services.{}'.format(service)
                srv = make_service(mqttc=mqttc, name=service_logger_name)
                notified = timeout(plugin, (srv, st))
            except Exception as exc:
                logger.error("Cannot invoke service for '%s': %s", service, exc)

            if not notified:
                logger.warn("Notification of '%s' for '%s' FAILED or TIMED OUT", service, topic)
        else:
            logger.warn("Notification of '%s' for '%s' suppressed: empty message", service, topic)

        q_in.task_done()

    logger.debug("Thread exiting...")


def load_services(services):
    for service in services:
        service_config = context.get_service_config(service)

        if service_config is None:
            logger.error("Skipping service '%s' with config section.", service)
            continue

        service_targets = context.get_service_targets(service)

        if service_targets is None:
            logger.error("Skipping service '%s' with no valid targets.", service)
            continue

        modname = context.get_service_module(service)

        extra_pkgs = [] if '.' in modname[1:] else ['mqttwarn.services']

        try:
            plugin_func = load_function(modname, 'plugin', extra_pkgs=extra_pkgs)
        except Exception as exc:
            logger.exception("Unable to load plugin module '%s' for service '%s': %s",
                             modname, service, exc)
        else:
            logger.info("Successfully loaded plugin module '%s' for service '%s'.",
                        modname, service)
            service_plugins[service] = {
                'name': service,
                'config': service_config,
                'targets': service_targets,
                'plugin': plugin_func,
                'module': modname,
            }


def load_topics(services):
    for section in context.get_handler_sections():
        targets = context.get_handler_targets(section)
        service_found = False

        if callable(targets):
            service_found = True
        elif isinstance(targets, list):
            for service, target in targets:
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
        logger.error("No services configured. Aborting")
        sys.exit(2)

    try:
        os.chdir(cf.directory)
    except Exception as exc:
        logger.error("Cannot chdir to %s: %s", cf.directory, exc)
        sys.exit(2)

    load_services(services)
    load_topics(services)

    # Initialize MQTT broker connection
    mqttc = paho.Client(cf.client_id, clean_session=cf.clean_session, protocol=cf.protocol,
                        transport=cf.transport)

    logger.debug("Attempting connection to MQTT broker %s:%s...", cf.hostname, cf.port)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_disconnect = on_disconnect

    # check for authentication
    if cf.username:
        mqttc.username_pw_set(cf.username, cf.password)

    # set the lwt before connecting
    if cf.lwt is not None:
        logger.debug("Setting Last Will and Testament to topic '%s', value %r", cf.lwt, LWTDEAD)
        mqttc.will_set(cf.lwt, payload=LWTDEAD, qos=0, retain=True)

    # Delays will be: 3, 6, 12, 24, 30, 30, ...
    # mqttc.reconnect_delay_set(delay=3, delay_max=30, exponential_backoff=True)

    if cf.tls:
        mqttc.tls_set(cf.ca_certs, cf.certfile, cf.keyfile, tls_version=cf.tls_version)

    if cf.tls_insecure:
        mqttc.tls_insecure_set(True)

    try:
        mqttc.connect(cf.hostname, int(cf.port), 60)
    except Exception as exc:
        logger.exception("Cannot connect to MQTT broker at %s:%s: %s", cf.hostname, cf.port, exc)
        sys.exit(2)

    # Launch worker threads to operate on queue
    logger.info('Starting %s worker threads', cf.num_workers)
    for i in range(cf.num_workers):
        t = threading.Thread(target=processor, kwargs={'worker_id': i})
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
                logger.error("[cron] section '%s' does not specify target function.", name)
                continue

            if cf.has_option(section, 'interval'):
                interval = cf.getfloat(section, 'interval')
            else:
                logger.error("[cron] section '%s' does not specify execution interval.", name)
                continue

            try:

                dottedpath, funcname = funcspec.split(':', 1)
                func = load_function(dottedpath, funcname)
            except Exception as exc:
                logger.error("[cron] could not load function '%s:%s': %s", funcspec, exc)
                continue

            now = cf.getboolean(section, 'now', fallback=False)
            logger.debug("Scheduling function '%s' as periodic task to run every %s seconds via "
                         "[cron:%s] section", funcname, interval, name)
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
            logger.warning("MQTT server disconnected, trying to reconnect every %s seconds",
                           reconnect_interval)
            time.sleep(reconnect_interval)


def cleanup(signum=None, frame=None):
    """Signal handler to ensure we disconnect cleanly in the event of a SIGTERM or SIGINT."""
    for ptname in ptlist:
        logger.debug("Cancel %s timer", ptname)
        ptlist[ptname].cancel()

    logger.debug("Disconnecting from MQTT broker...")
    if cf.lwt is not None:
        mqttc.publish(cf.lwt, LWTDEAD, qos=0, retain=True)

    mqttc.loop_stop()
    mqttc.disconnect()

    logger.info("Waiting for queue to drain")
    q_in.join()

    # Send exit signal to subsystems _after_ queue was drained
    global exit_flag
    exit_flag = True

    logger.debug("Exiting on signal %d", signum)
    sys.exit(signum)


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
    item.service = srv
    item.target = 'mqttwarn'
    item.data = {}        # FIXME

    # Launch plugin
    plugin = service_plugins[name]['plugin']
    response = plugin(srv, item)
    logger.info('Plugin response: %r', response)
