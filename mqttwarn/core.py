# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import logging
import os
import socket
import sys
import threading
import time
from datetime import datetime
from pkg_resources import resource_filename

import paho.mqtt.client as paho
import six

from .context import RuntimeContext, FunctionInvoker
from .cron import PeriodicThread
from .util import (load_function, load_module, timeout, parse_cron_options, sanitize_function_name,
                   Struct, Formatter, asbool)

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


class Job(object):
    def __init__(self, prio, service, section, topic, payload, data, target):
        self.prio = prio
        self.service = service
        self.section = section
        self.topic = topic
        self.payload = payload  # raw payload
        self.data = data        # decoded payload
        self.target = target
        logger.debug("New '%s:%s' job: %s", service, target, topic)

    def __cmp__(self, other):
        return ((self.prio > other.prio) - (self.prio < other.prio))


def render_template(filename, data):
    text = None
    if HAVE_JINJA is True:
        template = jenv.get_template(filename)
        text = template.render(data)

    return text


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

        subscribed = []
        for section in context.get_sections():
            topic = context.get_topic(section)
            qos = context.get_qos(section)

            if topic in subscribed:
                continue

            logger.debug("Subscribing to %s (qos=%d)", topic, qos)
            mqttc.subscribe(topic, qos)
            subscribed.append(topic)

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


def on_message(mosq, userdata, msg):
    """Handle message received from the broker."""
    logger.debug("Message received on %s: %r", msg.topic, msg.payload)
    topic = msg.topic

    try:
        payload = msg.payload.decode('utf-8')
    except UnicodeEncodeError:
        payload = msg.payload

    if msg.retain == 1:
        if cf.skipretained:
            logger.debug("Skipping retained message on %s", topic)
            return

    # Try to find matching settings for this topic
    for section in context.get_sections():
        # Get the topic for this section (usually the section name but optionally overridden)
        match_topic = context.get_topic(section)
        if paho.topic_matches_sub(match_topic, topic):
            logger.debug("Section [%s] matches message on %s. Processing...", section, topic)

            # Check for any message filters
            if context.is_filtered(section, topic, payload):
                logger.debug("Filter in section [%s] has skipped message on %s", section, topic)
                continue

            # Send the message to any targets specified
            send_to_targets(section, topic, payload)

# End of MQTT broker callbacks


def send_failover(reason, message):
    # Make sure we dump this event to the log
    logger.warn(message)
    # Attempt to send the message to our failover targets
    send_to_targets('failover', reason, message)


def send_to_targets(section, topic, payload):
    if not cf.has_section(section):
        logger.warn("Section [%s] does not exist in your INI file, skipping message on %s",
                    section, topic)
        return

    # Decode raw payload into transformation data
    data = decode_payload(section, topic, payload)

    dispatcher_dict = cf.getdict(section, 'targets')
    function_name = sanitize_function_name(context.get_config(section, 'targets'))

    if function_name is not None:
        targetlist = context.get_topic_targets(section, topic, data)
        if not isinstance(targetlist, list):
            logger.error('Topic target definition by function "%s" in section "%s" is empty or '
                         'incorrect. targetlist=%r, type=%s', function_name, section, targetlist,
                         type(targetlist))
            return

    elif isinstance(dispatcher_dict, dict):
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
        sorted_dispatcher = sorted(dispatcher_dict.items(), key=get_key, reverse=True)
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
        targetlist = cf.getlist(section, 'targets')
        if not isinstance(targetlist, list):
            # if targets is neither dict nor list
            logger.error("Target definition in section [%s] is incorrect", section)
            cleanup(0)
            return

    # Interpolate transformation data values into topic targets
    # be graceful if interpolation fails, but log a meaningful message
    targetlist_resolved = []
    for target in targetlist:
        try:
            target = target.format(**data)
            targetlist_resolved.append(target)
        except Exception as exc:
            logger.exception('Cannot interpolate transformation data into topic target "%s": %s. '
                             'section=%s, topic=%s, payload=%s, data=%s', target, exc, section,
                             topic, payload, data)

    targetlist = targetlist_resolved

    for service in targetlist:
        logger.debug("Message on %s going to %s", topic, service)
        # Each target is either "service" or "service:target"
        # If no target specified then notify ALL targets
        target = None

        # Check if this is for a specific target
        if ':' in service:
            try:
                service, target = service.split(':', 2)
            except (TypeError, ValueError):
                logger.warn("Invalid target %s - should be 'service:target'", service)
                continue

        # skip targets with invalid services
        if service not in service_plugins:
            logger.error("Invalid configuration: topic %s points to non-existing service %s",
                         topic, service)
            continue

        sendtos = None
        if target is None:
            sendtos = context.get_service_targets(service)
        else:
            sendtos = [target]

        for sendto in sendtos:
            job = Job(1, service, section, topic, payload, data, sendto)
            q_in.put(job)


def builtin_transform_data(topic, payload):
    """Return a dict with standard transformation data available to all plugins."""
    dt = datetime.now()
    return {
        'topic': topic,
        'payload': payload,
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


def xform(function, orig_value, transform_data):
    """Attempt transformation on orig_value.

    1st. function()
    2nd. inline {xxxx}

    """
    if orig_value is None:
        return None

    res = orig_value

    if function is not None:
        function_name = sanitize_function_name(function)
        if function_name is not None:
            try:
                res = cf.datamap(function_name, transform_data)
                return res
            except Exception as exc:
                logger.warn("Cannot invoke %s(): %s", function_name, exc)

        try:
            res = Formatter().format(function, **transform_data).encode('utf-8')
        except Exception as exc:
            logger.warning("Cannot format message: %s", exc)

    if isinstance(res, six.string_types):
        res = res.replace("\\n", "\n")

    return res


def decode_payload(section, topic, payload):
    """Decode message payload through transformation machinery."""
    transform_data = builtin_transform_data(topic, payload)
    topic_data = context.get_topic_data(section, topic)

    if topic_data is not None and isinstance(topic_data, dict):
        transform_data.update(topic_data)

    # The dict returned is completely merged into transformation data
    # The difference between this and `get_topic_data()` is that this
    # function receives the topic string as well as the payload and any
    # existing transformation data, and it can do 'things' with all.
    # This is the way it should originally have been, but I can no
    # longer fix the original ... (legacy)

    all_data = context.get_all_data(section, topic, transform_data)

    if all_data is not None and isinstance(all_data, dict):
        transform_data.update(all_data)

    # Attempt to decode the payload from JSON. If it's possible, add
    # the JSON keys into item to pass to the plugin, and create the
    # outgoing (i.e. transformed) message.
    try:
        payload = payload.rstrip("\0")
        payload_data = json.loads(payload)
    except Exception as exc:
        logger.debug("Cannot decode JSON object, payload=%s: %s", payload, exc)
    else:
        if isinstance(payload_data, dict):
            transform_data.update(payload_data)

    return transform_data


def processor(worker_id=None):
    """Queue runner.

    Pull a job from the queue, find the module in charge
    of handling the service, and invoke the module's plugin to do so.

    """
    conf = context.get_config

    while not exit_flag:
        logger.debug('Job queue has %s items to process', q_in.qsize())
        job = q_in.get()

        service = job.service
        section = job.section
        target = job.target
        topic = job.topic

        logger.debug("Processor #%s is handling: '%s' for %s", worker_id, service, target)

        # Sanity checks.
        # If service configuration or targets can not be obtained successfully,
        # log a sensible error message, fail the job and carry on with the next job.
        try:
            service_config = context.get_service_config(service)
            service_targets = context.get_service_targets(service)

            if target not in service_targets:
                raise KeyError("Invalid configuration: topic {topic} points to non-existing "
                               "target {} in service {}".format(target, service))

        except Exception as exc:
            logger.exception("Cannot handle service=%s, target=%s: %s", service, target, exc)
            q_in.task_done()
            continue

        item = {
            'service': service,
            'section': section,
            'target': target,
            'config': service_config,
            'addrs': service_targets[target],
            'topic': topic,
            'payload': job.payload,
            'data': None,
            'title': None,
            'image': None,
            'message': None,
            'priority': None
        }

        item['data'] = transform_data = job.data.copy()
        item['title'] = xform(conf(section, 'title'), SCRIPTNAME, transform_data)
        item['image'] = xform(conf(section, 'image'), '', transform_data)
        item['message'] = xform(conf(section, 'format'), job.payload, transform_data)

        try:
            item['priority'] = int(xform(conf(section, 'priority'), 0, transform_data))
        except Exception as exc:
            item['priority'] = 0
            logger.warn("Failed to determine the priority, defaulting to zero: %s", exc)

        if HAVE_JINJA is False and conf(section, 'template'):
            logger.warn("Templating not possible because Jinja2 is not installed")

        if HAVE_JINJA is True:
            template = conf(section, 'template')
            if template is not None:
                try:
                    text = render_template(template, transform_data)
                    if text is not None:
                        item['message'] = text
                except Exception as exc:
                    logger.warn("Cannot render '%s' template: %s", template, exc)

        if item.get('message') is not None and len(item.get('message')) > 0:
            st = Struct(**item)
            notified = False
            try:
                # Fire the plugin in a separate thread and kill it if it doesn't return in 10s
                module = service_plugins[service]['module']
                service_logger_name = 'mqttwarn.services.{}'.format(service)
                srv = make_service(mqttc=mqttc, name=service_logger_name)
                notified = timeout(module.plugin, (srv, st))
            except Exception as exc:
                logger.error("Cannot invoke service for '%s': %s", service, exc)

            if not notified:
                logger.warn("Notification of %s for '%s' FAILED or TIMED OUT", service,
                            item.get('topic'))
        else:
            logger.warn("Notification of %s for '%s' suppressed: text is empty", service,
                        item.get('topic'))

        q_in.task_done()

    logger.debug("Thread exiting...")


def load_services(services):
    for service in services:
        service_plugins[service] = {}

        service_config = cf.config('config:' + service)
        if service_config is None:
            logger.error("Service '%s' has no config section", service)
            sys.exit(1)

        service_plugins[service]['config'] = service_config

        module = cf.g('config:' + service, 'module', service)
        modulefile = resource_filename('mqttwarn.services', module + '.py')

        try:
            service_plugins[service]['module'] = load_module(modulefile)
            logger.info("Successfully loaded service '%s'", service)
        except Exception as exc:
            logger.exception('Unable to load service "%s" from file "%s": %s',
                             service, modulefile, exc)


def connect():
    """Load service plugins, connect to the broker, launch daemon threads and listen forever."""
    # FIXME: Remove global variables
    global mqttc

    services = cf.getlist('defaults', 'launch')

    if not services:
        logger.error("No services configured. Aborting")
        sys.exit(2)

    try:
        os.chdir(cf.directory)
    except Exception as exc:
        logger.error("Cannot chdir to %s: %s", cf.directory, exc)
        sys.exit(2)

    load_services(services)

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

    # If the config file has a [cron] section, the key names therein are
    # functions from 'myfuncs.py' which should be invoked periodically.
    # The key's value (must be numeric!) is the period in seconds.

    if cf.has_section('cron'):
        for name, val in cf.items('cron'):
            try:
                func = load_function(name=name, filepath=cf.functions)
                cron_options = parse_cron_options(val)
                interval = cron_options['interval']
                logger.debug("Scheduling function '%s' as periodic task to run every %s "
                             "seconds via [cron] section", name, interval)
                service = make_service(mqttc=mqttc, name='mqttwarn.cron')
                ptlist[name] = PeriodicThread(callback=func, period=interval, name=name,
                                              srv=service, now=asbool(cron_options.get('now')))
                ptlist[name].start()
            except AttributeError:
                logger.error("[cron] section has function [%s] specified, but that's not defined.",
                             name)
                continue

    while not exit_flag:
        reconnect_interval = 5

        try:
            mqttc.loop_forever()
        except socket.error:
            pass
        # FIXME: add logging with trace for any other exceptions

        if not exit_flag:
            logger.warning("MQTT server disconnected, trying to reconnect each %s seconds",
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
    invoker = FunctionInvoker(config=config,
                              srv=make_service(mqttc=mqttc, name='mqttwarn.context'))
    context = RuntimeContext(config=config, invoker=invoker)
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
    module = service_plugins[name]['module']
    response = module.plugin(srv, item)
    logger.info('Plugin response: %r', response)
