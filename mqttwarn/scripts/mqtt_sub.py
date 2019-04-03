#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Subscribe to one or more topics on an MQTT broker and display messages as they are published."""

__version__ = '0.1.0'
__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2019 Christopher Arndt"
__license__ = "MIT License"

import json
import logging
import sys
from os.path import expanduser, join

import six
from paho.mqtt.client import Client as MQTTClient

from .common import (HAVE_KEYRING, MQTT_PROTOCOLS, TLS_VERSIONS, get_client_id,
                     handle_authentication)

try:
    import configargparse as argparse
    HAVE_CONFIGARGPARSE = True
except ImportError:
    import argparse
    HAVE_CONFIGARGPARSE = False

try:
    import appdirs
except ImportError:
    CONFIG_DIR = expanduser('~/.config/mqtt-tools')
else:
    CONFIG_DIR = appdirs.AppDirs('mqtt-tools').user_config_dir


CONFIG_FILE = join(CONFIG_DIR, 'mqtt-sub.conf')
CA_CERTFILE = join(CONFIG_DIR, 'ca.crt')
MQTT_CLIENT_ID = 'mqtt-sub'

log = logging.getLogger('mqtt-sub')


def on_topic(topic):
    def wrapper(fn):
        fn._mqtt_topic = topic
        return fn
    return wrapper


class MQTTMeta(type):
    def __new__(metacls, classname, bases, classdict):
        cls = type.__new__(metacls, classname, bases, dict(classdict))
        cls._mqtt_handlers = []

        for name, elem in classdict.items():
            topic = getattr(elem, '_mqtt_topic', None)
            if topic:
                cls._mqtt_handlers.append((topic, elem))

        return cls


class MQTTSub(MQTTClient, metaclass=MQTTMeta):
    def __init__(self, qos=0, decode_json=False, format="{} {}", *args, **kwargs):
        self._data = kwargs['userdata'] = {}
        super().__init__(*args, **kwargs)
        self.qos = qos
        self.decode_json = decode_json
        self.format = format
        self.subscriptions = []
        self.topics = {}

        for topic, method in self._mqtt_handlers:
            self.message_callback_add(topic, method)

    def add_topics(self, *topics):
        for topic in topics:
            if isinstance(topic, six.string_types):
                topic, qos = topic, self.qos
            elif isinstance(topic, (list, tuple)):
                if len(topic) == 1:
                    topic, qos = topic[0], self.qos
                else:
                    topic, qos = topic[:2]
            else:
                continue

            self.topics[topic] = qos

    @staticmethod
    def on_message(self, data, message):
        log.debug("Message received on topic '%s': %r", message.topic, message.payload)

        try:
            payload = None
            if self.decode_json:
                try:
                    payload = json.loads(message.payload)
                except Exception as exc:
                    log.warn("Could not decode message payload as JSON: %s", exc)

            if payload is None:
                payload = message.payload.decode('utf-8')

            print(self.format.format(message.topic, payload))
        except Exception as exc:
            log.exception("Error in 'on_message' callback: %s", exc)

    @staticmethod
    def on_connect(self, data, flags, rc):
        try:
            if rc == 0:
                log.info("Connected to MQTT broker.")

                if self.topics:
                    self.subscriptions = self.subscribe(list(self.topics.items()))
                    log.debug("Subscription results: %r", self.subscriptions)

                return
            elif rc == 1:
                log.error("Connection refused - unacceptable protocol version.")
            elif rc == 2:
                log.error("Connection refused - identifier rejected.")
            elif rc == 3:
                log.error("Connection refused - server unavailable.")
            elif rc == 4:
                log.error("Connection refused - bad user name or password.")
            elif rc == 5:
                log.error("Connection refused - not authorised.")
            else:
                log.error("Connection failed - result code %d.", rc)
        except Exception as exc:
            log.exception("Error in 'on_connect' callback: %s", exc)

        self.disconnect()


def main(args=None):
    if HAVE_CONFIGARGPARSE:
        ap = argparse.ArgumentParser(prog='mqtt-sub', description=__doc__ + '\n\n',
                                     add_help=False, default_config_files=[CONFIG_FILE],
                                     formatter_class=argparse.RawTextHelpFormatter)
        ap.add_argument('-c', '--config', is_config_file=True, metavar='PATH',
                        help='Config file path')
    else:
        ap = argparse.ArgumentParser(prog='mqtt-sub', description=__doc__,
                                     add_help=False, formatter_class=argparse.RawTextHelpFormatter)

    ap.add_argument('-h', '--help', action="help", help="Show help message")
    ap.add_argument('-v', '--verbose', action="store_true",
                    help="Enable debug logging")
    ap.add_argument('-i', '--client-id', default=get_client_id(MQTT_CLIENT_ID), metavar="ID",
                    help="MQTT client ID (default: %s-<user>-<pid>)" % MQTT_CLIENT_ID)
    ap.add_argument('-H', '--host', default='localhost',
                    help="MQTT broker server name or address (default: %(default)r)")
    ap.add_argument('-p', '--port', type=int,
                    help="MQTT broker port (default: 1883 or 8883 (with --use-tls))")
    ap.add_argument('-s', '--use-tls', action="store_true",
                    help="Use TLS for connection to MQTT broker")
    ap.add_argument('-t', '--transport', default='tcp', choices=['tcp', 'websockets'],
                    help="Transport used for connection to MQTT broker (default: %(default)r)")
    ap.add_argument('-u', '--username', help="MQTT broker user name")

    password_help = "MQTT broker password"
    if HAVE_KEYRING:
        password_help += " (can be retrieved from / stored in keyring)"

    ap.add_argument('-P', '--password', help=password_help)
    ap.add_argument('-q', '--qos', default=0, type=int, choices=[0, 1, 2],
                    help="Quality of service for the subscription(s) (default: %(default)r)")
    ap.add_argument('-C', '--no-clean-session', dest="clean_session", action="store_false",
                    help="Retain subscriptions when client disconnects")
    ap.add_argument('--cafile', default=CA_CERTFILE, metavar='PATH',
                    help="Path of file with trusted CA certificates (default: %(default)r)")
    ap.add_argument('--protocol', default='mqttv311', choices=['mqttv31', 'mqttv311'],
                    help="MQTT protocol version to use for connection (default: %(default)r)")
    ap.add_argument('--tls-version', default='tlsv1.2', choices=['tlsv1', 'tlsv1.1', 'tlsv1.2'],
                    help="TLS protocol version to use for secure connection (default: %(default)r)")
    ap.add_argument('-j', '--decode-json', action="store_true",
                    help="Attempt to decode message payload as JSON")
    ap.add_argument('topics', nargs='*', help="MQTT message topic(s) to subscribe to")

    args = ap.parse_args(args if args is not None else sys.argv[1:])

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARN,
                        format="[%(name)s] %(message)s")

    if not args.topics:
        try:
            args.topics = sys.stdin.readlines()
        except KeyboardInterrupt:
            return 1

    if not args.port:
        args.port = 8883 if args.use_tls else 1883

    mqtt = MQTTSub(
        client_id=args.client_id,
        clean_session=args.clean_session,
        protocol=MQTT_PROTOCOLS[args.protocol],
        transport=args.transport,
        qos=args.qos,
        decode_json=args.decode_json
    )

    if args.use_tls:
        mqtt.tls_set(ca_certs=args.cafile, tls_version=TLS_VERSIONS[args.tls_version])

    if args.username:
        res = handle_authentication(args)
        if res:
            return res

        mqtt.username_pw_set(args.username, args.password)

    mqtt.add_topics(args.topics)
    try:
        log.debug("Attempting connection to MQTT broker at %s:%s ...", args.host, args.port)
        mqtt.connect(host=args.host, port=args.port)
    except Exception as exc:
        log.error("Could not connect to MQTT broker: %s", exc)
    else:
        try:
            mqtt.loop_forever()
        except KeyboardInterrupt:
            mqtt.disconnect()
            return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
