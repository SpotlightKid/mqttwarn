#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Publish a message to an MQTT broker."""

__version__ = '0.1.0'
__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2019 Christopher Arndt"
__license__ = "MIT License"

import logging
import sys
from os.path import expanduser, join

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


CONFIG_FILE = join(CONFIG_DIR, 'mqtt-pub.conf')
CA_CERTFILE = join(CONFIG_DIR, 'ca.crt')
MQTT_CLIENT_ID = 'mqtt-pub'

log = logging.getLogger('mqtt-pub')


class OneShotMQTTClient(MQTTClient):
    """Simple MQTT client which publishes just one message and then disconnects again."""

    def __init__(self, *args, **kwargs):
        self._data = kwargs.setdefault('userdata', {})
        super().__init__(*args, **kwargs)
        self.msg = None

    def set_message(self, topic, payload, qos=0, retain=False):
        self.msg = (topic, payload, qos, retain)

    @staticmethod
    def on_connect(self, data, flags, rc):
        try:
            if rc == 0:
                log.info("Connected to MQTT broker.")

                if self.msg:
                    log.debug("Publishing message to topic '%s'.", self.msg[0])
                    minfo = self.publish(*self.msg)
                    log.debug("Message #%s published.", minfo.mid)
                    data['mid'] = minfo.mid

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

    @staticmethod
    def on_publish(self, data, mid):
        try:
            if mid == data.get('mid'):
                log.debug("Publish confirmation for message #%s received. Signalling exit.", mid)
                self.disconnect()
        except Exception as exc:
            log.exception("Error in 'on_publish' callback: %s", exc)
            self.disconnect()


def main(args=None):
    if HAVE_CONFIGARGPARSE:
        ap = argparse.ArgumentParser(prog='mqtt-pub', description=__doc__ + '\n\n',
                                     add_help=False, default_config_files=[CONFIG_FILE],
                                     formatter_class=argparse.RawTextHelpFormatter)
        ap.add_argument('-c', '--config', is_config_file=True, metavar='PATH',
                        help='Config file path')
    else:
        ap = argparse.ArgumentParser(prog='mqtt-pub', description=__doc__,
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
                    help="Quality of service with which to publish message (default: %(default)r)")
    ap.add_argument('-r', '--retain', action="store_true",
                    help="Mark message as as 'last known good' value for topic on the broker")
    ap.add_argument('--cafile', default=CA_CERTFILE, metavar='PATH',
                    help="Path of file with trusted CA certificates (default: %(default)r)")
    ap.add_argument('--protocol', default='mqttv311', choices=['mqttv31', 'mqttv311'],
                    help="MQTT protocol version to use for connection (default: %(default)r)")
    ap.add_argument('--tls-version', default='tlsv1.2', choices=['tlsv1', 'tlsv1.1', 'tlsv1.2'],
                    help="TLS protocol version to use for secure connection (default: %(default)r)")
    ap.add_argument('topic', help="MQTT message topic to publish to")
    ap.add_argument('payload', nargs='*',
                    help="MQTT message payload to publish (all remaining positional args)")

    args = ap.parse_args(args if args is not None else sys.argv[1:])

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARN,
                        format="[%(name)s] %(message)s")
    if args.payload:
        payload = " ".join(args.payload)
    else:
        try:
            payload = sys.stdin.read()
        except KeyboardInterrupt:
            return 1

    if not args.port:
        args.port = 8883 if args.use_tls else 1883

    mqtt = OneShotMQTTClient(client_id=args.client_id, protocol=MQTT_PROTOCOLS[args.protocol],
                             transport=args.transport)
    if args.use_tls:
        mqtt.tls_set(ca_certs=args.cafile, tls_version=TLS_VERSIONS[args.tls_version])

    if args.username:
        res = handle_authentication(args)
        if res:
            return res

        mqtt.username_pw_set(args.username, args.password)

    mqtt.set_message(topic=args.topic, payload=payload, qos=args.qos, retain=args.retain)

    try:
        log.debug("Attempting connection to MQTT broker at %s:%s ...", args.host, args.port)
        mqtt.connect(host=args.host, port=args.port)
    except Exception as exc:
        log.error("Could not connect to MQTT broker: %s", exc)
    else:
        try:
            mqtt.loop_forever()
        except KeyboardInterrupt:
            return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
