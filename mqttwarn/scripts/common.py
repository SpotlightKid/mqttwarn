#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Common code for mqtt-tools command line scripts."""

import getpass
import os
import ssl

from paho.mqtt.client import MQTTv31, MQTTv311

try:
    import keyring
    HAVE_KEYRING = True
except ImportError:
    HAVE_KEYRING = False


MQTT_PROTOCOLS = {
    'mqttv311': MQTTv31,
    'tlsv1.1': MQTTv311,
}
TLS_VERSIONS = {
    'tlsv1': ssl.PROTOCOL_TLSv1,
    'tlsv1.1': ssl.PROTOCOL_TLSv1_1,
    'tlsv1.2': ssl.PROTOCOL_TLSv1_2
}


def get_client_id(prefix):
    return '{}-{}-{}'.format(prefix, getpass.getuser(), os.getpid())


def handle_authentication(args):
    if not args.password:
        service_name = 'mqtt:{}:{}'.format(args.host, args.port)

        if HAVE_KEYRING:
            args.password = keyring.get_password(service_name, args.username)

        if not args.password:
            prompt = "Enter password for user '{}' on MQTT broker {}:{}': ".format(
                args.username, args.host, args.port)
            try:
                args.password = getpass.getpass(prompt)
            except (EOFError, KeyboardInterrupt):
                print('')
                return 2

            if HAVE_KEYRING and args.password:
                try:
                    res = input("Store password for user '{}' on '{}' in keyring? [Y/n] ".format(
                        args.username, service_name))
                except (EOFError, KeyboardInterrupt):
                    print('')
                    return 2
                else:
                    if res.strip().lower() in ('y', ''):
                        keyring.set_password(service_name, args.username, args.password)


