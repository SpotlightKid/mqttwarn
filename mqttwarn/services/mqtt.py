#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

import paho.mqtt.publish as mqtt  # pip install --upgrade paho-mqtt
import six

try:
    from configparser import RawConfigParser
except ImportError:
    from ConfigParser import RawConfigParser


def read_conf(ini_file, params):
    c = RawConfigParser()
    c.read(ini_file)

    if c.has_section('defaults'):
        # differentiate bool, int, str
        if c.has_option('defaults', 'hostname'):
            params['hostname'] = c.get('defaults', 'hostname')
        if c.has_option('defaults', 'client_id'):
            params['client_id'] = c.get('defaults', 'client_id')
        if c.has_option('defaults', 'port'):
            params['port'] = c.getint('defaults', 'port')
        if c.has_option('defaults', 'qos'):
            params['qos'] = c.getint('defaults', 'qos')
        if c.has_option('defaults', 'retain'):
            params['retain'] = c.getboolean('defaults', 'retain')

    if c.has_section('auth'):
        params['auth'] = dict(c.items('auth'))

    if c.has_section('tls'):
        params['tls'] = dict(c.items('tls'))


def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    conf = item.config.get

    hostname = conf('hostname', 'localhost')
    port = int(conf('port', '1883'))
    qos = int(conf('qos', 0))
    retain = int(conf('retain', 0))
    username = conf('username', None)
    password = conf('password', None)
    params = {
        'hostname': hostname,
        'port': port,
        'qos': qos,
        'retain': retain,
        'client_id': None,
    }

    if username is not None:
        params['auth'] = {
            'username': username,
            'password': password
        }

    try:
        outgoing_topic, ini_file = item.addrs
    except ValueError:
        outgoing_topic = item.addrs[0]
    else:
        if ini_file is not None:
            try:
                read_conf(ini_file, params)
            except Exception as exc:
                srv.log.error("Target mqtt cannot load/parse INI file `%s': %s", ini_file, exc)
                return False

    # Attempt to interpolate data into topic name. If it isn't possible
    # ignore, and return without publish
    if item.data is not None:
        try:
            outgoing_topic = item.addrs[0].format(**item.data)
        except Exception as exc:
            srv.log.debug("Message not published. Outgoing topic cannot be formatted: %s", exc)
            return False

    outgoing_payload = item.message
    if isinstance(outgoing_payload, six.string_types):
        outgoing_payload = outgoing_payload.encode('utf-8')

    try:
        mqtt.single(outgoing_topic, outgoing_payload, **params)
    except Exception as exc:
        srv.log.warning("Cannot PUBlish via 'mqtt:%s': %s", item.target, exc)
        return False

    return True
