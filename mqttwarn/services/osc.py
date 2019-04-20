#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mqttwarn osc service plugin.

The ``osc`` relays MQTT messages via the Open Sound Control (OSC) protocol.

The address of each service target must be ahve two elements: the address of
the OSC message to send and a string of OSC type tags for the message data.

.. code-block:: ini

    [config:osc]
    host = localhost
    port = 9001
    protocol = UDP
    targets = {
            #           OSC address          type tags
            'target1': ['/osc/address/path', 's']
        }

    [osc/target1]
    targets = osc:target1

The MQTT message payload as a string will be used as the OSC message data,
which means the OSC type tags should be ``'s``. Use a different type tags to
convert the payload to another type, e.g. ``'i'`` or ``'f'`` for numeric
values. If the OSC message should have more than one data element, the MQTT
message payload must be converted into a tuple or list of values using a
function set via the ``format`` option of the topic handler, which triggers the
service.

Example:

.. code-block:: python

    # myfuncs.py
    def get_xy(payload, data):
        try:
            return int(data['x']), int(data['y'])
        except (KeyError, ValueError):
            return None

.. code-block:: ini

    [config:osc]
    targets = {
            'xypad': ['/mydevice/control/1/xy', 'ii']
        }

    [osc/xypad]
    targets = osc:xypad
    format: myfuncs:get_xy()

MQTT message JSON payload:

.. code-block:: json

    {"x": 50, "y": 100}

"""

__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2019 Christopher Arndt"
__license__ = "MIT License"


import liblo


class ConfigurationError(Exception):
    pass


class Plugin:
    def __init__(self, srv=None, config=None):
        self.srv = srv
        self.log = srv.log
        self.config = config
        conf = self.config.get
        self.host = conf('host', 'localhost')
        self.port = conf('port', 9001)

        try:
            self.protocol = getattr(liblo, conf('protocol', 'UDP').upper())
        except Exception as exc:
            raise ConfigurationError("Error in osc service '%s' configuration: %s" %
                                     (srv.name, exc))
        else:
            self.osc_addr = liblo.Address(self.host, self.port, self.protocol)

    def close(self):
        self.log.debug("'osc' service close called.")

    def plugin(self, srv, item):
        srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

        if len(item.addrs) < 2 or not item.addrs[0] or not item.addrs[1]:
            srv.log.error("Service '%s' target '%s' address invalid.", item.service, item.target)
            return False

        if isinstance(item.message, (list, tuple)):
            data = item.message
        else:
            data = (item.message,)

        try:
            osc_args = tuple(zip(item.addrs[1], data))
        except (TypeError, ValueError) as exc:
            srv.log.error("Could not map OSC type tags ('%s') for target '%s:%s' to payload data "
                          "'%s': %s", item.addrs[1], item.service, item.target, data, exc)
            return False

        try:
            srv.log.debug("Sending OSC message to %s: %s %r", self.osc_addr.url, item.addrs[0],
                          osc_args)
            liblo.send(self.osc_addr, item.addrs[0], *osc_args)
        except (IOError, liblo.AddressError) as exc:
            self.log.warn("Could not send OSC message: %s", exc)
            return False

        return True

    __call__ = plugin
