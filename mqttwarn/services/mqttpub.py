#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

import six


def plugin(srv, item):
    """Publish via MQTT to the same broker connection.

    Requires topic, qos and retain flag to be specified in target address.

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    outgoing_topic, qos, retain = item.addrs[:3]

    # Attempt to interpolate data into topic name.
    # If it isn't possible ignore messsage, and return without publishing.
    if item.data is not None:
        try:
            outgoing_topic = outgoing_topic.format(**item.data)
        except:
            srv.log.debug("Outgoing topic cannot be formatted; not published.")
            return False

    outgoing_payload = item.message
    if isinstance(outgoing_payload, six.string_types):
        outgoing_payload = outgoing_payload.encode('utf-8')

    try:
        srv.mqttc.publish(outgoing_topic, outgoing_payload, qos=qos, retain=retain)
    except Exception as exc:
        srv.log.warning("Cannot PUBlish via 'mqttpub:%s': %s", item.target, exc)
        return False

    return True
