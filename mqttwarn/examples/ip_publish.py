# -*- coding: utf-8 -*-
"""Functions for periodically executed tasks froma [cron:xxx] seccion."""

import socket

import requests


def publish_public_ip_address(srv=None):
    """Periodic task for publishing your public IP address to the MQTT bus.

    Receives a Service class instance, which can be used to write to mqttwarn's
    log via the ``log`` attribute and use its MQTT client instance via the
    ``mqttc`` attribute.

    Its return value is ignored and any exceptions it throws are caught in
    its worker thread and logged with level ``ERROR`` and a traceback.

    """
    hostname = socket.gethostname()
    resp = requests.get('https://httpbin.org/ip')

    try:
        data = resp.json()
        ip_address = data['origin']
    except Exception as exc:
        if srv is not None:
            srv.log.error("Error reading JSON response: %s", exc)
        return

    if ',' in ip_address:
        ip_address = ", ".join(sorted(set(addr.strip() for addr in ip_address.split(','))))

    if srv is not None:
        # optional debug logger
        srv.log.debug("Publishing public IP address '%s' of host '%s'.", ip_address, hostname)

        # publish ip address to mqtt
        srv.mqttc.publish('test/ip/' + hostname, ip_address, retain=True)
