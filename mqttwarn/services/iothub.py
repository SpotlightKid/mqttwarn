# -*- coding: utf-8 -*-

__author__    = 'Morten Høybye Frederiksen <morten()mfd-consult.dk>'
__copyright__ = 'Copyright 2016 Morten Høybye Frederiksen'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

HAVE_IOTHUB=True
try:
    import iothub_client
    from iothub_client import *
    from iothub_client_args import *
    import uuid
except ImportError:
    HAVE_IOTHUB=False

iothub_clients = {}

def iothub_connect(srv, item, deviceid, devicekey):
    # item.config is brought in from the configuration file
    try:
        hostname = item.config['hostname']
    except Exception as exc:
        srv.log.error("Incorrect target configuration for target=%s: %s", item.target, exc)
        return False
    protocol = item.config.get('protocol', 'AMQP')
    timeout = item.config.get('timeout')
    minimum_polling_time = item.config.get('minimum_polling_time')
    message_timeout = item.config.get('message_timeout')

    # Prepare connection to Azure IoT Hub
    connection_string = "HostName=%s;DeviceId=%s;SharedAccessKey=%s" % (hostname, deviceid, devicekey)
    connection_string, protocol = get_iothub_opt(["-p", protocol], connection_string)
    client = IoTHubClient(connection_string, protocol)
    if client.protocol == IoTHubTransportProvider.HTTP:
        if timeout is not None:
            client.set_option("timeout", timeout)
        if minimum_polling_time is not None:
            client.set_option("MinimumPollingTime", minimum_polling_time)
    if message_timeout is not None:
        client.set_option("messageTimeout", message_timeout)
    srv.log.info("Client: protocol=%s, hostname=%s, device=%s" % (protocol, hostname, deviceid))
    return client

def iothub_send_confirmation_callback(msg, res, srv):
    if res != IoTHubClientConfirmationResult.OK:
        srv.log.error("Message confirmation: id=%s: %s", msg.message_id, res)
    else:
        srv.log.debug("Message confirmation: id=%s: %s", msg.message_id, res)

def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if not HAVE_IOTHUB:
        srv.log.error("Azure IoT SDK is not installed")
        return False

    # addrs is a list[] containing device id and key.
    deviceid, devicekey = item.addrs

    # Create connection
    try:
        if not deviceid in iothub_clients:
            iothub_clients[deviceid] = iothub_connect(srv, item, deviceid, devicekey)
        client = iothub_clients[deviceid]
    except Exception as exc:
        srv.log.error("Unable to connect for target=%s, deviceid=%s: %s" % (item.target, deviceid, exc))
        return False

    # Prepare message
    try:
        if type(item.message) == unicode:
            msg = IoTHubMessage(bytearray(item.message, 'utf8'))
        else:
            msg = IoTHubMessage(item.message)
        msg.message_id = str("%s:%s" % (item.target, uuid.uuid4().hex))
    except Exception as exc:
        srv.log.error("Unable to prepare message for target=%s: %s" % (item.target, exc))
        return False

    # Send
    try:
        client.send_event_async(msg, iothub_send_confirmation_callback, srv)
        srv.log.debug("Message queued: id=%s", msg.message_id)
    except Exception as exc:
        srv.log.error("Unable to send to IoT Hub for target=%s: %s" % (item.target, exc))
        return False

    return True
