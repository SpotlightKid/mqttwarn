# -*- coding: utf-8 -*-
# mqttwarn example function extensions

import time

import six

try:
    import json
except ImportError:
    import simplejson as json


def OwnTracksTopic2Data(topic):
    if isinstance(topic, six.string_types):
        try:
            # owntracks/username/device
            _, username, device = topic.split('/')
        except ValueError:
            username = 'unknown'
            device = 'unknown'

        return dict(username=username, device=device)


def OwnTracksConvert(data):
    if isinstance(data, dict):
        # Better safe than sorry:
        # Clone transformation dictionary to prevent leaking local modifications
        # See also https://github.com/jpmens/mqttwarn/issues/219#issuecomment-271815495
        data = data.copy()
        tst = int(data.get('tst', time.time()))
        data['tst'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(tst))
        return "{username} {device} {tst} at location {lat},{lon}".format(**data)


def OwnTracksBattFilter(topic, message):
    """Filter out any OwnTracks notifications which do not contain the 'batt' parameter.

    When the filter function returns True, the message is filtered out, i.e. not
    processed further.

    """
    batt = json.loads(message).get('batt')

    if batt is not None:
        try:
            # Suppress message if value stored under key 'batt' is greater than 20
            return int(batt) > 20
        except ValueError:
            return True

    # Suppress message because no 'batt' key in data
    return True


def TopicTargetList(topic=None, data=None, srv=None):
    """Compute list of topic targets based on MQTT topic and/or transformation data.

    Receives MQTT topic, transformation data and service object.
    Returns list of topic target identifiers.

    """
    # optional debug logger
    if srv is not None:
        srv.logging.debug('topic={topic}, data={data}, srv={srv}'.format(**locals()))

    # Use a fixed list of topic targets for demonstration purposes.
    targets = ['log:debug']

    # In the real world, you would compute proper topic targets based on information
    # derived from transformation data, which in turn might have been enriched
    # by ``datamap`` or ``alldata`` transformation functions before, like that:
    if 'condition' in data:
        if data['condition'] == 'sunny':
            targets.append('file:mqttwarn')
        elif data['condition'] == 'rainy':
            targets.append('log:warn')

    return targets


def publish_public_ip_address(srv=None):
    """Periodic task for publishing your public IP address to the MQTT bus.

    Receives service object.
    Returns None.

    """
    import socket
    import requests

    hostname = socket.gethostname()
    ip_address = requests.get('https://httpbin.org/ip').json().get('origin')

    if srv is not None:
        # optional debug logger
        srv.logging.debug("Publishing public IP address '%s' of host '%s'.", ip_address, hostname)

        # publish ip address to mqtt
        srv.mqttc.publish('test/ip/' + hostname, ip_address)
