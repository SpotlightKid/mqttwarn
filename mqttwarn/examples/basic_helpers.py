# -*- coding: utf-8 -*-
#
# mqttwarn example helper functions for topic handlers
#

def topic2targetlist(topic=None, data=None, srv=None):
    """Compute list of topic targets based on MQTT topic and/or transformation data.

    Receives MQTT topic, transformation data and service object.

    Must return a list of topic target identifiers, each being a two-item tuple
    (service, target).

    """
    srv.logging.debug('topic={topic}, data={data}, srv={srv}'.format(**locals()))

    # Use a fixed list of topic targets for demonstration purposes.
    targets = [('log', 'debug')]

    # In the real world, you would probably compute proper topic targets based
    # on information derived from transformation data (which in turn might
    # have been enriched by ``datamap`` transformation functions before)
    # like this:
    if data.get('condition') == 'sunny':
        targets.append(('file', 'mqttwarn'))
    elif data.get('condition') == 'rainy':
        targets.append(('log', 'warn'))

    return targets
