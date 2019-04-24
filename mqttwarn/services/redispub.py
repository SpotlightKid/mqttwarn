# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>, Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2014 Jan-Piet Mens, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import redis


def plugin(srv, item):
    """mqttwarn ``redispub`` service plugin.

    Forwards the MQTT message payload (possibly formatted) to a Redis
    subscripition channel.

    Expects the service target addresses to contain the Redis channel name as
    the first and only item.

    Example configuration:

    .. code-block:: ini

        [config:redispub]
        host = localhost
        db = 0
        password =
        targets = {
                'my-channel': ['my-channel']
            }

        [redis/my-channel]
        targets = redispub:my-channel
        format = Message received via MQTT at {_ltiso}: {payload}

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if not item.addrs or not item.addrs[0]:
        srv.log.error("Invalid configuration for service target '%s:%s': "
                      "Redis channel may not be null.")
        return False

    channel = item.addrs[0]
    conf = item.config.get
    host = conf('host', 'localhost')
    password = conf('password')

    try:
        port = int(conf('port'))
    except (TypeError, ValueError):
        port = 6379

    try:
        db = int(conf('db'))
    except (TypeError, ValueError):
        db = 0

    try:
        rp = redis.Redis(host=host, port=port, db=db or 0, password=password or None)
        # The redis client creates the connection on demand. There's no need to
        # put the creation of the Redis instance in a separate try/except block
        rp.publish(channel, item.message)
    except Exception as exc:
        srv.log.warn("Could not publish to Redis channel '%s' on %s:%s db=%i: %s",
                     channel, host, port, db, exc)
        return False
    else:
        srv.log.debug("Sucessfully published message on Redis channel '%s'.", channel)

    return True
