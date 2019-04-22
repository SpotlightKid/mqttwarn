# -*- coding: utf-8 -*-

__author__    = 'Jan-Piet Mens <jpmens()gmail.com>'
__copyright__ = 'Copyright 2014 Jan-Piet Mens'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

HAVE_REDIS=True
try:
    import redis
except:
    HAVE_REDIS=False

def plugin(srv, item):
    ''' redispub. Expects addrs to contain (channel) '''

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if HAVE_REDIS is False:
        srv.log.warn("Redis is not installed")
        return False

    host = item.config.get('host', 'localhost')
    port = int(item.config.get('port', 6379))

    try:
        rp = redis.Redis(host, port)
    except Exception as exc:
        srv.log.warn("Cannot connect to redis on %s:%s : %s" % (host, port, exc))
        return False

    channel = item.addrs[0]
    text = item.message

    try:
        rp.publish(channel, text)
    except Exception as exc:
        srv.log.warn("Cannot publish to redis on %s:%s : %s" % (host, port, exc))
        return False

    return True
