# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


def plugin(srv, item):
    """log service plugin."""
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)
    level = item.addrs[0]
    text = item.message

    levels = {
        'debug': srv.log.debug,
        'info': srv.log.info,
        'warn': srv.log.warning,
        'crit': srv.log.critical,
        'error': srv.log.error,
    }

    try:
        levels[level](text)
    except Exception as exc:
        srv.log.warn("Cannot invoke service log with level `%s': %s", level, exc)
        return False

    return True
