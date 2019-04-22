# -*- coding: utf-8 -*-

__author__ = """\
Fabian Affolter <fabian()affolter-engineering.ch>,
Christopher Arndt <info@chrisarndt.de>
"""
__copyright__ = "Copyright 2014 Fabian Affolter, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import syslog


FACILITIES = {
    'auth': syslog.LOG_AUTH,
    'cron': syslog.LOG_CRON,
    'daemon': syslog.LOG_DAEMON,
    'kernel': syslog.LOG_KERN,
    'local0': syslog.LOG_LOCAL0,
    'local1': syslog.LOG_LOCAL1,
    'local2': syslog.LOG_LOCAL2,
    'local3': syslog.LOG_LOCAL3,
    'local4': syslog.LOG_LOCAL4,
    'local5': syslog.LOG_LOCAL5,
    'local6': syslog.LOG_LOCAL6,
    'local7': syslog.LOG_LOCAL7,
    'lpr': syslog.LOG_LPR,
    'mail': syslog.LOG_MAIL,
    'news': syslog.LOG_NEWS,
    'syslog': syslog.LOG_SYSLOG,
    'user': syslog.LOG_USER,
    'uucp': syslog.LOG_UUCP,
}

try:
    FACILITIES['authpriv'] = syslog.LOG_AUTHPRIV
except AttributeError:
    pass


OPTIONS = {
    'cons': syslog.LOG_CONS,
    'pid': syslog.LOG_PID,
}

try:
    OPTIONS['perror'] = syslog.LOG_PERROR
except AttributeError:
    pass


PRIORITIES = {
    5: syslog.LOG_EMERG,
    4: syslog.LOG_ALERT,
    3: syslog.LOG_CRIT,
    2: syslog.LOG_ERR,
    1: syslog.LOG_WARNING,
    0: syslog.LOG_NOTICE,
    -1: syslog.LOG_INFO,
    -2: syslog.LOG_DEBUG
}


def plugin(srv, item):
    """mqttwarn ``syslog`` service plugin.

    This service relays MQTT messages to a local syslog server.

    .. code-block:: ini

        [config:syslog]
        targets = {
                #        facility, options
                'user': ['user', 'pid'],
                'kernel': ['kernel', 'pid']
            }

        [log/user]
        targets = log:user
        priority = 1

    Facilities (case-insensitive):

    ``'AUTH'``, ``'CRON'``, ``'DAEMON'``, ``'KERN'``, ``'LOCAL0'`` to
    ``'LOCAL7'``, ``'LPR'``, ``'MAIL'``, ``'NEWS'``, ``'SYSLOG'``, ``'USER'``,
    ``'UUCP'``, and, if defined in ``<syslog.h>``, ``'AUTHPRIV'``.

    Log options (case-insensitive):

    ``'CONS'``, ``'PID'``, and, if defined in ``<syslog.h>``, ``'PERROR'``.
    Several option can be given separated by commas.

    +--------------+-----------+-------------------------------------------+
    | Topic option | Required? | Description                               |
    +==============+===========+===========================================+
    | ``title``    | no        | application title (default: ``mqttwarn``) |
    +--------------+-----------+-------------------------------------------+
    | ``priority`` | no        | log level (default: 0 = ``LOG_NOTICE``)   |
    +--------------+-----------+-------------------------------------------+

    Where ``priority`` can be between ``-2`` and ``5`` and maps to ``syslog``
    levels according to the following table:

    +----------+------------------+
    | Priority | Syslog Log Level |
    +==========+==================+
    | -2       | LOG_DEBUG        |
    +----------+------------------+
    | -1       | LOG_INFO         |
    +----------+------------------+
    | 0        | LOG_NOTICE       |
    +----------+------------------+
    | 1        | LOG_WARNING      |
    +----------+------------------+
    | 2        | LOG_ERR          |
    +----------+------------------+
    | 3        | LOG_CRIT         |
    +----------+------------------+
    | 4        | LOG_ALERT        |
    +----------+------------------+
    | 5        | LOG_EMERG        |
    +----------+------------------+

    Example syslog output::

        Apr 22 12:42:42 mqttest019 mqttwarn[9484]: Disk utilization: 94%

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if len(item.addrs) < 2:
        srv.error("Service target '%s:%s' address missing log facility and/or option.",
                  item.service, item.target)
        return False

    title = item.get('title', srv.SCRIPTNAME)
    facility = FACILITIES.get(item.addrs[0].lower())
    priority = PRIORITIES.get(item.get('priority'), 0)
    message = item.message
    options = 0

    for val in item.addrs[1].lower().split(','):
        options |= OPTIONS.get(val.strip(), 0)

    if facility is None:
        facility = syslog.LOG_USER
        item.addrs[0] = 'USER'

    try:
        srv.log.debug("Logging message to syslog facility LOG_%s, options %i.",
                      item.addrs[0].upper(), options)
        syslog.openlog(title, options, facility)
        syslog.syslog(priority, message)
    except Exception as exc:
        srv.log.error("Error sending to syslog: %s", exc)
        return False
    else:
        srv.log.debug("Successfully sent syslog message.")
    finally:
        syslog.closelog()

    return True
