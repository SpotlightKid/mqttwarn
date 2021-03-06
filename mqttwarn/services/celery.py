# -*- coding: utf-8 -*-

__author__    = 'Orhan Hirsch <orhanhenrik()gmail.com>'
__copyright__ = 'Copyright 2017 Orhan Hirsch'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

HAVE_CELERY=True
try:
    import celery
    import json
except ImportError:
    HAVE_CELERY=False

def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)
    if not HAVE_CELERY:
        srv.log.error("'celery' or 'json' module not installed")
        return False

    config = item.config

    app = celery.Celery(
        config['app_name'],
        broker=config['broker_url']
    )

    for target in item.addrs:
        message = item.message
        try:
            if target['message_format'] == 'json':
                message = json.loads(message)
            app.send_task(target['task'], [item.message])
        except Exception as exc:
            srv.log.warning("Error: %s" % exc)
            return False

    return True
