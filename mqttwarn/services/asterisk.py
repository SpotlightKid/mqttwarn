# -*- coding: utf-8 -*-

__author__    = 'Artem Alexandrov <alexandrov@devexperts.com>'
__copyright__ = 'Copyright 2014 Artem Alexandrov'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

# Please install pyst2
import asterisk.manager

def plugin(srv, item):

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    host     = item.config['host']
    port = item.config['port']
    username = item.config['username']
    password = item.config['password']
    extension = item.config['extension']
    context  = item.config['context']

    gateway  = item.addrs[0]
    number   = item.addrs[1]
    title    = item.title
    message  = item.message

    try:
        manager = asterisk.manager.Manager()
        manager.connect(host, port)
        response = manager.login(username, password)
        srv.log.debug("Authentication {}".format(response))
        channel = gateway + number
        channel_vars = {'text': message}
        # originate the call
        response = manager.originate(channel, extension, context=context, priority='1', caller_id=extension, variables=channel_vars)
        srv.log.info("Call {}".format(response))
        manager.logoff()
    except asterisk.manager.ManagerSocketException as exc:
        srv.log.error("Error connecting to the manage: %s", exc)
    except asterisk.manager.ManagerAuthException as exc:
        srv.log.error("Error logging in to the manager: %s", exc)
    except asterisk.manager.ManagerException as exc:
        srv.log.error("Error: %s", exc)

    finally:
    # remember to clean up
        try:
            manager.close()
        except asterisk.manager.ManagerSocketException:
            pass

    return True
