# -*- coding: utf-8 -*-

__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2019 Christopher Arndt"
__license__ = "MIT License"


from .oneshotxmppclient import send_message


def plugin(srv, item):
    """mqttwarn ``xmpp`` service plugin.

    Sends the (possibly formatted) MQTT message payload to one or more XMPP
    recipients.

    The service target address must be a list of recipients' JIDs.

    The service configuration section must also set the JID and password of
    the the sender.

    Example:

    .. code-block:: ini

        [config:xmpp]
        jid = myjabber@example.com/mqttwarn
        password = letmeinplease
        targets = {
                'mybuddies': ['joe@example.org',  'alice@bopton.com']
            }

        [xmpp/mybuddies]
        targets = xmpp:mybuddies
        format = mqttwarn notification at {_lthhmmss}: {payload}

    """
    target = item.target
    service = item.service
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, service, target)

    recipients = item.addrs
    text = item.message
    jid = item.config.get('jid')
    password = item.config.get('password')

    if not jid or not password:
        srv.log.error("Invalid configuration for XMPP service target '%s:%s': sender JID or "
                      "password not set.", service, target)

    if not recipients:
        srv.log.warn("Invalid configuration for XMPP service target '%s:%s': no recipients "
                     "configured.", service, target)
        return False

    try:
        srv.log.debug("Sending XMPP message to: %s", ", ".join(recipients))
        for target in recipients:
            send_message(jid, password, recipients, text)
    except Exception as exc:
        srv.log.exception("Error sending XMPP message to %s: %s", target, exc)
        return False
    else:
        srv.log.debug("Successfully sent XMPP message.")

    return True
