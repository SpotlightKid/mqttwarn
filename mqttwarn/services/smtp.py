# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>, Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2014 Jan-Piet Mens, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def plugin(srv, item):
    """Send a message to SMTP recipient(s)."""
    service = item.service
    target = item.target
    topic = item.topic

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, service, target)

    conf = item.config.get
    sender = conf('sender')

    if not sender:
        srv.log.warn("Skipping SMTP notification for service '%s': no sender configured.", service)
        return False

    recipients = item.addrs

    if not recipients:
        srv.log.warn("Skipping SMTP notification for service '%s:%s' on topic '%s': "
                     "no recipients configured.", service, target, topic)
        return False

    server = conf('server', 'localhost')
    usessl = conf('usessl', False)
    starttls = conf('starttls', False)
    port = conf('port', 465 if usessl else 587)
    username = conf('username')
    password = conf('password')

    if conf('html', False):
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(item.message, 'plain'))
        msg.attach(MIMEText(item.message, 'html'))
    else:
        msg = MIMEText(item.message, 'plain')

    msg['Subject'] = item.get('title', srv.SCRIPTNAME + " notification")
    msg['To'] = ", ".join(recipients)
    msg['From'] = sender
    msg['X-Mailer'] = srv.SCRIPTNAME

    try:
        srv.log.debug("Sending SMTP notification for service '%s:%s' on topic '%s'. "
                      "Recipients: %r", service, target, topic, recipients)

        if usessl:
            server = smtplib.SMTP_SSL(server, port)
        else:
            server = smtplib.SMTP(server, port)

        server.set_debuglevel(conf('debuglevel', 0))
        server.ehlo()

        if not usessl and starttls:
            server.starttls()

        if username:
            server.login(username, password)

        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        srv.log.info("Successfully sent SMTP notification.")
    except Exception as exc:
        srv.log.warn("Error sending SMTP notification for service '%s:%s', on topic '%s'. "
                     "Recipients: %r Error: %s", service, target, recipients, exc)
        return False

    return True
