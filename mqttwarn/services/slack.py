#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__   = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


HAVE_SLACK=True
try:
    from slacker import Slacker
except ImportError:
    HAVE_SLACK=False


def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if HAVE_SLACK == False:
        srv.log.error("Can not use service 'slack': slacker module missing.")
        return False

    # check for service level token
    token = item.config.get('token')

    # get the target tokens
    addrs = item.addrs[:]
    as_user = False

    # check if we have the optional as_user token (extract and remove if so)
    if isinstance(addrs[-1], bool):
        as_user = addrs[-1]
        addrs.pop()

    # check for target level tokens (which have preference)
    try:
        if len(addrs) == 4:
            token, channel, username, icon = addrs
        else:
            channel, username, icon = addrs
    except Exception as exc:
        srv.log.error("Incorrect target configuration for target=%s: %s", item.target, exc)
        return False

    # if no token then fail
    if token is None:
        srv.log.error("No token found for slack.")
        return False

    # if the incoming payload has been transformed, use that,
    # else the original payload
    text = item.message

    try:
        slack = Slacker(token)
        slack.chat.post_message(channel, text, as_user=as_user, username=username, icon_emoji=icon)
    except Exception as exc:
        srv.log.error("Cannot post to slack %s: %s", channel, exc)
        return False

    return True
