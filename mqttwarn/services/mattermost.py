#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__    = 'Jan-Piet Mens <jp@mens.de>'
__copyright__ = 'Copyright 2018 Jan-Piet Mens'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

import six

HAVE_REQUESTS = True
try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    HAVE_REQUESTS = False

try:
    import json
except ImportError:
    import simplejson as json


def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if HAVE_REQUESTS == False:
        srv.log.error("Missing module: requests")
        return False

    hook_url    = item.addrs[0]
    channel     = item.addrs[1]
    username    = item.addrs[2]    # may be None
    icon_url    = item.addrs[3]    # may be None

    text        = item.message
    try:
        """ Try to format a Markdown table if we have JSON in the payload """
        """ ETOOMESSY; volunteers to refactor? """
        title = item.get('title', None)
        j = json.loads(text)
        keylen = vallen = 10
        for key in j:
            # print type(key), keylen, len(key)
            if isinstance(key, six.string_types) and keylen < len(key):
                keylen = len(key)
            if isinstance(j[key], six.string_types) and vallen < len(j[key]):
                vallen = len(j[key])
        s = ""
        if title is not None and title != "":
            s = "## %s\n" % title
        key = "key"
        val = "value"
        s = s + "| {0:<{kw}}  | {1:<{vw}} |\n".format("key", "value", kw=keylen, vw=vallen)
        s = s + "|:{0:<{kw}}  |:{1:<{vw}} |\n".format('-'*keylen, '-'*vallen, kw=keylen, vw=vallen)
        for key in j:
            s = s + "| {0:<{kw}}  | {1:<{vw}} |\n".format(key, j[key], kw=keylen, vw=vallen)
        text = s
    except Exception as exc:
        srv.log.debug("not JSON; proceeding with text")
        pass


    payload = {}
    payload["channel"]     = channel
    payload["text"]        = text
    if username is not None:
        payload["username"]    = username
    if icon_url is not None:
        payload["icon_url"]    = icon_url

    # print payload

    headers = {
        "Content-type" : "application/json",
        "Accept"       : "application/json"
        }

    try:
        r = requests.post(hook_url, data=json.dumps(payload), headers=headers)
        if r.status_code != requests.codes.ok:
            srv.log.warning("Invalid response from Mattermost Webhook: %s" % (r.text))
            return False
    except Exception as exc:
        srv.log.warning("Failed to POST request to Mattermost Webhook: %s", exc)
        return False

    return True
