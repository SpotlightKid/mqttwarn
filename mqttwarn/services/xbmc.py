# -*- coding: utf-8 -*-

__author__    = 'Ben Jones <ben.jones12()gmail.com>'
__copyright__ = 'Copyright 2014 Ben Jones'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

import urllib
import urllib2
import base64
try:
    import json
except ImportError:
    import simplejson as json

def plugin(srv, item):

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    xbmchost = item.addrs[0]
    xbmcusername = None
    xbmcpassword = None

    if len(item.addrs) == 3:
        xbmcusername = item.addrs[1]
        xbmcpassword = item.addrs[2]

    title    = item.title
    message  = item.message
    image    = item.image

    jsonparams = {
        "jsonrpc" : "2.0",
        "method"  : "GUI.ShowNotification",
        "id"      : 1,
        "params"  : {
            "title"       : title,
            "message"     : message,
            "image"       : image,
            "displaytime" : 10000
        }
    }
    jsoncommand = json.dumps(jsonparams)

    url = 'http://%s/jsonrpc' % (xbmchost)
    try:
        srv.log.debug("Sending XBMC notification to %s [%s]..." % (item.target, xbmchost))
        req = urllib2.Request(url, jsoncommand)
        req.add_header("Content-type", "application/json")
        if xbmcpassword is not None:
            base64string = base64.encodestring ('%s:%s' % (xbmcusername, xbmcpassword))[:-1]
            authheader = "Basic %s" % base64string
            req.add_header("Authorization", authheader)
        response = urllib2.urlopen(req, timeout = 2)
        srv.log.debug("Successfully sent XBMC notification")
    except urllib2.URLError, e:
        srv.log.error("URLError: %s", exc)
        return False
    except Exception as exc:
        srv.log.error("Error sending XBMC notification to %s [%s]: %s" % (item.target, xbmchost, exc))
        return False

    return True
