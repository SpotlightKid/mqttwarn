# -*- coding: utf-8 -*-
# The code for thingspeak plugin for mqttwarn is based on other plugins

from urllib import urlencode
from httplib import HTTPSConnection, HTTPException
from ssl import SSLError

__author__    = 'Marcel Verpaalen'
__copyright__ = 'Copyright 2015 Marcel Verpaalen'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

builddata = {}

def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    build = ""
    message = item.message

    try:
      if len(item.addrs) == 3:
        apikey, field_id , build = item.addrs
      elif len(item.addrs) == 2:
        apikey, field_id = item.addrs
      else:
        srv.log.warn("thingspeak target is incorrectly configured. use 2 or 3 arguments")
        return False
    except:
        srv.log.warn("thingspeak target is incorrectly configured")
        return False

    if isinstance(field_id, basestring):
        # field_id is an actual thingspeak field
        builddata.update({field_id: message.encode('utf-8')})
    else:
        # field_id is an ordered list of parsed message data field names
        try:
            for n, f in enumerate(field_id):
                field = "field%s" % (n+1)
                value = ("{%s}" % f).format(**item.data).encode('utf-8')
                builddata.update({field: value})
        except Exception as exc:
            srv.log.warn("unable to extract fields or values, skipping: %s / %s: %s", field_id, message, exc)
            return False

    if build == "true":
        srv.log.debug("thingspeak content building. Update %s to '%s' stored for later submission." , field_id, message.encode('utf-8'))
        return True

    data = {'api_key': apikey}
    data.update(builddata)
    builddata.clear()

    http_handler = HTTPSConnection("api.thingspeak.com")

    try:
        http_handler.request("POST", "/update",
                         headers={'Content-type': "application/x-www-form-urlencoded"},
                         body=urlencode(sorted(data.items()))
                         )
    except (SSLError, HTTPException), e:
        srv.log.warn("Thingspeak update failed: %s" % exc)
        return False

    response = http_handler.getresponse()
    body = response.read()

    if body == '0':
        srv.log.warn("Thingspeak channel '%s' field '%s' update failed. Reponse: %s, %s, %s" % (item.target, field_id, response.status, response.reason, body))
    else:
        srv.log.debug("Reponse: %s, %s, update: %s" % (response.status, response.reason, body))

    return True
