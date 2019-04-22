# -*- coding: utf-8 -*-

# The code for pushalot() plugin for mqttwarn is based on other plugins
# by Matthew Bordignon @bordignon on twitter 2014

from urllib import urlencode
from httplib import HTTPSConnection, HTTPException
from ssl import SSLError

def plugin(srv, item):

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    apikey = item.addrs[0]

    title = item.get('title', srv.SCRIPTNAME)
    message = item.message

    http_handler = HTTPSConnection("pushalot.com")

    data = {'AuthorizationToken': apikey,
            'Title': title.encode('utf-8'),
            'Body': message.encode('utf-8')
            }
    
    try:
        http_handler.request("POST", "/api/sendmessage",
                         headers={'Content-type': "application/x-www-form-urlencoded"},
                         body=urlencode(data)
                         )
    except (SSLError, HTTPException), e:
        srv.log.warn("Pushalot notification failed: %s" % exc)
        return False            

    response = http_handler.getresponse()

    srv.log.debug("Reponse: %s, %s" % (response.status, response.reason))

    return True 
