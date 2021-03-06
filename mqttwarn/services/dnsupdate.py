# -*- coding: utf-8 -*-

__author__    = 'Jan-Piet Mens <jpmens()gmail.com>'
__copyright__ = 'Copyright 2015 Jan-Piet Mens'
__license__   = """Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"""

HAVE_DNS = True
try:
    import dns.update
    import dns.query
    import dns.tsigkeyring
except ImportError:
    HAVE_DNS = False

def plugin(srv, item):

    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if HAVE_DNS == False:
        return False

    config   = item.config

    dns_nameserver = config['dns_nameserver']
    dns_keyname    = config['dns_keyname']
    dns_keyblob    = config['dns_keyblob']

    try:
        zone, domain, ttl, rrtype = item.addrs
    except:
        srv.log.error("Incorrect target configuration for {0}/{1}".format(item.service, item.target))
        return False

    text = item.message

    try:
        keyring = dns.tsigkeyring.from_text( { str(dns_keyname) : str(dns_keyblob) });

        update = dns.update.Update(zone,
            keyring = keyring,
            keyname = dns_keyname,
            keyalgorithm = 'hmac-sha256')   #FIXME configurable

        if rrtype.upper() == 'TXT':
            text = '"%s"' % text

        update.replace(domain, ttl, rrtype, text)
        response = dns.query.tcp(update, dns_nameserver, timeout=10)

        srv.log.debug("DNS Update: {0}".format(dns.rcode.to_text(response.rcode())))
    except Exception as exc:
        srv.log.warning("Cannot notify to dnsupdate `%s': %s" % (dns_nameserver, exc))
        return False

    return True
