# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


def plugin(srv, item):
    """file service plugin."""
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)
    mode = 'a'

    # item.config is brought in from the configuration file
    config = item.config

    # addrs is a list[] associated with a particular target.
    # While it may contain more than one item (e.g. pushover)
    # the `file' service carries one only, i.e. a path name
    filename = item.addrs[0].format(**item.data).encode('utf-8')

    # If the incoming payload has been transformed, use that,
    # else the original payload
    text = item.message

    if isinstance(config, dict):
        if config.get('append_newline'):
            text = text + '\n'

        if config.get('overwrite'):
            mode = 'w'

    try:
        with open(filename, mode) as fp:
            fp.write(text)
    except Exception as exc:
        srv.log.warning("Cannot write to file `%s': %s", filename, exc)
        return False

    return True
