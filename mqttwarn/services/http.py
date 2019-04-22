#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "2019 Christopher Arndt"
__license__ = "MIT License"


import requests
import six


def plugin(srv, item):
    """mqttwarn ``http`` service plugin.

    .. code-block:: python

        'target': [
            url,                  # only HTTP and HTTPS are supported
            {
                'method': 'GET',  # Request method, possible values: 'GET', 'POST'
                'data': {},       # Request data dictionary (see below)
                'auth': (),       # Credentials for HTTP Basic Auth, a (username, password) tuple
                'json': False,    # If True, send JSON encoded request data (POST requests only)
                'timeout': 10     # Request timeout in seconds (for connect and response read)
            }
        ]

    Service target addresses must be given as a one or two-item sequence, with the URL to send an
    HTTP(S) request to as the first item and the request parameters as an optional second item.

    All keys in the parameter dict, if given, are optional (default values shown above).

    If the service config option ``format_url`` is true, the URL will be formatted using the
    MQTT payload data transformation dict. Errors on formatting will abort the service call.

    Each value in the request data dictionary (if it is a string) is subjected to string formatting
    using the MQTT payload data transformation dict. Errors on formatting will be ignored and the
    original value is kept as is.

    As a special case, if the value starts with an ``@`` character (e.g. ``'@name'``), it will not
    be formatted via ``.format()``; instead, the value (minus the ``@``) is used as a key in the
    transformation data dictionary to look up the value directly. This can be used, for example, to
    send non-string values in JSON-encoded request data. If the key is not found, the value will be
    set to ``None``. Unless the request data is send JSON-encoded, the corresponding request data
    item then will be not be sent.

    Requires:

    * requests_
    * six_

    .. _requests: https://pypi.org/project/requests/
    .. _six: https://pypi.org/project/six/

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    try:
        url = item.addrs[0]
    except IndexError:
        srv.error("Service target '%s:%s' has no URL configured.", item.service, item.target)
        return False

    try:
        params = item.addrs[1]
    except IndexError:
        params = {}

    method = params.get('method', 'GET').upper()
    data = params.get('data')
    auth = params.get('auth', False)
    use_json = params.get('json', False)
    timeout = params.get("timeout", 10)
    kwargs = {'headers': {'User-agent': srv.SCRIPTNAME}}

    if auth and isinstance(auth, (tuple, list)) and len(auth) == 2:
        kwargs['auth'] = auth

    if item.config.get('format_url', False):
        # Try to transform the URL if the service's 'format_url' is truthy.
        # Fail service call on errors.
        try:
            url = url.format(**item.data)
        except Exception as exc:
            srv.log.debug("URL cannot be formatted: %s", exc)
            return False

    if data is not None:
        for key, value in data.items():
            if not isinstance(value, six.string_types):
                continue

            # Request data dict value starts with '@':
            # use it as a key to look up the actual value in the MQTT
            # payload data transformation dict.
            if value.startswith('@'):  # '@message'
                data[key] = item.data.get(value[1:], None)
            else:
                try:
                    data[key] = value.format(**item.data)
                except Exception as exc:
                    srv.log.debug("Parameter '%s' cannot be formatted: %s", key, exc)
                    return False

    if method == 'GET':
        if data is not None:
            kwargs['params'] = data
    elif method == 'POST':
        if data:
            kwargs['json' if use_json else 'data'] = data
        else:
            kwargs['data'] = item.message
    else:
        srv.log.warn("Unsupported HTTP method: %s", method)
        return False

    try:
        srv.log.debug("kwargs: %r", kwargs)
        response = requests.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
    except Exception as exc:
        srv.log.warn("%s request to %s failed: %s", method, url, exc)
        return False

    return True
