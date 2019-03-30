# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import sys
import ast
import logging

from io import open

try:
    from configparser import RawConfigParser, NoOptionError
except ImportError:
    from ConfigParser import RawConfigParser, NoOptionError

HAVE_TLS = True
try:
    import ssl
except ImportError:
    HAVE_TLS = False


logger = logging.getLogger(__name__)


class Config(RawConfigParser):
    """RawConfigParser wrapper providing defaults and custom config value access methods."""

    specials = {
        'TRUE': True,
        'FALSE': False,
        'NONE': None,
    }

    loglevels = {
        'CRITICAL': 50,
        'DEBUG': 10,
        'ERROR': 40,
        'FATAL': 50,
        'INFO': 20,
        'NOTSET': 0,
        'WARN': 30,
        'WARNING': 30,
    }

    def __init__(self, configuration_file, defaults=None):
        defaults = defaults or {}
        super(Config, self).__init__()

        with open(configuration_file, 'r', encoding='utf-8') as fp:
            self.readfp(fp)

        # Set defaults
        self.hostname = 'localhost'
        self.port = 1883
        self.transport = 'tcp'
        self.username = None
        self.password = None
        self.client_id = None
        self.lwt = None
        self.skipretained = False
        self.clean_session = False
        self.protocol = 4

        self.logformat = '%(asctime)-15s %(levelname)-8s [%(name)-25s] %(message)s'
        self.logfile = None
        self.loglevel = 'DEBUG'

        self.functions = None
        self.num_workers = 1

        self.directory = '.'
        self.ca_certs = None
        self.tls_version = None
        self.certfile = None
        self.keyfile = None
        self.tls_insecure = False
        self.tls = False

        self.__dict__.update(defaults)
        self.__dict__.update(self.config('defaults'))

        if not HAVE_TLS:
            logger.error("TLS (SSL) parameters set but no ssl module TLS.")
            sys.exit(2)

        if self.ca_certs is not None:
            self.tls = True

        if self.tls_version is not None:
            if self.tls_version == 'tlsv1_2':
                self.tls_version = ssl.PROTOCOL_TLSv1_2
            if self.tls_version == 'tlsv1_1':
                self.tls_version = ssl.PROTOCOL_TLSv1_1
            if self.tls_version == 'tlsv1':
                self.tls_version = ssl.PROTOCOL_TLSv1
            if self.tls_version == 'sslv3':
                self.tls_version = ssl.PROTOCOL_SSLv3

        self.loglevelnumber = self.level2number(self.loglevel)

    def level2number(self, level):
        return self.loglevels.get(level.upper(), self.loglevels['DEBUG'])

    def g(self, section, key, default=None):
        try:
            val = self.get(section, key)
        except NoOptionError:
            return default

        if val.upper() in self.specials:
            return self.specials[val.upper()]

        try:
            return ast.literal_eval(val)
        except ValueError:
            # e.g. %(xxx)s in string
            return val
        except SyntaxError:
            # If not a valid Python literal, e.g. list of targets comma-separated
            return val

    def getlist(self, section, key):
        """Return a list, return None if key is not present in section."""
        try:
            val = self.get(section, key)
            return [s.strip() for s in val.split(',')]
        except Exception as exc:
            logger.warn("Expecting a list in section '%s', key '%s': %s", section, key, exc)
            return None

    def getdict(self, section, key):
        try:
            data = self.g(section, key)
            if not isinstance(data, dict):
                raise TypeError("Option value %r is not a dictionary." % (data,))
        except Exception as exc:
            logger.warn("Expecting a dict in section '%s', key '%s': %s", section, key, exc)
        else:
            return data

    def config(self, section):
        """Convert a whole section's options into a dict.

        E.g. turns::

            [config:mqtt]
            host = 'localhost'
            username = None
            list = [1, 'aaa', 'bbb', 4]

        into::

            {'username': None, 'host': 'localhost', 'list': [1, 'aaa', 'bbb', 4]}

        Ẃe cannot use ``config.items()`` because we want each value to be retrieved with method
        ``g()`` defined above.

        Options named 'targets' and 'module' are excluded from the returned dict.

        If the given section does not exist, returns None.

        """
        if self.has_section(section):
            return {key: self.g(section, key)
                    for key in self.options(section) if key not in ('targets', 'module')}
