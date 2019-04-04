# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import importlib
import pkg_resources
import re

import six


class Struct:
    """Convert Python dict to data object.

    http://stackoverflow.com/questions/1305532/

    """
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __repr__(self):
        return '<Struct:\n%s>' % "\t\n".join("%s: %r" % kv for kv in six.iteritems(self.__dict__))

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def asdict(self):
        return {k: v for k, v in six.iteritems(self.__dict__)}


def is_funcspec(s):
    if s and ':' in s:
        dottedpath, name = s.split(':', 1)

        for identifier in dottedpath.split('.'):
            if not re.match(r'[_a-zA-Z][_a-zA-Z0-9]*$', identifier):
                return False

        return bool(re.match(r'[_a-zA-Z][_a-zA-Z0-9]*\(\)', name))

    return False


def load_function(dottedpath, name, extra_pkgs=None):
    mod = None

    if not name.startswith('.'):
        try:
            mod = importlib.import_module(dottedpath)
        except ModuleNotFoundError:
            pass

    if not mod and extra_pkgs:
        for pkg in extra_pkgs:
            try:
                mod = importlib.import_module(pkg + '.' + dottedpath)
            except ModuleNotFoundError:
                pass
            else:
                break

    if not mod:
        raise ModuleNotFoundError("Could not find module '%s'." % dottedpath)

    func = getattr(mod, name, getattr(mod, name.capitalize(), None))

    if func is None:
        raise ImportError("Could not import '%s' from '%s'" % (name, dottedpath))

    if not callable(func):
        raise TypeError("'%s:%s' is not callable" % (name, dottedpath))

    return func


def get_resource_content(package, filename, encoding='utf-8'):
    with pkg_resources.resource_stream(package, filename) as stream:
        return stream.read().decode(encoding)
