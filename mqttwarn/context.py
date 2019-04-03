# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import logging

import six

from .util import is_funcspec, load_function


log = logging.getLogger(__name__)


class RuntimeContext(object):
    """Global runtime context object.

    This carries runtime information and provides the core with essential
    methods for accessing the configuration and for invoking parts of the
    transformation machinery.

    """
    def __init__(self, config):
        self.config = config

    def get_handler_sections(self):
        sections = []

        for section in self.config.sections():
            if section == 'defaults':
                continue

            if section.startswith('cron:'):
                continue

            if section == 'failover':
                continue

            if section.startswith('config:'):
                continue

            if self.config.has_option(section, 'targets'):
                sections.append(section)
            else:
                log.warn("Section '%s' has no targets defined.", section)

        return sections

    def get_handler_topic(self, section):
        if self.config.has_option(section, 'topic'):
            return self.config.get(section, 'topic')

        return section

    def get_config(self, section, name):
        if self.config.has_option(section, name):
            return self.config.g(section, name)

    def get_handler_targets(self, section):
        """Return list of topic handler targets or function to dynamically get targets later.

        Targets are returned as two-element tuples (service, target), where service is the
        name of the service section to use and target a key of the targets defined by that
        service. Target may be None or an empty string.

        Returns None if no targets are specified or the targets function cannot be imported.

        """
        value = self.config.g(section, 'targets', fallback=None)

        if is_funcspec(value):
            dottedpath, funcname = value.split(':', 1)

            try:
                return load_function(dottedpath, funcname)
            except Exception as exc:
                log.warn("Could not import topic targets function '%s' defined in section "
                         "'%s': %s", value, section, exc)
        elif isinstance(value, six.string_types):
            targetlist = [item.strip() for item in value.split(',')]
            value = []

            for service in targetlist:
                if isinstance(service, six.string_types):
                    try:
                        service, target = service.split(':', 1)
                    except ValueError:
                        target = None
                else:
                    service, target = service[:2]

                value.append((service, target or None))

            return value
        elif isinstance(value, dict):
            for topic, targetlist in list(value.items()):
                value[topic] = []

                if isinstance(targetlist, six.string_types):
                    targetlist = [targetlist]

                for service in targetlist:
                    if isinstance(service, six.string_types):
                        try:
                            service, target = service.split(':', 1)
                        except ValueError:
                            target = None
                    else:
                        service, target = service[:2]

                    value[topic].append((service, target or None))

            return value

    def get_handler_config(self, section):
        return self.config[section]

    def get_service_config(self, service):
        return self.config.config('config:' + service)

    def get_service_module(self, service):
        return self.config.g('config:' + service, 'module', fallback=service)

    def get_service_targets(self, service):
        try:
            return self.config.getdict('config:' + service, 'targets')
        except Exception as exc:
            log.error("No valid targets defined for service '%s': %s", service, exc)
