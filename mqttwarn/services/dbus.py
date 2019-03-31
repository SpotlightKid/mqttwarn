#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import six

try:
    import pydbus
except ImportError:
    pydbus = None


def plugin(srv, item):
    """Send a message through dbus to the user's desktop.

    Example configuration::

        [config:notify]
        module = 'dbus'
        targets = {
                # target: [method, arg0, arg1, ..., argn]
                'warn': ['Notify', '{service}', 0, '{image}', 'Warning', '{message}', [], [], 1000],
                'note': ['Notify', '{service}', 0, '{image}', 'Note', '{message}', [], [], 1000],
            }

        # You only need the following settings, if you want to use DBUS
        # services other than the default one.

        # This is the name of the default DBUS service used.
        service = 'org.freedesktop.Notifications'
        # If path is not set, it is derive from the DBUS service name
        path = 'org.freedesktop.Notifications'
        # If interface is not set, it is the same as the DBUS service name
        interface = 'org.freedesktop.Notifications'

        [notify/warn]
        targets = notify:warn
        image = '/usr/share/icons/gnome/32x32/places/network-server.png'

    """
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if not pydbus:
        srv.logging.error("Cannot send DBUS message; 'pydbus' module not installed.")
        return False

    try:
        cfg = item.config
        method = item.addrs[0]
        data = {
            'image': item.get('image'),
            'message': item.message,
            'service': item.service,
            'timeout': cfg.get('expire_timeout', 5000),
            'title': item.get('title') or srv.SCRIPTNAME,
        }
        data.update(item.data)
        args = [arg.format(**data) if isinstance(arg, six.string_types) else arg
                for arg in item.addrs[1:]]

        service = cfg.get('service', 'org.freedesktop.Notifications')
        path = cfg.get('path')
        interface = cfg.get('interface', service)
    except Exception as exc:
        srv.logging.error("Error setting up dbus service %s: %s", item.target, exc)

    try:
        srv.logging.debug("Sending message to %s...", item.target)
        session_bus = pydbus.SessionBus()
        obj = session_bus.get(service, path)
        interface = obj[interface]
        getattr(interface, method)(*args)
    except AttributeError:
        srv.logging.error("DBUS interface has not method '%s'.", interface, method)
    except Exception as exc:
        srv.logging.error("Error sending message to %s: %s", item.target, exc)
    else:
        srv.logging.debug("Successfully sent message.")
        return True

    return False
