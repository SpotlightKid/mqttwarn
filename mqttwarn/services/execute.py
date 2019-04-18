#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Tobias Brunner <tobias()tobru.ch>, Christopher Arndt"
__copyright__ = "Copyright 2016 Tobias Brunner, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import subprocess
import tempfile


def plugin(srv, item):
    """``execute`` service plugin.

    Launches the specified external program and its arguments.

    The target address must be the command line to run as list of strings, with
    the actual program to run as the fist argument.

    Example:

    .. code-block:: ini

        [config:execute]
        targets = {
                #         argv0, argv1, ..., argvn
                'touch': ['touch', '/tmp/executed']
            }

    Arguments (but not the program name), will be optionally formatted using
    the payload transformation data as the substitution dictionary for
    formatting placeholders. The substitution dictionary also contains the
    notfication message under the key ``message``. This is the MQTT message
    payload, which itself will have been possibly formatted or transformed via
    the ``format`` option of the topic handler, which triggered the ``execute``
    service. By default it will be the payload as a string, decoded using UTF-8
    encoding.

    To enable argument formatting, set the ``format_args`` option in the
    service config section to ``true``.

    Argument formatting example:

    .. code-block:: ini

        [config:execute]
        targets = {
                'volume': ['amixer', 'sset', 'Master', '{message}%']
            }
        format_args = true

        [mixer/master]
        targets: execute:volume

    The working directory for the external program can be set with the ``cwd``
    option. It defaults to the standard temporary directory as returned by
    ``tempfile.gettempdir()``.

    Note, that for each message targeted to the ``execute`` service, a new
    process is spawned (fork/exec), so it is quite resource-intensive.

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if not item.addrs or not item.addrs[0]:
        srv.log.error("No command configured for target '%s'.", item.target)
        return False

    cwd = item.config.get('cwd', tempfile.gettempdir())

    if item.config.get('format_args', False):
        cmd = [item.addrs[0]]
        for arg in item.addrs[1:]:
            try:
                cmd.append(arg.format(message=item.message, **item.data))
            except Exception as exc:
                srv.log.warn("Could not format argument '%s' of target '%s': %s",
                             arg, item.target, exc)
                cmd.append(arg)
    else:
        cmd = item.addrs

    try:
        srv.log.debug("Executing command: %s", cmd)
        # Capture command stdout and stderr output,
        # so that it does not end up on mqttwarn's stdout.
        res = subprocess.check_output(cmd, stdin=None, stderr=subprocess.STDOUT, shell=False,
                                      universal_newlines=True, cwd=cwd)
    except OSError as exc:
        srv.log.error("Cannot execute '%s': %s", cmd, exc)
    except subprocess.CalledProcessError as exc:
        srv.log.error("Command '%s' returned non-zero exit value: %s", cmd, exc.returncode)
        srv.log.debug("Command output: %s", res)
    else:
        return True

    return False
