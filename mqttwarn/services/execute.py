#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Tobias Brunner <tobias()tobru.ch>, Christopher Arndt"
__copyright__ = "Copyright 2016 Tobias Brunner, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import subprocess
import tempfile


def plugin(srv, item):
    """excute service plugin.

    Launches the specified external program and its arguments.

    Target address must be the command line to run as list of strings,
    with the actual program to run as the fist argument.

    Example:

        [config:execute]
        targets = {
                         # argv0, argv1, ..., argvn
                'touch': ['touch', '/tmp/executed']
           }

    To pass the published MQTT payload to the command, use the ``[TEXT]``
    placeholder as (part of) the value of any argument and all occurences
    of this placeholder will be replaced with the payload as string
    (decoded using UTF-8 encoding).

    The placeholder string can be changed with the ``text_replace`` option.


        [config:execute]
        targets = {
                          # argv0, argv1, ..., argvn
                'volume': ['amixer', 'sset', 'Master', '[vol]']
           }
        text_replace = '[vol]'

        [mixer/master]
        targets: execute:volume

    The working directory for the external program can be set with the the
    `cwd` option. It defaults to the standard temporary directory as returned
    by ``tempfile.gettempdir()``.

    """
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    replace = item.config.get('text_replace', '[TEXT]')
    cwd = item.config.get('cwd', tempfile.gettempdir())
    text = item.message
    cmd = [arg.replace(replace, text) for arg in item.addrs]

    try:
        # Capture command stdout and stderr output,
        # so that it does not end up on mqttwarn's stdout.
        res = subprocess.check_output(cmd, stdin=None, stderr=subprocess.STDOUT, shell=False,
                                       universal_newlines=True, cwd=cwd)
    except OSError as exc:
        srv.logging.error("Cannot execute '%s': %s", cmd, exc)
    except subprocess.CalledProcessError as exc:
        srv.logging.error("Command '%s' returned non-zero exit value: %s", cmd, exc.returncode)
        srv.logging.debug("Command output: %s", res)
    else:
        return True

    return False
