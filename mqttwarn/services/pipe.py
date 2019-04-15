#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>, Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2014 Jan-Piet Mens, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import subprocess
import tempfile


def plugin(srv, item):
    """``pipe`` service plugin.

    The ``pipe`` service launches the specified program and its arguments and
    pipes the (possibly formatted) message to the program's *stdin*. If the
    message doesn't have a trailing newline (``\n``), the service appends one.

    The target address must be the command line to run as list of strings, with
    the actual program to run as the fist argument.

    Example:

    .. code-block:: ini

        [config:pipe]
        targets = {
                #      argv0, argv1 ...
                'wc': ['wc',  '-l']
            }

    Arguments (but not the program name), will be optionally formatted using
    the payload transformation data as the substitution dictionary for
    formatting placeholders. To enable argument formatting, set the
    ``format_args`` option in the service config section to ``true``.

    Argument formatting example:

    .. code-block:: ini

        [config:pipe]
        targets = {
                'mail': ['mail', '-s' '{subject}' '{to}']
            }
        format_args = true

        [/mail/send]
        targets = pipe:mail
        format = {body}

    With this configuration the following JSON payload published to
    `/mail/send`` would result in an email message with the subject "What's
    up?" and the body "Wanna hang out?" to be sent to the recipient
    "joe@example.com" (assuming a local MTA is correctly set up):

    .. code-block:: json

        {"to": "joe@example.com", "subject": "What's up?", "body": "Wanna hang out?"}

    The working directory for the external program can be set with the ``cwd``
    option. It defaults to the standard temporary directory as returned by
    ``tempfile.gettempdir()``.

    Note, that for each message targeted to the ``pipe`` service, a new process
    is spawned (fork/exec), so it is quite resource-intensive.

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if not item.addrs or not item.addrs[0]:
        srv.log.error("No command configured for target '%s'.", item.target)
        return False

    cwd = item.config.get('cwd', tempfile.gettempdir())
    text = item.message
    if not text.endswith('\n'):
        text = text + '\n'

    if item.config.get('format_args', False):
        cmd = [item.addrs[0]]
        for arg in item.addrs[1:]:
            try:
                cmd.append(arg.format(**item.data))
            except Exception as exc:
                srv.log.warn("Could not format argument '%s' of target '%s': %s",
                             arg, item.target, exc)
                cmd.append(arg)
    else:
        cmd = item.addrs

    try:
        srv.log.debug("Executing pipe command: %s", cmd)
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, close_fds=True, shell=False,
                                universal_newlines=True, cwd=cwd)
    except Exception as exc:
        srv.log.warn("Cannot create pipe: %s", exc)
        return False

    try:
        proc.stdin.write(text)
    except IOError as exc:
        srv.log.warn("Cannot write to pipe: errno %d", exc.errno)
        return False
    except Exception as exc:
        srv.log.warn("Cannot write to pipe: %s", exc)
        return False

    proc.stdin.close()
    proc.wait()
    return True
