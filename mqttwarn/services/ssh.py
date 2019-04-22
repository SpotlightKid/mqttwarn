# -*- coding: utf-8 -*-

import os
from shlex import quote

import paramiko
import six


__author__ = "David Ventura, Christopher Arndt <info@chrisarndt.>"
__copyright__ = "Copyright 2016 David Ventura, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


def credentials(host, user=None, password=None, port=22):
    creds = {"hostname": host, "port": port, "username": user, "password": password}
    ssh_config_path = os.path.expanduser("~/.ssh/config")

    if user is None and os.path.exists(ssh_config_path):
        ssh_config = paramiko.SSHConfig()

        with open(ssh_config_path) as fp:
            ssh_config.parse(open(fp))

        host_config = ssh_config.lookup(host)
        ident = host_config.get("identityfile")

        if isinstance(ident, list):
            ident = ident[0]

        creds = {
            "hostname": host_config["hostname"],
            "port": int(host_config.get("port", port)),
            "username": host_config.get("user", user),
            "password": password,
            "key_filename": ident,
        }

    return creds


def plugin(srv, item):
    """ssh service plugin.

    The ``ssh`` service can run commands over ssh. If both user and password are defined in the
    service config, they will be used to connect to the host. If no user is defined in the service
    config, the service will parse the user's ssh config file (``~/.ssh/config``) to see which SSH
    key (``IdentityFile``) to use for the given host name. The password, if set, will be used as
    the passphrase to unlock the key. If the SSH config also defines a `User` and `Port` for the
    given host name, they will be used too.

    If using a key, only the host is required, but if no username is set either via the service or
    the SSH config, the connection will use the username of the user the mqttwarn process runs as,
    which may not be what is intended. If the SSH config

    .. note:: using this module lets you specify a username and a password, which can be used to
        login to the target system. As such, your ``mqttwarn.ini`` configuration file should be
        well protected from prying eyes! (This applies generally, for other target specifications
        with credentials as well.)

        Also, anyone, who can publish to the MQTT topics defined by the topic handlers for this
        service can run trigger execution of the remote commands without further authetication.
        Make sure, the ACLfor these topics on your MQTT broker are set up accordingly.

    Each service target specifies *one* command to execute on the remote host as a string. The
    string can contain placeholders for arguments (using ``str.format()`` syntax). The arguments
    will be obtained from the MQTT message payload: if the payload can be decoded from JSON to a
    dictionary, the value of the ``args`` key, which should be a list, is used. Otherwise the
    payload interpreted as a UTF-8 encoded string (possibly transformed via a ``format`` option in
    the topic handler section) is used as a single argument. Arguments are quoted using
    ``shlex.quote`` before being substituted.

    The output of the command is ignored.

    Configuration example::

        [config:ssh]
        host  = 192.168.1.1
        port  = 22
        user  = username
        password  = password
        targets = {
                'addnote': ['echo {} >> notes.txt'],
                'addwebuser': ['htpasswd -b ~/etc/htpasswd.txt {} {}']
            }

        [ssh/onearg/+]
        format = New entry {_dtiso}: {payload}
        targets = ssh:addnote

        [ssh/twoargs/+]
        targets = ssh:addwebuser

    Example published to trigger the each topic handler defined in the example above::

        mosquitto_pub -t ssh/onearg/test -m 'alice'
        mosquitto_pub -t ssh/twoargs/test -m '{"args": ["bob", "mysecret"]}'

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)
    conf = item.config.get
    host = conf("host", "localhost")
    port = conf("port", 22)
    user = conf("user")
    password = conf("password")
    environment = conf("environment")
    timeout = conf("timeout")
    command = item.addrs[0]
    args = item.data.get("args", item.message)

    if isinstance(args, six.string_types):
        args = [args]

    if isinstance(args, (list, tuple)):
        args = tuple(quote(v) for v in args)  # escape the shell args

    command = command.format(*args)
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    creds = credentials(host, user=user, password=password, port=port)


    try:
        ssh.connect(**creds)
    except Exception as exc:
        srv.log.warning("Could not connect to host '%s': %s", host, exc)
        return False

    try:
        srv.log.debug("Executing command '%s' on host '%s'.", command, host)
        _, stdout, stderr = ssh.exec_command(command, timeout=timeout, environment=environment)
    except Exception as exc:
        srv.log.warning("Could not run command '%s' on host '%s': %s", command, host, exc)
        return False
    finally:
        ssh.close()

    return True
