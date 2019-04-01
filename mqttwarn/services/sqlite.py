#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

try:
    import sqlite3
except ImportError:
    sqlite3 = None


def plugin(srv, item):
    """sqlite service plugin.

    Records MQTT payload in an SQLite database table with a column called 'payload' of type TEXT.

    Expects addrs to contain (path, tablename).

    """
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if sqlite3 is None:
        srv.log.warn("sqlite3 is not installed.")
        return False

    path = item.addrs[0]
    table = item.addrs[1]

    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        srv.log.warn("Cannot connect to sqlite at '%s': %s", path, exc)
        return False

    try:
        with conn:
            conn.execute('CREATE TABLE IF NOT EXISTS "%s" (payload TEXT)' % table)
    except sqlite3.Error as exc:
        srv.log.warn("Cannot create sqlite table '%s' in '%s': %s", table, path, exc)
        conn.close()
        return False

    try:
        with conn:
            conn.execute('INSERT INTO "%s" VALUES (?)' % table, (item.message,))
    except sqlite3.Error as exc:
        srv.log.warn("Cannot INSERT INTO sqlite table '%s': %s", table, exc)
    finally:
        conn.close()

    return True
