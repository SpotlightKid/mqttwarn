#!/usr/bin/env python
# -*- coding: utf-8 -*-

# __originalauthor__ = "Jan-Piet Mens <jpmens()gmail.com>"
__author__ = "Kuthullu Himself <kuthullu()gmail.com>"
__copyright__ = "Copyright 2016 Kuthullu Himself"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


from datetime import datetime

try:
    import sqlite3
except ImportError:
    sqlite3 = None


def plugin(srv, item):
    """sqlite_timestamp service plugin.

    Records MQTT payload in an SQLite database table with an auto-incrementing integer 'id' column,
    a column called 'payload' of type TEXT and a 'timestamp' column of type 'DATETIME', with
    the UTC timestamp of when the record is written.

    Expects addrs to contain (path, tablename).

    """
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if sqlite3 is None:
        srv.logging.warn("sqlite3 is not installed")
        return False

    path = item.addrs[0]
    table = item.addrs[1]

    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        srv.logging.warn("Cannot connect to sqlite at %s: %s", path, exc)
        return False

    try:
        with conn:
            conn.execute('CREATE TABLE IF NOT EXISTS "%s" (id INTEGER PRIMARY KEY AUTOINCREMENT, '
                         'payload TEXT, timestamp DATETIME NOT NULL)' % table)
    except sqlite3.Error as exc:
        srv.logging.warn('Cannot create sqlite table "%s" in %s : %s', table, path, exc)
        conn.close()
        return False

    try:
        with conn:
            conn.execute('INSERT INTO "%s" VALUES (NULL, ?, ?)' % table,
                         (item.message, datetime.utcnow()))
    except sqlite3.Error as exc:
        srv.logging.warn("Cannot INSERT INTO sqlite table '%s': %s", table, exc)
    finally:
        conn.close()

    return True
