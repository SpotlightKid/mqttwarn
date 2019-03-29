#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Vium <https://github.com/Vium>, Christopher Arndt <info@chrisarndt.de>"
__copyright__ = "Copyright 2016, 2019"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

# Based on the great SQLITE code by Jan-Piet Mens

import unicodedata

import six

try:
    import sqlite3
except ImportError:
    sqlite3 = None


def normalize_unicode(s, encoding='ascii'):
    return unicodedata.normalize('NFKD', s).strip().encode(encoding, 'ignore').decode(encoding)


def plugin(srv, item):
    """sqlite_json2cols service plugin.

    Expects addrs to contain (path, tablename) and payload to be a JSON dict with string
    or numeric values.

    """
    srv.logging.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    if sqlite3 is None:
        srv.logging.warn("sqlite3 is not installed")
        return False

    path = item.addrs[0]
    table = item.addrs[1]
    data = item.data

    if not data or not isinstance(data, dict):
        srv.logging.warn("Incorrect payload format (must be dict).")

    col_names = []
    col_definitions = []
    col_values = []

    # Determine column types from MQTT payload JSON data.
    # Data is expected to be a dict mapping column names to values, e.g.
    # {"sensor_id": "testsensor", "whatdata": "hello", "data": 1}
    for key in data:
        srv.logging.debug("Key: %r", key)
        key = key.strip()

        if not key or key.startswith('_') or key in ('payload', 'topic'):
            # We just want to save the payload.
            # There is probably a better way
            continue

        value = data[key]

        if isinstance(value, six.integer_types):
            col_type = 'float'
        elif isinstance(value, six.string_types):
            col_type = 'varchar(20)'

        col_name = normalize_unicode(key)

        if col_name:
            col_definitions.append('"%s" %s' % (col_name, col_type))
            col_names.append(col_name)
            col_values.append(value)

    if not col_names:
        srv.logging.warn("No valid column fields found in payload")
        return False

    # Derive SQL table column definition string in case the table has to be created
    # i.e. 'key1 varchar(20), key2 varchar(20), key3 float'
    col_definitions = ", ".join(col_definitions)

    # Collect column names for INSERT query string
    col_names = ', '.join('"%s"' % name for name in col_names)

    # Collect column values and build INSERT query placeholder string
    col_placeholders = ", ".join(["?"] * len(col_values))

    try:
        conn = sqlite3.connect(path)
    except sqlite3.Error as exc:
        srv.logging.warn("Cannot connect to sqlite at '%s': %s", path, exc)
        return False

    try:
        query = 'CREATE TABLE IF NOT EXISTS "%s" (%s);' % (table, col_definitions)
        srv.logging.debug("Creating SQLite table: %s", query)
        with conn:
            conn.execute(query)
    except sqlite3.Error as exc:
        srv.logging.warn("Cannot create sqlite table '%s' in '%s': %s", table, path, exc)
        conn.close()
        return False
    else:
        srv.logging.debug("SQLite CREATE TABLE sucessful")

    try:
        query = 'INSERT INTO "%s" (%s) VALUES (%s);' % (table, col_names, col_placeholders)
        srv.logging.debug("Insert into SQLite: %s %% %r", query, tuple(col_values))
        with conn:
            conn.execute(query, col_values)
    except sqlite3.Error as exc:
        srv.logging.warn("Cannot INSERT INTO sqlite table '%s': %s", table, exc)
    else:
        srv.logging.debug("SQLite INSERT successful.")
    finally:
        conn.close()

    return True
