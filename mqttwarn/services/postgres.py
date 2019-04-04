#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Based on the mysql.py service by Jan-Piet Mens
#
"""mqttwarn postgres service plugin.

The ``postgres`` plugin saves the MQTT message payload in a PostgreSQL
database table, using the keys of the JSON-decoded payload as column names.

It behaves virtually identically to the MySQL_ plugin and is configured in the
same way. Here's an example service configuration section in ``mqttwarn.ini``:

.. code-block:: ini

    [config:postgres]
    host = 'localhost'
    user = 'username'
    password = 'password'
    database = 'databasename'
    targets = {
            'table1': ['person', 'message', 'schema']
        }

    [pq/1]
    targets = postgres:table1

Suppose we create the following table for the target ``table1`` defined above:

.. code-block:: postgres

    CREATE TABLE person (id INT GENERATED ALWAYS AS IDENTITY, name VARCHAR);

and publish this JSON payload:

.. code-block:: bash

    mosquitto_pub -t pg/1 -m '{"name": "Jane Jolie", "number": 17}'

This will result in the two columns ``id`` and ``name`` being populated:

.. code-block:: postgres

    +----+------------+
    | id | name       |
    +====+============+
    | 1  | Jane Jolie |
    +----+------------+

The second item in a target definition of the service defines a _fallback
column_, into which _mqttwarn_ adds the "rest of" the payload, i.e. all JSON
data, for which no column is found in the table. Lets now add our fallback
column to the schema:

.. code-block:: postgres

    ALTER TABLE person ADD message TEXT;

Publishing the same payload again, will insert this row into the table:

.. code-block:: postgres

    +----+------------+------------------+
    | id | name       | message          |
    +====+============+==================+
    | 1  | Jane Jolie | NULL             |
    +----+------------+------------------+
    | 2  | Jane Jolie | {"number": 17}   |
    +----+------------+------------------+

As you may guess, if we add a ``number`` column to the table, it will be
correctly populated with the value ``17`` as well.

The payload of messages which do not contain valid JSON will be coped verbatim
to the _fallback column_:

.. code-block:: postgres

    +----+------+-------------+--------+
    | id | name | message     | number |
    +====+======+=============+========+
    | 3  | NULL | I love MQTT | NULL   |
    +----+------+-------------+--------+

You can add columns with the names of the built-in transformation data (e.g.
``_dthhmmss``) to have those values stored automatically.

"""

__author__ = """\
Jan-Piet Mens <jpmens()gmail.com>,
Martyn Whitwell <martyn.whitwell()gmail.com>,
Christopher Arndt <info@chrisarndt.de>
"""
__copyright__ = "Copyright 2016 Jan-Piet Mens, Martyn Whitwell, 2019 Christopher Arndt"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"


import json

# http://initd.org/psycopg/
import psycopg2


META_KEYS = set([
    '_dtepoch',
    '_dthhmm',
    '_dthhmmss',
    '_dtiso',
    '_ltiso',
    'payload',
    'raw_payload',
    'topic',
])


class ConfigurationError(Exception):
    pass


class Plugin:
    def __init__(self, srv=None, config=None):
        self.srv = srv
        self.log = srv.log
        self.config = config

    def add_row(self, cursor, schema, tablename, rowdata, message, fallback_col):
        # filter out keys that are not column names
        cursor.execute(
            """
            SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = %s AND table_name = %s;
            """,
            (schema, tablename)
        )
        allowed_keys = set(row[0].lower() for row in cursor.fetchall())

        if not allowed_keys:
            raise ConfigurationError("No columns found in table '%s.%s': unable to proceed." %
                                     (schema, table_name))

        # We want neither the global transformation data (META_KEYS) in the
        # fallback column nor the data for keys, which match columns present
        # in the table.
        data_keys = set(key.lower() for key in rowdata)
        payload_keys = data_keys.difference(META_KEYS)
        usable_keys = allowed_keys.intersection(data_keys)
        unknown_keys = payload_keys.difference(allowed_keys)

        if fallback_col in allowed_keys:
            if unknown_keys:
                if fallback_col not in payload_keys:
                    rowdata[fallback_col] = json.dumps({key: rowdata[key] for key in unknown_keys})
            elif not payload_keys:
                rowdata[fallback_col] = message

            if fallabck_col in rowdata:
                usable_keys.add(fallback_col)
        else:
            self.srv.log.warn("Fallback column '%s' does not exist in table '%s.%s'.",
                              fallback_col, schema, tablename)
            unknown_keys.add(fallback_col)

        columns = ", ".join('"%s"' % name for name in usable_keys)
        values_template = ", ".join(["%s"] * len(usable_keys))

        sql = 'INSERT INTO %s.%s (%s) values (%s);' % (schema, tablename, columns, values_template)
        values = tuple(rowdata[key] for key in usable_keys)

        self.srv.log.debug("Query: %s values: %r unknown keys: %r", sql, values, unknown_keys)
        cursor.execute(sql, values)
        return unknown_keys


    def plugin(self, srv, item):
        srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

        conf = item.config.get
        host = conf("host", "localhost")
        port = conf("port", 5432)
        user = conf("user")
        password = conf("password")
        database = conf("database")

        try:
            # XXX tablename not sanitized
            table_name = item.addrs[0].format(**item.data)
            fallback_col = item.addrs[1].format(**item.data)

            try:
                schema = item.addrs[2].format(**item.data)
            except (LookupError, NameError, ValueError, TypeError):
                schema = "public"
        except Exception as exc:
            self.log.error("postgres target incorrectly configured: %s", exc)
            return False

        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password,
                                    database=database)
            cursor = conn.cursor()
        except Exception as exc:
            self.log.error("Could not connect to postgres data '%s' at '%s:%s': %s",
                           database, host, port, exc)
            return False

        # Create a new dict for column data and fill it with payload JSON data,
        # attempting to format each value with the transformation data.
        col_data = {}

        if item.data is not None:
            for key, value in item.data.items():
                try:
                    col_data[key] = value.format(**item.data)
                except Exception:
                    col_data[key] = value

        try:
            unknown_keys = self.add_row(cursor, schema, table_name, col_data, item.message,
                                        fallback_col)

            if unknown_keys:
                if fallback_col in unknown_keys:
                    self.log.error("Fallback column '%s' not found in table '%s.%s'. "
                                   "*Dropped* values of the following data keys: %s",
                                   fallback_col, schema, table_name, ", ".join(unknown_keys))
                elif fallback_col in item.data:
                    self.log.error("Data for fallback column '%s' already present in payload. "
                                   "*Dropped* values of the following data keys: %s",
                                   fallback_col, ", ".join(unknown_keys))
                else:
                    self.log.warn("Data for keys '%s' written to fallback column '%s'.",
                                  ", ".join(unknown_keys), fallback_col)

            conn.commit()
        except Exception as exc:
            self.log.error("Could not add postgres row: %s", exc)
            return False
        finally:
            cursor.close()
            conn.close()

        return True

    __call__ = plugin
