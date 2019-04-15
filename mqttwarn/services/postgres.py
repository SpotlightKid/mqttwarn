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
    minconn = 0                 ; initial/minimum number of database pool connections
    maxconn = 4                 ; maximum number of concurrent database pool connections
    targets = {
            'table1': ['public.person', 'message']
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
from contextlib import contextmanager
from threading import BoundedSemaphore

# http://initd.org/psycopg/
import psycopg2
from psycopg2.pool import ThreadedConnectionPool

META_KEYS = set([
    '_dt',
    '_dtepoch',
    '_dtiso',
    '_lt',
    '_lthhmm',
    '_lthhmmss',
    '_ltiso',
    'payload',
    'raw_payload',
    'topic',
])


class ConfigurationError(Exception):
    pass


# https://stackoverflow.com/a/53437049/390275

class BlockingThreadedConnectionPool(ThreadedConnectionPool):
    def __init__(self, minconn=0, maxconn=4, *args, **kwargs):
        self._semaphore = BoundedSemaphore(maxconn)
        super().__init__(minconn, maxconn, *args, **kwargs)

    def getconn(self, *args, **kwargs):
        self._semaphore.acquire()
        return super().getconn(*args, **kwargs)

    def putconn(self, *args, **kwargs):
        super().putconn(*args, **kwargs)
        self._semaphore.release()


class Plugin:
    def __init__(self, srv=None, config=None):
        self.srv = srv
        self.log = srv.log
        self.config = config
        conf = self.config.get
        self.host = conf("host", "localhost")
        self.port = conf("port", 5432)
        self.user = conf("user")
        self.password = conf("password")
        self.database = conf("database")
        self.minconn = conf("minconn", 0)
        self.maxconn = conf("maxconn", 4)
        self.db = BlockingThreadedConnectionPool(
            minconn=self.minconn,
            maxconn=self.maxconn,
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )

    def close(self):
        self.db.closeall()

    __del__ = close

    @contextmanager
    def get_connection(self):
        con = self.db.getconn()
        try:
            yield con
        finally:
            self.db.putconn(con)

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

            if fallback_col in rowdata:
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

        try:
            # XXX tablename not sanitized
            try:
                schema, table_name = item.addrs[0].split('.', 1)
            except ValueError:
                table_name = item.addrs[0]
                schema = 'public'

            table_name = table_name.format(**item.data)
            schema = schema.format(**item.data)
            fallback_col = item.addrs[1].format(**item.data)
        except (LookupError, NameError, ValueError, TypeError) as exc:
            self.log.error("postgres target incorrectly configured: %s", exc)
            return False

        # Attempt to format each JSON data value with the transformation data.
        for key, value in item.data.items():
            if key not in META_KEYS:
                try:
                    item.data[key] = value.format(**item.data)
                except Exception:
                    pass

        try:
            with self.get_connection() as conn:
                try:
                    cursor = conn.cursor()
                    unknown_keys = self.add_row(cursor, schema, table_name, item.data,
                                                item.message, fallback_col)
                    conn.commit()
                except Exception as exc:
                    self.log.error("Could not add postgres row: %s", exc)
                    return False
                finally:
                    cursor.close()
        except psycopg2.Error as exc:
            self.log.error("Could not connect to postgres data '%s' at '%s:%s': %s",
                           self.database, self.host, self.port, exc)
            return False

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

        return True

    __call__ = plugin
