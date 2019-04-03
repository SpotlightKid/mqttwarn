#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Jan-Piet Mens <jpmens()gmail.com>"
__copyright__ = "Copyright 2014 Jan-Piet Mens"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

import MySQLdb


# https://mail.python.org/pipermail/tutor/2010-December/080701.html
def add_row(cursor, tablename, rowdict):
    # XXX tablename not sanitized
    # XXX test for allowed keys is case-sensitive
    unknown_keys = None

    # filter out keys that are not column names
    cursor.execute("describe %s" % tablename)
    allowed_keys = set(row[0] for row in cursor.fetchall())
    keys = allowed_keys.intersection(rowdict)

    if len(rowdict) > len(keys):
        unknown_keys = set(rowdict) - allowed_keys

    columns = ", ".join(keys)
    values_template = ", ".join(["%s"] * len(keys))

    sql = "insert into %s (%s) values (%s)" % (
        tablename, columns, values_template)
    values = tuple(rowdict[key] for key in keys)
    cursor.execute(sql, values)

    return unknown_keys


def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s, target=%s", __file__, item.service, item.target)

    conf = item.config.get
    host = conf('host', 'localhost')
    port = conf('port', 3306)
    user = conf('user')
    passwd = conf('pass')
    dbname = conf('dbname')

    try:
        table_name = item.addrs[0].format(**item.data).encode('utf-8')
        fallback_col = item.addrs[1].format(**item.data).encode('utf-8')
    except Exception as exc:
        srv.log.warn("'mysql' service incorrectly configured: %s", exc)
        return False

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=dbname)
        cursor = conn.cursor()
    except Exception as exc:
        srv.log.warn("Cannot connect to MySQL server: %s", exc)
        return False

    text = item.message

    # Create new dict for column data. First add fallback column
    # with full payload. Then attempt to use formatted JSON values
    col_data = {
        fallback_col: text
    }

    if fallback_col == 'NOP':
        del(col_data['fallback_col'])

    if item.data is not None:
        for key in item.data.keys():
            try:
                col_data[key] = item.data[key].format(**item.data).encode('utf-8')
            except Exception:
                col_data[key] = item.data[key]

    try:
        unknown_keys = add_row(cursor, table_name, col_data)
        if unknown_keys is not None:
            srv.log.debug("Skipping unused keys %s" % ",".join(unknown_keys))

        conn.commit()
    except Exception as exc:
        srv.log.warn("Cannot add mysql row: %s", exc)
        return False
    finally:
        cursor.close()
        conn.close()

    return True
