# -*- coding: utf-8 -*-

# Credits to Jan-Piet Mens for the mysql.py code which served as basis for this module

__author__ = "João Paulo Barraca <jpbarraca()gmail.com>"
__copyright__ = "Copyright 2014 João Paulo Barraca"
__license__ = "Eclipse Public License - v 1.0 (http://www.eclipse.org/legal/epl-v10.html)"

import re
import time

import MySQLdb
import six


def add_row(srv, cursor, index_table_name, table_name, rowdict, ignorekeys):
    keys = []
    clean_key = re.compile(r'[^\d\w_-]+')

    for k, v in rowdict.items():
        if k in ignorekeys:
            continue

        key = clean_key.sub('', k)
        keys.append({'ori': k, 'clean': key})

    try:
        cursor.execute("describe %s" % table_name)
    except Exception:
        colspec = ['`id` INT AUTO_INCREMENT']

        for k in keys:
            if isinstance(rowdict[k['ori']], six.integer_types):
                colspec.append('`%s` LONG' % k['clean'])
            elif isinstance(rowdict[k['ori']], float):
                colspec.append('`%s` FLOAT' % k['clean'])
            else:
                colspec.append('`%s` TEXT' % k['clean'])

        query = 'CREATE TABLE `%s` (' % table_name
        query += ','.join(colspec)
        query += ', PRIMARY KEY ID(`id`)) CHARSET=utf8'

        try:
            cursor.execute(query)
        except Exception as exc:
            srv.log.warn("Mysql target incorrectly configured. Could not create table %s: %s",
                         table_name, exc)
            return False

    try:
        columns = ''
        values_template = ''
        sql = ''
        values = []

        for i in range(len(keys)):
            if i > 0:
                columns += ","
                values_template += ","

            columns += " " + keys[i]['clean']
            values_template += " %s"
            values.append(MySQLdb.escape_string(str(rowdict[keys[i]['ori']])))

        sql = "insert into %s (%s) values (%s)" % (table_name, columns, values_template)
        cursor.execute(sql, tuple(values))
    except Exception as exc:
        srv.log.warn("Could not insert value into table %s. Query: %s, values: %s, Error: %s",
                     table_name, sql, str(values), exc)
        return False

    try:
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        query = ('INSERT INTO %s SET topic="%s", ts="%s" ON DUPLICATE KEY UPDATE ts="%s"' %
                 index_table_name, table_name, now, now)
        cursor.execute(query)
    except Exception as exc:
        srv.log.warn("Could not insert value into index table '%s': %s", index_table_name, exc)

    return True


def plugin(srv, item):
    srv.log.debug("*** MODULE=%s: service=%s target=%s", __file__, item.service, item.target)

    conf = item.config.get
    host = conf('host', 'localhost')
    port = conf('port', 3306)
    user = conf('user')
    passwd = conf('pass')
    dbname = conf('dbname')
    index_table_name = conf('index')
    #ignore_keys = conf('ignore_')

    # Sanitize table_name
    table_name = item.data['topic'].replace('/', '_')
    table_name = re.compile(r'[^\d\w_]+').sub('', table_name)

    try:
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=dbname)
        cursor = conn.cursor()
    except Exception as exc:
        srv.log.warn("Cannot connect to mysql: %s", exc)
        return False

    # Create new dict for column data. First add fallback column
    # with full payload. Then attempt to use formatted JSON values
    col_data = {}

    if item.data is not None:
        for key in item.data.keys():
            try:
                if isinstance(col_data[key], six.string_types):
                    col_data[key] = item.data[key].format(**item.data).encode('utf-8')
            except Exception:
                col_data[key] = item.data[key]

    try:
        result = add_row(srv, cursor, index_table_name, table_name, col_data, item.addrs)
        if not result:
            srv.log.debug("Failed building values to add to database")
        else:
            conn.commit()
    except Exception as exc:
        srv.log.exception("Cannot add mysql row: %s", exc)
        return False
    finally:
        cursor.close()
        conn.close()

    return True
