# -*- coding: utf-8 -*-
"""Helper functions for handling location updates via MQTT from OwnTracks app.

If you want to save your locastion updates in a PostgreSQL database using the
'postgres' servcie plugin, create a database table with the following
definition:

.. code-block:: postgres

    CREATE TABLE owntracks (
        id INT GENERATED ALWAYS AS IDENTITY,
        "tst" TIMESTAMP,
        "username" VARCHAR,
        "device" VARCHAR,
        "acc" INTEGER,
        "alt" INTEGER,
        "batt" INTEGER,
        "conn" CHAR(1),
        "cog" INTEGER,
        "lat" FLOAT,
        "lon" FLOAT,
        "t" CHAR(1),
        "tid" CHAR(2),
        "vac" INTEGER,
        "vel" INTEGER,
        "extradata" VARCHAR
    );

Then, assuming the database with this table is named ``mqttwarn``, add the
following configuration section for the ``postgres`` service and a topic
handler section targetting this service to your ``mqttwarn.ini`` file::

    [config:postgres]
    host = <your postgres server host name here>
    user = <your database username here>
    password = <your database password here>
    database = mqttwarn
    targets = {
            'owntracks': ['owntracks', 'extradata'],
        }

    [owntracks-location]
    topic = owntracks/+/+
    targets = log:info, postgres:owntracks
    filter = owntracks_helpers:owntracks_filter()
    datamap = owntracks_helpers:owntracks_dataconvert()
    format = {username} {device} {tst} at location {lat},{lon}

The function for the ``filter`` option makes sure that only messages of type
``location`` are passed to the targets. The function for the ``datamap`` option
extracts the user and device name from the message topic and adds it to the
payload data. It also converts the integer Unix-timestamp in the payload data
to a ``datatime.datetime``instance, which can be inserted into the ``tst``
table column with the type TIMESTAMP. The format string from the ``format``
option will be used to format the payload data into a human-friendly message to
display via the ``log:info`` target.

"""

import datetime

import json


def owntracks_filter(topic, payload):
    return not payload.startswith(b'{"_type":"location"')


def owntracks_datamap(topic, data):
    try:
        # owntracks/username/device
        _, username, device = topic.split('/', 2)
    except ValueError:
        username = 'unknown'
        device = 'unknown'

    data.update(dict(username=username, device=device))

    tst = data.get('tst')

    if tst:
        data['tst'] = datetime.datetime.fromtimestamp(tst)
    else:
        data['tst'] = data['_dt']

    del data['_type']


def owntracks_batt_filter(topic, payload):
    """Filter out any OwnTracks notifications which do not contain the 'batt' parameter.

    When the filter function returns True, the message is filtered out, i.e. not
    processed further.

    """
    batt = json.loads(payload).get('batt')

    if batt is not None:
        try:
            # Suppress message if value stored under key 'batt' is greater than 20
            return int(batt) > 20
        except ValueError:
            return True

    # Suppress message because no 'batt' key in data
    return True
