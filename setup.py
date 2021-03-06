# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers
"""mqttwarn subscribes to any number of MQTT topics and publishes received payloads to one or more
notification services after optionally applying sophisticated transformations.

"""

import sys
from setuptools import setup, find_packages


requires = [
    'six>=1.11.0',
    'paho-mqtt>=1.3',
    'jinja2>=2.8',
    'docopt>=0.6.2',
    'requests>=2.18.4',
    'stopit>=1.1.2',
]

extras = {
    'amqp': [
        'puka>=0.0.7',
    ],
    'apns': [
        'apns>=2.0.1',
    ],
    'asterisk': [
        'pyst2>=0.5.0',
    ],
    'celery': [
        'celery',
    ],
    'dbus': [
        'pydbus>=0.6.0',
    ],
    'dnsupdate': [
        'dnspython>=1.15.0',
    ],
    'fbchat': [
        'fbchat>=1.3.6',
    ],
    'gss': [
        'gdata>=2.0.18',
    ],
    'gss2': [
        'gspread>=2.1.1',
        'oauth2client>=4.1.2',
    ],
    'iothub': [
        'iothub-client>=1.1.2.0',
    ],
    'mysql': [
        'mysql',
    ],
    'nma': [
        'PyNMA>=1.0',
    ],
    'nsca': [
        'pynsca>=1.6',
    ],
    'osxnotify': [
        'pync>=1.6.1',
    ],
    'pastebinpub': [
        'Pastebin>=1.1.2',
    ],
    'postgres': [
        'psycopg2>=2.7.4',
    ],
    'prowl': [
        'prowlpy>=0.52',
    ],
    'pushbullet': [
        'PushbulletPythonLibrary>=2.3',
    ],
    'redispub': [
        'redis>=2.10.6',
    ],
    'rrdtool': [
        'rrdtool>=0.1.12',
    ],
    'serial': [
        'pyserial>=3.4',
    ],
    'slack': [
        'slacker>=0.9.65',
    ],
    'ssh': [
        'paramiko>=2.4.1',
    ],
    'tootpaste': [
        'Mastodon.py>=1.2.2',
    ],
    'twilio': [
        'twilio>=6.11.0',
    ],
    'twitter': [
        'python-twitter>=3.4.1',
    ],
    'websocket': [
        'websocket-client>=0.47.0',
    ],
    'xively': [
        'xively-python',
    ],
    'xmpp': [
        'xmpppy>=0.5.0rc1',
    ],
    'scripts': [
        'appdirs>=1.4.0',
        'keyring>=19.0.0',
        'ConfigArgParse>=0.14.0'
    ]
}

setup(
    name='mqttwarn',
    version='0.10.1',
    description='mqttwarn - subscribe to MQTT topics and notify pluggable services',
    long_description=__doc__,
    license="EPL 2.0",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Environment :: Plugins",
        "Intended Audience :: Developers",
        "Intended Audience :: Education",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Manufacturing",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Telecommunications Industry",
        "License :: OSI Approved :: Eclipse Public License 2.0 (EPL-2.0)",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Operating System :: MacOS",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Communications",
        "Topic :: Education",
        "Topic :: Internet",
        "Topic :: Internet :: MQTT",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: XMPP",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator",
        "Topic :: Software Development :: Embedded Systems",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Pre-processors",
        "Topic :: Software Development :: Testing",
        "Topic :: System :: Archiving",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Monitoring",
        "Topic :: System :: Networking :: Monitoring",
        "Topic :: System :: Systems Administration",
        "Topic :: Text Processing",
        "Topic :: Utilities",
    ],
    author='Jan-Piet Mens, Ben Jones',
    author_email='jpmens()gmail.com, ben.jones12()gmail.com',
    maintainer='Christopher Arndt',
    maintainer_email='info@chrisarndt.de',
    url='https://github.com/SpotlightKid/mqttwarn',
    keywords='mqtt notification plugins data acquisition push transformation engine mosquitto',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'mqttwarn': [
            '*.ini',
        ],
    },
    zip_safe=False,
    test_suite='mqttwarn.test',
    install_requires=requires,
    extras_require=extras,
    dependency_links=[
        'https://github.com/jacobb/prowlpy/archive/master.tar.gz#egg=prowlpy'
    ],
    entry_points={
        'console_scripts': [
            'mqttwarn = mqttwarn.commands:run',
            'mqtt-pub = mqttwarn.scripts.mqtt_pub:main',
            'mqtt-sub = mqttwarn.scripts.mqtt_sub:main',
        ],
    },
)
