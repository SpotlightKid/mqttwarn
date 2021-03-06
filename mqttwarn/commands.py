# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import os
import sys
import json
import signal
import logging

from docopt import docopt

from . import __version__
from .configuration import Config
from .core import bootstrap, connect, cleanup, run_plugin
from .util import get_resource_content


log = logging.getLogger(__name__)

APP_NAME = 'mqttwarn'


def run():
    """
    Usage:
      {program} [make-config]
      {program} [make-samplefuncs]
      {program} [--plugin=] [--data=]
      {program} --version
      {program} (-h | --help)

    Configuration file options:
      make-config               Will dump configuration file content to STDOUT,
                                suitable for redirecting into a configuration file.

    Miscellaneous options:
      --version                 Show version information
      -h --help                 Show this screen

    """
    # Use generic commandline options schema and amend with current program name
    commandline_schema = run.__doc__.format(program=APP_NAME)

    # Read commandline options
    options = docopt(commandline_schema, version=APP_NAME + ' ' + __version__)

    if options['make-config']:
        payload = get_resource_content('mqttwarn.examples', 'basic/mqttwarn.ini')
        print(payload)

    elif options['make-samplefuncs']:
        payload = get_resource_content('mqttwarn.examples', 'basic/samplefuncs.py')
        print(payload)

    elif options['--plugin'] and options['--data']:
        # Decode arguments
        plugin = options['--plugin']
        data = json.loads(options['--data'])

        # Launch service plugin in standalone mode
        launch_plugin_standalone(plugin, data)

    # Run mqttwarn in service mode when no command line arguments are given
    else:
        run_mqttwarn()


def launch_plugin_standalone(plugin, data):
    # Load configuration file
    scriptname = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    config = load_configuration(name=scriptname)

    # Setup logging
    setup_logging(config)
    log.info("Running service plugin '%s' with data '%s'.", plugin, data)

    # Launch service plugin
    run_plugin(config=config, name=plugin, data=data)


def run_mqttwarn():
    # Script name (without extension) used as last resort fallback for config/logfile names
    scriptname = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # Load configuration file
    config = load_configuration(name=scriptname)

    # Setup logging
    setup_logging(config)
    log.info("Starting %s.", scriptname)
    log.info("Log level is %s.", logging.getLevelName(log.getEffectiveLevel()))

    # Handle signals
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    # Bootstrap mqttwarn.core
    bootstrap(config=config, scriptname=scriptname)

    # Connect to broker and start listening
    connect()


def load_configuration(configfile=None, name=None):
    if configfile is None:
        configfile = os.getenv(name.upper() + 'INI', name + '.ini')

    if not os.path.exists(configfile):
        raise ValueError('Configuration file "{}" does not exist'.format(configfile))

    defaults = {
        'client_id': name,
        'lwt': 'clients/{}'.format(name),
        'logfile': os.getenv(name.upper() + 'LOG', name + '.log'),
    }

    return Config(configfile, defaults=defaults)


def setup_logging(config):
    level = getattr(logging, config.loglevel, 'INFO')

    # Send log messages to sys.stderr by configuring "logfile = stream://sys.stderr"
    if config.logfile.startswith('stream://sys.'):
        stream = getattr(sys, config.logfile.replace('stream://sys.', ''))
        logging.basicConfig(stream=stream, level=level, format=config.logformat)

    # Send log messages to file by configuring "logfile = 'mqttwarn.log'"
    else:
        logging.basicConfig(filename=config.logfile, level=level, format=config.logformat)
