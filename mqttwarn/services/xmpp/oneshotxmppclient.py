#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A basic XMPP bot that will log in, send a message, and then log out.

Source: https://slixmpp.readthedocs.io/getting_started/sendlogout.html

Slixmpp: The Slick XMPP Library
Copyright (C) 2010  Nathanael C. Fritz
This file is part of Slixmpp.

See the file LICENSE for copying permission.

Adapted for mqttwarn by Christopher Arndt.

"""

import asyncio
import logging
import sys

import six
import slixmpp


class OneShotXMPPClient(slixmpp.ClientXMPP):
    """A basic XMPP bot that will log in, send a message, and then log out."""

    def __init__(self, jid, password, recipients, message, subject=None):
        super().__init__(jid, password)

        # The message we wish to send, and the JID(s) that should receive it.
        if isinstance(recipients, six.string_types):
            recipients = [recipients]

        self.recipients = recipients
        self.message = message
        self.subject = subject or message

        # The session_start event will be triggered when the bot establishes
        # its connection with the server and the XML streams are ready for use.
        # We want to listen for this event so that we we can initialize our
        # roster.
        self.add_event_handler("session_start", self.start)

    def start(self, event):
        """Process the session_start event.

        Typical actions for the session_start event are
        requesting the roster and broadcasting an initial
        presence stanza.

        Arguments:
            event -- An empty dictionary. The session_start
                     event does not provide any additional
                     data.

        """
        self.send_presence()
        self.get_roster()

        for recipient in self.recipients:
            self.send_message(mto=recipient, mbody=self.message,
                              msubject=self.subject, mtype='chat')

        self.disconnect(wait=True)


def send_message(jid, password, recipients, message, subject=None):
    """Create OneShotXMPPClient instance, connect to server and send message to given recipients."""
    # If this function runs in a thread that is not the main thread, we need to
    # create an asyncio loop manually and set it to be the default one for the
    # thread's context, so OneShotXMPPClient's base class
    # slixmpp.xmlstream.XMLStream uses it. There doesn't seem to be an easy way
    # to pass an event loop to it, because in several places the code creates
    # asyncio.Futures using the default event loop.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Setup the OneShotXMPPClient and register plugins. Note that while plugins may
    # have interdependencies, the order in which you register them does
    # not matter.
    xmpp = OneShotXMPPClient(jid, password, recipients, message, subject)
    xmpp.register_plugin('xep_0030') # Service Discovery
    xmpp.register_plugin('xep_0199') # XMPP Ping

    # Connect to the XMPP server and start processing XMPP stanzas.
    xmpp.connect()
    #xmpp.loop.run_until_complete(xmpp.disconnected)
    xmpp.process(forever=False)


def main(args=None):
    from argparse import ArgumentParser, REMAINDER

    # Setup the command line arguments.
    parser = ArgumentParser(description=__doc__.splitlines()[0])

    # Output verbosity options.
    parser.add_argument("-q", "--quiet", help="set logging to ERROR",
                        action="store_const", dest="loglevel",
                        const=logging.ERROR, default=logging.INFO)
    parser.add_argument("-d", "--debug", help="set logging to DEBUG",
                        action="store_const", dest="loglevel",
                        const=logging.DEBUG, default=logging.INFO)

    # JID and password options.
    parser.add_argument("-j", "--jid",
                        help="JID to use as sender")
    parser.add_argument("-p", "--password", dest="password",
                        help="password to use for sender")
    parser.add_argument("-t", "--to", action="append",
                        help="JID to send the message to (may be given more than once)")
    parser.add_argument("-s", "--subject",
                        help="The message subject (defaults to same as message)")
    parser.add_argument('message', nargs=REMAINDER, help="message to send")

    args = parser.parse_args(args if args is not None else sys.argv[1:])

    # Setup logging.
    logging.basicConfig(level=args.loglevel, format='%(levelname)-8s %(message)s')

    if args.message:
        message = (" ".join(args.message)).strip()
    else:
        try:
            message = sys.stdin.read().strip()
        except KeyboardInterrupt:
            return 1

    print("Subject: %r" % args.subject)
    print("Message: %r" % message)

    if not message:
        return "No message given on command line or via standard input. Nothing do do."

    try:
        if not args.jid:
            args.jid = input("JID: ")

        if not args.password:
            from getpass import getpass
            args.password = getpass("Password: ")

        if not args.to:
            args.to.append(input("Send To: "))
    except (EOFError, KeyboardInterrupt):
        print('')
        return 1

    if not args.jid:
        return "No sender JID specified. Aborting."

    if not args.password:
        return "No sender password specified. Aborting."

    if not args.to:
        return "No recipient(s) JID(s) specified. Aborting."

    send_message(args.jid, args.password, args.to, message, args.subject)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]) or 0)
