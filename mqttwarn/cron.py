# -*- coding: utf-8 -*-
# (c) 2014-2019 The mqttwarn developers

import logging
import threading


log = logging.getLogger(__name__)


# This class, shamelessly stolen from https://gist.github.com/cypreess/5481681
# The `srv' bits are added for mqttwarn
class PeriodicThread(object):
    """Python periodic Thread using Timer with instant cancellation."""

    def __init__(self, callback=None, period=1, name=None, srv=None, now=False, *args, **kwargs):
        self.name = name
        self.srv = srv
        self.now = now
        self.args = args
        self.kwargs = kwargs
        self.callback = callback
        self.period = period
        self.stop = False
        self.current_timer = None
        self.schedule_lock = threading.Lock()

    def start(self):
        """Mimic Thread standard start method."""
        if self.now:
            # Schedule periodic task to run right now
            self._run()
        else:
            # Schedule periodic task with designated interval
            self.schedule_timer()

    def run(self):
        """By default run callback.

        Override it if you want to use inheritance.

        """
        if self.callback is not None:
            self.callback(self.srv, *self.args, **self.kwargs)

    def _run(self):
        """Run desired callback and then reschedule Timer (if thread is not stopped)."""
        try:
            self.run()
        except Exception as exc:
            log.exception("Exception in running periodic thread: %s", exc)
        finally:
            with self.schedule_lock:
                if not self.stop:
                    self.schedule_timer()

    def schedule_timer(self):
        """Schedule next Timer run."""
        self.current_timer = threading.Timer(self.period, self._run)
        if self.name:
            self.current_timer.name = self.name
        self.current_timer.start()

    def cancel(self):
        """Mimic Timer standard cancel method."""
        with self.schedule_lock:
            self.stop = True
            if self.current_timer is not None:
                self.current_timer.cancel()

    def join(self):
        """Mimic Thread standard join method."""
        self.current_timer.join()
