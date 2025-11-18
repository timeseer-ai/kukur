"""Run Kukur as a Windows service.

This is called by the init script of cx_freeze.
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
import os.path
import subprocess
import sys
import threading

import cx_Logging


class KukurService:
    """Kukur Windows service."""

    def __init__(self):
        self.stopEvent = threading.Event()
        self.stopRequestedEvent = threading.Event()

    def initialize(self, _config_file_name):
        """Run startup tasks."""
        self.directory = os.path.dirname(sys.executable)
        cx_Logging.StartLogging(
            os.path.join(self.directory, "service.log"), cx_Logging.DEBUG
        )

    def run(self):
        """Start Kukur."""
        cx_Logging.Debug("Starting kukur.exe")
        self.process = subprocess.Popen(["kukur.exe"], cwd=self.directory)
        self.process.wait()
        self.stopRequestedEvent.wait()
        self.stopEvent.set()

    def stop(self):
        """Stop Kukur."""
        if self.process:
            cx_Logging.Debug("Stopping kukur.exe")
            self.process.kill()
        self.stopRequestedEvent.set()
        self.stopEvent.wait()
