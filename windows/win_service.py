"""Run Kukur as a Windows service.

This is called by the init script of cx_freeze."""

import os.path
import subprocess
import sys
import threading

import cx_Logging
import psutil


class KukurService():
    '''Kukur Windows service'''

    def __init__(self):
        self.stopEvent = threading.Event()
        self.stopRequestedEvent = threading.Event()

    def initialize(self, config_file_name):
        self.directory = os.path.dirname(sys.executable)
        cx_Logging.StartLogging(os.path.join(self.directory, 'service.log'), cx_Logging.DEBUG)
        return None

    def run(self):
        cx_Logging.Debug('Starting kukur.exe')
        self.process = subprocess.Popen(['kukur.exe'], cwd=self.directory)
        self.process.wait()
        self.stopRequestedEvent.wait()
        self.stopEvent.set()

    def stop(self):
        if self.process:
            cx_Logging.Debug('Stopping kukur.exe')
            for child in psutil.Process(self.process.pid).children(recursive=True):
                child.kill()
            self.process.kill()
        self.stopRequestedEvent.set()
        self.stopEvent.wait()
