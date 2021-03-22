"""Kukur Windows Service configuration.

This is the entry point for the cx_freeze init script.
It reads the properties in here to start the actual service."""

NAME = 'kukur-%s'
DISPLAY_NAME = 'Kukur - %s'
MODULE_NAME = 'windows.win_service'
CLASS_NAME = 'KukurService'
