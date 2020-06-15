# executionControlPy
Schedules the execution of tasks and prevents more than one process from taking the same task to execute. Uses a mysql table for control.

Requirements: package http://github.com/steel-a/dbpy

To run the automatic tests, it's necessary:
- a mysql database;
- configure the test connection string in package dbpy, file tests/connStringCfg.py.

Note: I use the following directory structure:

apps/
..packages/
...dbpy
...executionControlPy

If you use the same structure, you ca use /requirements/install-requirements.sh to install the requirements