import sys

import py
import pytest
from xprocess import ProcessStarter

def start_sonar_server(path,xprocess):
    class Starter(ProcessStarter):
        pattern = "listening on http://localhost:9191"
        args = ['node', path + '/sonar-server/launch.js', '--dev', '-s' '/tmp']   
    xprocess.ensure("sonarServer", Starter)
    
def stop_server(xprocess):
    xprocess.getinfo("sonarServer").terminate()  

def pytest_addoption(parser):
    parser.addoption("--path", action="store", help="USAGE: pytest tests/basic.py --path [/path/to/sonar]")