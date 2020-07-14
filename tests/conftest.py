def pytest_addoption(parser):
    parser.addoption("--path", action="store", help="USAGE: pytest tests/basic.py --path [/path/to/sonar]")