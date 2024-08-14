def pytest_addoption(parser):
    parser.addoption("--mappers", action="store", default=None)
    parser.addoption("--mapper", action="store", default=None)