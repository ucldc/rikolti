from typing import Callable


def returns_callable(func: Callable) -> Callable:
    """
    A decorator that returns a lambda that calls the wrapped function when invoked
    """
    def inner(*args, **kwargs):
        return lambda: func(*args, **kwargs)

    return inner
