from typing import Union


def flatten(object: Union[list, tuple]) -> list:
    result = []

    def loop(obj: Union[list, tuple]) -> None:
        for item in obj:
            if isinstance(item, (list, tuple)):
                loop(item)
            else:
                result.append(item)

    loop(object)
    return result
