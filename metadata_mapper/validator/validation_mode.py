from abc import ABC
from enum import Enum
from typing import Any

class ValidationMode(Enum):

    class Mode(ABC):
        def compare(self, one: Any, two: Any) -> bool:
            if type(one) == type(two):
                type_str = type(one).__name__
                fn = getattr(self, f"{type_str}_compare")
                return fn(one, two)
            else:
                return self.any_compare(one, two)
        
        def str_compare(self, one: str, two: str) -> bool:
            return one == two

        def list_compare(self, one: list, two: list) -> bool:
            return one == two

        def dict_compare(self, one: dict, two: dict) -> bool:
            return one == two

        def NoneType_compare(self, one: None, two: None) -> bool:
            return True
        
        def any_compare(self, one: Any, two: Any) -> bool:
            return False
        

    class StrictMode(Mode):
        """
        Strict validation mode generally requires exact matches to pass.
        """
        pass


    class LaxMode(Mode):
        """
        Lax validation mode generally permits case- and order-insensitive comparisons.
        """
        @staticmethod
        def str_compare(one, two):
            return one.lower() == two.lower()

        @staticmethod
        def list_compare(one: list, two: list) -> bool:
            return sorted(one) == sorted(two)

        def any_compare(self, one: Any, two: Any) -> bool:
            return str(one) == str(two)
        

    STRICT = StrictMode()
    LAX = LaxMode()