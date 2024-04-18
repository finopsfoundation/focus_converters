from abc import ABC, abstractmethod
from functools import wraps
from typing import Dict, Optional, Type

import polars as pl
from pydantic import BaseModel


class BaseFOCUSConverterPlugin(ABC):
    @classmethod
    @abstractmethod
    def get_arguments_model(cls) -> Optional[Type[BaseModel]]:
        """
        Returns the Pydantic model for the arguments of the conversion function
        This can be used to validate the arguments passed to the conversion function
        If None is returned, no validation will be performed
        """
        ...

    @classmethod
    @abstractmethod
    def conversion_plan_hook(cls, plan, column_validator) -> pl.Expr:
        """
        Hook for preparing the conversion plan
        This step should consume the current conversion plan and return the modified plan
        """
        ...


plugins: Dict[str, BaseFOCUSConverterPlugin] = {}


def focus_conversion(plan_name: str):
    def class_decorator(cls: BaseFOCUSConverterPlugin):
        # Iterate over all attributes of the class looking for methods
        for name, method in cls.__dict__.items():
            if callable(method):
                if name == "get_arguments_model":
                    setattr(
                        cls,
                        name,
                        with_provider(return_type=Optional[BaseModel])(method),
                    )
                elif name == "conversion_plan_hook":
                    setattr(
                        cls,
                        name,
                        with_provider(return_type=pl.Expr)(method),
                    )

        if plan_name in plugins:
            raise ValueError(f"Duplicate plan name '{plan_name}'")
        plugins[plan_name] = cls

        return cls

    return class_decorator


def with_provider(return_type: type = None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if return_type is None and result is not None:
                raise TypeError(
                    f"Expected '{func.__name__}' to return None, got {type(result).__name__} instead"
                )
            elif return_type is not None and not isinstance(result, return_type):
                raise TypeError(
                    f"Expected '{func.__name__}' to return {return_type}, got {type(result).__name__} instead"
                )
            return result

        return wrapper

    return decorator
