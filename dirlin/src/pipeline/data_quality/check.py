import inspect
from typing import Callable, Any, TypeVar


class Check:
    def __init__(
            self,
            check_function: Callable[..., ...],
            *,
            fix_function: Callable[..., Any] | None = None,
            option_keywords: list[str] | str = "option"
    ):
        ############################
        # HANDLING THE FUNCTION
        ############################
        # I think the fastest way to complete this change would be to create a mapping based on the info we need
        # Check needs to know exactly what parameters it needs

        # ==== function level info ====
        self._check_function = check_function
        """function wrapper. This function is used to perform boolean checks."""

        self.fix_function = fix_function
        """function wrapper. Going to start off by separating the fix from checking, but will end up combining logic"""

        self.name: str = self._check_function.__name__
        """name of the function"""

        # ==== parameter level info ====
        # The mapping needs to have a set or dict that I can quickly reference multiple times
        # - parameter dictionaries could be built off this by using the keys
        self.__annotations__: dict[str, Any] = inspect.get_annotations(self._check_function)
        """all parameters of the function. `param`: `Type(arg)`, key-value pair"""

        # used for quick parameter name checks and class signatures
        self._param_signatures = list(inspect.signature(self._check_function).parameters)
        """similar to __annotations__ but discloses only name and use of self or cls as an argument (param instance)"""

        # option signatures ('option', user input)
        self._arg_option_signature: list = self._handle_string_to_list_conversion(option_keywords)
        """defaults to `option`. Marks params with option as a prefix. Denotes params not tied to specific fields."""

        # ==== API level info ====
        # this section is for what the check needs to request or respond with to other requests
        # - For example, we need to know what parameters we are expecting from outside sources
        self.expected_arguments: dict[str, Any] = {
            param: ptype for param, ptype in self.__annotations__.items()
            if param not in self._arg_option_signature and param != "return"
        }
        """returns the expected parameters of the check function

        For example, if a function has the parameters `def function(arg1, arg2, arg3)`,
        the function will return a dictionary of `{arg1: type(arg1), arg2: type(arg2), arg3: type(arg3)}`.
        """

        # i) we need to add some checks now since `inspect.get_annotations` does not return
        # a proper dictionary with all parameters unless it's been typed. I think we force
        # the user to type their functions to make it clean
        _function_signature_check = [
            sig for sig in inspect.signature(self._check_function).parameters.keys()
            if sig not in self._arg_option_signature and sig not in ('self', 'cls')
        ]
        if len(self.expected_arguments) != len(_function_signature_check):
            raise IndexError(
                f"One or more parameters are not type in the function `{self._check_function.__name__}`."
                f" Please add the proper typing to your function parameters."
            )

    def run(self, **kwargs) -> Any:
        """API that wraps around the `_check_function` so that we can  deal with things like
        signatures within the Check class since it abstracts it out of the Validation class.

        Uses `_handle_class_signatures()` to handle class signatures`. Returns whatever the
        check_function would have returned.
        """
        _has_class_signature = self._handle_class_signatures()

        if not _has_class_signature:
            return self._check_function(**kwargs)
        return self._check_function(_has_class_signature, **kwargs)

    def _handle_class_signatures(self) -> str | None:
        """identifies whether the check function has class arguments (self, cls) as its first argument

        Example of a function without a signature:
            - def function_without_signature(param1: type1, param2: type2)

        Example of a function with a signature:
            - def function_with_signature(self, param1: type1, param2: type2)

        Returns the class argument itself (self, cls) if the function has a class signature, or None
        if there is no class signature in the function.
        """
        _example_param_instances = ('self', 'cls')
        """Represents the arguments used for class functions or instantiated object functions"""

        if self._param_signatures:
            first_arg = self._param_signatures[0]
            first_arg_is_class_signature = first_arg in _example_param_instances
        else:
            raise KeyError(f"{self.name} has no parameters!")
        if not first_arg_is_class_signature:
            return None
        return first_arg

    def _handle_string_to_list_conversion(self, multi_type_param: str | list[str]) -> list[str] | list:
        """identifies the argument type, and converts it into a list of string

        Specifically used for parameters in the class that requires str types to be converted to lists
        to make it easier to loop through without considering scalar values.

        Returns empty list `[]` if the argument is not given.

        """
        if isinstance(multi_type_param, str):
            return [multi_type_param]
        elif isinstance(multi_type_param, list):
            return multi_type_param
        elif multi_type_param is None:
            return []
        else:
            raise TypeError(f"{self.name} can only handle string or list")


CheckType = TypeVar('CheckType', bound=Check)
"""Object type Check"""
