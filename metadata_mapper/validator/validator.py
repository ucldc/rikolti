import itertools
import re
import urllib.parse
from typing import Any, Callable, Union

from .validation_log import ValidationLog, ValidationLogLevel
from .validation_mode import ValidationMode


class Validator:

    def __init__(self, **options):
        self.log = ValidationLog(options.get("log_level", ValidationLogLevel.WARNING))
        self.default_validation_mode = options.get("validation_mode",
                                                   ValidationMode.STRICT)
        self.verbose = options.get("verbose", False)
        self.setup()

    def setup(self) -> None:
        """Optional post-init callback, useful to modify validatable fields, etc."""
        pass

    def validate(self, key: str, rikolti_data: dict,
                 comparison_metadata: dict, validation_mode = None) -> ValidationLog:
        """
        Performs validation of mapped_metadata against comparison_metadata.

        For each entry in validatable_fields, the defined validations
        are provided a key/value pair from both datasets and invoked.
        If validation fails, an entry is added to self.log,
        which is ultimately returned.

        Parameters:
            rikolti_data: dict[str, Any]
                Rikolti record data
            comparison_data: dict[str, Any]
                Comparison (usually Solr) data

        Returns: ValidationLog
        """
        self.key = key
        self.rikolti_data = rikolti_data
        self.comparison_data = comparison_metadata
        self.validation_mode = validation_mode or self.default_validation_mode

        if not self.rikolti_data or not self.comparison_data:
            missing_data_desc = "mapped Rikolti" if not self.rikolti_data else "Solr"
            print(f"ERROR: No {missing_data_desc} data found for key {self.key}")
            # error = {
            #     "key": self.key,
            #     "level": "ERROR",
            #     "description": f"No {missing_data_desc} data found for key"
            # }
            # self.log.add(**error)
        else:
            self.successfully_validated_fields = []

            self.before_validation()

            for validation_def in self.validatable_fields:
                self._perform_validations(self._normalize_validation_definition(validation_def))

            self.rights_validation()
            self.after_validation()

            self._maybe_create_validation_success_entry()

        return self.log

    def set_validatable_fields(self, fields: list[dict[str, Any]]) -> list[dict]:
        """
        Sets fields to be validated, replacing defaults.

        Parameters:
            fields: list[dict[str, Any]] (default: [])
                A list of dicts containing validation definitions.
                See default_validatable_fields for examples.

        Returns: list[dict]
        """
        self.validatable_fields = fields
        return self.validatable_fields
    
    def add_validatable_field(self, field: str,
                              validations: Union[Callable,
                                                 list[Callable],
                                                 dict[Callable, ValidationLogLevel]
                                                 ],
                              level: ValidationLogLevel = None,
                              validation_mode: ValidationMode = None,
                              replace: bool = True
                              ) -> bool:
        """
        Adds a single field to this validator's validatable_fields list.

        Parameters:
            field: str
                The unique field name
            validations: list[Callable, list[Callable], dict[Callable, ValidationLogLevel]]
                The validation(s) to run on this field. If a dict, you can define a
                ValidationLogLevel per validation to log on failure.
            level: ValidationLogLevel
                The log level for failure. Can be overridden on individual validations.
            validation_mode: ValidationMode
                The ValidationMode to use for these validations
            replace: bool
                If a validation definition already exists for this field, should this
                definition replace it?

        Returns: bool
        """
        existing = [f for f in self.validatable_fields if f["field"] == field]
        if len(existing) > 0:
            if replace:
                self.remove_validatable_field(field)
            else:
                return False

        validation_def = {
            "field": field,
            "type": type,
            "validations": validations,
            "level": level,
            "validation_mode": validation_mode
        }

        self.validatable_fields.append({k: v for k, v in validation_def.items() if v})
        return True

    def add_validatable_fields(self, *fields: list[dict[str, Any]]) -> dict[str, bool]:
        """
        Adds a list of validatable fields.

        Parameters:
            fields: list[dict[str, Any]]
                A list of validatable fields to add, following the pattern
                described in #add_validatable_field
        
        Returns: dict[str, bool]
        """
        return {
            field["field"]: self.add_validatable_field(field)
            for field in fields
        }

    def modify_validatable_field(self, field: str, **changes) -> bool:
        """
        Merges options into an existing validatable field definition.

        Parameters:
            field: str
                The validatable field definition to modify
            changes: dict[str, Any]
                The changes to merge into the validatable field

        Returns: bool
        """
        existing = [f for f in self.validatable_fields if f["field"] == field]
        if len(existing != 1):
            return False
        else:
            new_field_def = { **existing[0], **changes }
            self.remove_validatable_field(field)
            self.add_validatable_field(new_field_def)

    def modify_validatable_fields(self, *fields: list[str],
                                  **changes) -> dict[str, bool]:
        """
        Merges a single set of options into multiple validatable field definitions.
        """
        return { field: self.modify_validatable_field(field, **changes)
                for field in fields
                }

    def remove_validatable_field(self, field: str) -> bool:
        orig_len = len(self.validatable_fields)

        self.validatable_fields = [f for f in self.validatable_fields
                                   if f["field"] != field]
        
        return orig_len < len(self.validatable_fields)

    def generate_keys(self, collection: list[dict], type: str = None,
                      context: dict = {}) -> dict[str, dict]:
        """
        Given a list of records, generates keys and returns a dict with the
        original list contents as values.

        This can be used to override the creation of keys for a given
        mapper to ensure that key intersection works in the
        validate_mapping module.

        Parameters:
            collection: list[dict]
                Data to be added to resulting dict
            type: str (default: None)
                Optional context. Usually this will be used to tell
                this method if it's dealing with Rikolti or Solr data.
            context: dict (default: {})
                Any information needed to perform these calculations.

        Returns: dict[str, dict]
            dict of dicts, keyed to ensure successful intersection.
        """
        if type == "Rikolti":
            return {f"{context.get('collection_id')}--{r['calisphere-id']}": r
                    for r in collection}
        elif type == "Solr":
            return {r['harvest_id_s']: r for r in collection}

    def before_validation(self, **kwargs) -> None:
        """
        Optional pre-validation callback.
        """
        pass

    def rights_validation(self, **kwargs) -> None:
        if (not self.rikolti_data.get('rights') and
            not self.rikolti_data.get('rights_uri')):
            self.log.add(
                key=self.key,
                field="record rights",
                description="No rights or rights_uri found")
        return

    def after_validation(self, **kwargs) -> None:
        """
        Optional post-validation callback.
        """
        pass

    # Static validators
    #
    # Validators should return None if they pass.
    #
    # Otherwise, they should return a description string, or a
    # dict with detailed error info.
    #
    # Can also return a list of dicts if multiple
    # validations are performed, each of which should be logged
    # separately.

    @staticmethod
    def ark_type(validation_def: dict, rikolti_value: Any,
                 _: Any) -> Union[str, None]:
        """
        Validates that the value is an ARK.
        """
        if (
            not isinstance(rikolti_value, str) and
            not re.fullmatch(r'ark\:\/\d{5}\/\d{6}', rikolti_value)
        ):
            return "ID is not an 'ark:/'"

    @staticmethod
    def verify_type(expected: Union[type, Callable, list[type]]) -> Callable:
        def inner(validation_def: dict, rikolti_value: Any, _: Any) -> Union[str, None]:
            if not Validator.compare_type(expected, rikolti_value):
                return {
                    "description": "Type mismatch",
                    "actual": type(rikolti_value).__name__,
                    "expected": expected.__name__
                }

        return inner

    @staticmethod
    def type_match(validation_def: dict, rikolti_value: Any,
                   comparison_value: Any) -> Union[str, None]:
        """
        Validates that the rikolti value is of the same type
        as the comparison type.
        """
        expected_type = Validator.derive_type(comparison_value)
        result = Validator.compare_type(expected_type, rikolti_value)

        if not result:
            actual_type = type(rikolti_value)

            return {
                "description": "Type mismatch",
                "actual": actual_type.__name__,
                "expected": expected_type.__name__
            }

    @staticmethod
    def content_match(validation_def: dict, rikolti_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is the equal.
        """
        if not validation_def["validation_mode"].value.compare(
            rikolti_value, comparison_value):
            return "Content mismatch"

    @staticmethod
    def required_field(validation_def: dict, rikolti_value: Any,
                       comparison_value: Any) -> Union[str, None]:
        """
        Validates that a mapped value exists for a given field.

        In this context, anything Falsey will fail, such as empty iterables.
        If an empty iterable is acceptable, use #not_null.
        """
        if not rikolti_value:
            return "Required field is false, null, or empty"

    @staticmethod
    def not_null(validation_def: dict, rikolti_value: Any,
                 comparison_value: Any) -> Union[str, None]:
        """
        Validates that a mapped value is not None.

        In this context, empty iterables or any other Falsey value
        except None will pass.
        """
        if rikolti_value is None:
            return "Required field is null"

    # Type helpers

    @staticmethod
    def derive_type(value: Any) -> Union[type, Callable]:
        """
        Attempts to derive the type of a value for comparison.

        Does not currently handle nested lists well.

        Parameters:
            value: Any
                The value of which to derive the type

        Returns: Union[type, Callable]
        """
        if isinstance(value, list):
            inner_types = set([Validator.derive_type(item) for item in value])
            return Validator.list_of(*inner_types)
        elif isinstance(value, dict):
            inner_types = set([Validator.derive_type(item)
                               for k, item
                               in value.items()])
            return Validator.dict_of(*inner_types)
        else:
            return type(value)

    @staticmethod
    def compare_type(expected_type: Union[type, Callable, list[Union[type, Callable]]], 
                     value: Any) -> Any:
        """
        Compares a value with a its expected type.

        `expected_type` can be a type (checked with instanceof), a Callable
        (that is invoked) and expected to return a boolean result),
        or a list of types and/or Callables.

        This method will typically be invoked from a higher-order function.

        Parameters:
            expected_value: Union[type, Callable, list[Union[type, Callable]]]
                The expected type, a Callable that performs a type check, or
                list of expected types or Callables. If a list, only one must eval
                true in order to pass.
            value: Any
                The value to check.

        Returns: boolean
        """
        if value is None:
            return True
        
        if isinstance(expected_type, Callable):
            return expected_type(value)
        elif isinstance(expected_type, type):
            return isinstance(value, expected_type)
        elif isinstance(expected_type, list):
            return any(Validator.compare_type(t, value) for t in expected_type)

    @staticmethod
    def nested_value(iterable_type: type,
                     nested_types: Union[type, list[type]],
                     value_to_check: Any) -> bool:
        """
        Checks to see if an iterable and its contents are of given types.

        For example, you can check to see if an object is a list of strings
        with the args `nested_value(list, str, list_to_check)`. Or, you can
        check to see if an object is a list of strings or Nones with
        `nested_value(list, [str, None], list_to_check)`.

        This method will typically be invoked from a higher-order-function.

        Parameters:
            container_type: type
                The type that the iterable should be, i.e. list or dict.
            nested_types: Union[type, list[type]]
                The type or types that nested values are allowed to be
            value_to_check: Any
                The iterable to check

        Returns: boolean
        """
        if not isinstance(nested_types, list):
            nested_types = [nested_types]

        return (isinstance(value_to_check, iterable_type) and
                all(type(val) in nested_types for val in value_to_check))

    @staticmethod
    def list_of(*types: list[type]) -> Callable:
        """
        Returns an anonymous function that can test if an object is a list
        containing defined types.

        Parameters:
            types: Union[type, list[type]]
                The type or types that are acceptable in the list

        Returns: Callable
        """
        type_list = list(types)
        lam = lambda value: Validator.nested_value(list, type_list, value)  # noqa: E731, E501
        # Assign __name__ for readable expected value output
        lam.__name__ = f"List of {', '.join([t.__name__ for t in type_list])}"
        return lam

    @staticmethod
    def dict_of(*types: list[type]) -> Callable:
        """
        Returns an anonymous function that can test if an object is a dict
        containing values of defined types.

        Parameters:
            types: Union[type, list[type]]
                The type or types that are acceptable in the dict's values

        Returns: Callable
        """
        return lambda value: Validator.nested_value(dict, list(types), value)

    # Private

    def _perform_validations(self, validation_def: dict[str, Any]) -> None:
        """
        Runs validations for a given validation definition and adds to
        the log entry list.
        """
        if not validation_def.get("validations"):
            return

        field = validation_def["field"]
        rikolti_value = self.rikolti_data.get(field)
        comp_value = self.comparison_data.get(field)

        validations = self._normalize_validations(
            validation_def["validations"], validation_def["level"])

        for validator, level in validations.items():
            raw_results = validator(validation_def, rikolti_value, comp_value)
            normalized_results = self._normalize_validator_results(raw_results)

            if len(normalized_results) > 0:
                for result in normalized_results:
                    self._build_entries(validation_def, result, level,
                                        rikolti_value, comp_value)
            else:
                if self.verbose:
                    self._build_entries(validation_def, "Validation success",
                                        ValidationLogLevel.INFO, rikolti_value,
                                        comp_value)
                else:
                    self.successfully_validated_fields.append(validation_def.get("field"))

    def _normalize_validation_definition(self, validation_def: dict) -> dict[str, Any]:
        validation_def["validation_mode"] = validation_def.get("validation_mode",
                                                               self.validation_mode)
        validation_def["level"] = validation_def.get("level",
                                                     ValidationLogLevel.ERROR)

        return validation_def

    def _normalize_validations(self, validations: Union[
                                                        Callable,
                                                        list[Callable],
                                                        dict[Callable, str]
                                                        ],
                               default_level: ValidationLogLevel
                               ) -> dict[Callable, str]:
        """
        Ensures that provided validations are a dict of Callable: ValidationLogLevel

        If a ValidationLogLevel is not provided for a validation, fall back to
        self.default_level.
        """
        if isinstance(validations, Callable):
            return {validations: default_level}
        elif isinstance(validations, list):
            return {callable: default_level for callable in validations}
        elif isinstance(validations, dict):
            return validations

    def _build_entries(self, validation_def: dict, validation_result: Any,
                      level: str, rikolti_value: Any, comp_value: Any) -> None:
        """
        Given a validation result, adds to self.log.

        Builds additional default keys/values as needed.
        """
        if isinstance(validation_result, str):
            log_dict = {
                "description": validation_result,
                "level": level
            }
        elif isinstance(validation_result, dict):
            log_dict = {**validation_result, "level": level}
        elif not validation_result:
            log_dict = {
                "description": "Validation failed",
                "level": level
            }

        entry = {
            **self._default_log_entry(validation_def,
                                      rikolti_value,
                                      comp_value),
            **log_dict
        }

        self.log.add(**entry)

    def _default_log_entry(self, validation_def: dict[str, Any],
                            rikolti_value: Any, comparison_value: Any) -> dict:
        solr_query = f'harvest_id_s:"{self.key}"'
        solr = (
            "/solr/dc-collection/select?q="
            f"{urllib.parse.quote_plus(solr_query)}"
            "&wt=json&indent=true"
        )
        couch = f"/couchdb/_utils/document.html?ucldc/{self.key}"
        calisphere = f"/search/?q={urllib.parse.quote_plus(self.key)}"

        return {
            "key": self.key,
            "level": ValidationLogLevel.ERROR,
            "field": validation_def["field"],
            "description": "Validation failed",
            "actual": rikolti_value,
            "expected": comparison_value,
            "calisphere_prd": f"https://calisphere.org{calisphere}",
            "solr_prd": f"https://harvest-prd.cdlib.org{solr}",
            "couch_prd": f"https://harvest-prd.cdlib.org{couch}",
            "calisphere_test": f"https://calisphere-test.cdlib.org{calisphere}",
            "solr_stg": f"https://harvest-stg.cdlib.org{solr}",
            "couch_stg": f"https://harvest-stg.cdlib.org{couch}",
        }

    def _maybe_create_validation_success_entry(self) -> None:
        self.log.add(
            key=self.key,
            level=ValidationLogLevel.INFO,
            field=", ".join(self.successfully_validated_fields),
            description="Successfully validated"
        )

    @staticmethod
    def _normalize_validator_results(results: Any) -> list[Any]:
        """
        Accepts any arbitrary output from a validator function and
        turns it into a flat list.
        """
        if not isinstance(results, list):
            ret = [results]
        else:
            nested_result_lists = list(map(Validator._ensure_is_list, results))
            ret = itertools.chain(*nested_result_lists)

        return list(filter(None, ret))

    @staticmethod
    def _ensure_is_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else [value]


default_validatable_fields: list[dict[str, Any]] = [
    # Full fidelity fields
    # Full type and content matches required
    {
        "field": "id",
        "validations": [
                        Validator.content_match,
                        Validator.verify_type(str),
                        Validator.ark_type
        ]
    },
    {
        "field": "identifier",
        "validations": [
                        Validator.content_match,
                        Validator.verify_type(Validator.list_of(str))
                        ],
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "title",
        "validations": [
                        Validator.required_field,
                        Validator.content_match,
                        Validator.verify_type(Validator.list_of(str))
                        ]
    },
    {
        "field": "type",
        "validations": [
                        Validator.required_field,
                        Validator.content_match,
                        Validator.verify_type(Validator.list_of(str))
                        ]
    },
    {
        "field": "rights",
        "validations": [
                        Validator.content_match,
                        Validator.verify_type(Validator.list_of(str))
                        ]
    },
    {
        "field": "rights_uri",
        "validations": [
                        Validator.content_match,
                        Validator.verify_type(str),
                        ]
    },
    {
        "field": "is_shown_at",
        "validations": [
                        Validator.required_field,
                        Validator.content_match,
                        Validator.verify_type(str)
                        ]
    },
    {
        "field": "is_shown_by",
        "validations": [
                        Validator.required_field,
                        Validator.content_match,
                        Validator.verify_type(str)
                        ]
    },
    # Partial fidelity fields
    # Content match required; nulls okay
    {
        "field": "alternative_title",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "contributor",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "coverage",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "creator",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING,
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "date",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING,
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "description",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "extent",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "format",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING,
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "genre",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "language",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "location",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "provenance",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "publisher",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "relation",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_holder",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_note",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_date",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "source",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "spatial",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "subject",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING,
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "temporal",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING,
        "validation_mode": ValidationMode.ORDER_INSENSITIVE_IF_LIST
    },
    {
        "field": "transcription",
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    }
]

Validator.validatable_fields = default_validatable_fields