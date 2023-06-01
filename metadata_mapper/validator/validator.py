import itertools

from typing import Any, Callable, Union

from .validation_log import ValidationLog, ValidationLogLevel
from .validation_mode import ValidationMode

class Validator:

    def __init__(self, fields: list[dict[str, Any]] = None,
                 **options):
        self.log = ValidationLog(options.get("log_level", ValidationLogLevel.WARNING))
        self.set_validatable_fields(fields or {})
        self.default_validation_mode = options.get("default_validation_mode",
                                                   ValidationMode.STRICT)
        self.verbose = options.get("verbose", False)

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

            self.after_validation()

            self._maybe_create_validation_success_entry()

        return self.log

    def set_validatable_fields(self, fields: list[dict[str, Any]] = [],
                               merge: bool = True) -> list[dict]:
        """
        Set and/or overrides fields to be validated.

        If merge is True, merges provided fields into
        default_validatable_fields, overriding same-named keys.

        If merge is False, simply sets the fields to the provided list.

        Parameters:
            fields: list[dict[str, Any]] (default: [])
                A list of dicts containing validation definitions.
                See default_validatable_fields for examples.
            merge: bool (default: True)
                Should `fields` be merged into `default_validatable_fields`?

        Returns list[dict]
        """
        if merge:
            field_dict = {
                **{d["field"]: d for d in default_validatable_fields},
                **{d["field"]: d for d in fields}
            }

            self.validatable_fields = [v for k, v in field_dict.items()]
        else:
            self.validatable_fields = fields

        return self.validatable_fields

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
    def full_match(validation_def: dict, rikolti_value: Any,
                   comparison_value: Any) -> Union[list, None]:
        """
        Validates that both type and content match
        """
        result = [
            Validator.type_match(validation_def,
                                 rikolti_value,
                                 comparison_value),
            Validator.content_match(validation_def,
                                    rikolti_value,
                                    comparison_value)
        ]

        return [r for r in result if r is not None]

    @staticmethod
    def type_match(validation_def: dict, rikolti_value: Any,
                   _: Any) -> Union[str, None]:
        """
        Validates that the value is of the expected type.
        """
        expected_type = validation_def["type"]

        if isinstance(expected_type, Callable):
            result = expected_type(rikolti_value)
        elif isinstance(expected_type, type):
            result = isinstance(rikolti_value, expected_type)
        elif isinstance(expected_type, list):
            result = any(isinstance(rikolti_value, type) for type in expected_type)

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

            if len(normalized_results) > 1:
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
                            rikolti_value: Any, comparison_value: Any) -> None:
        return {
            "key": self.key,
            "level": ValidationLogLevel.ERROR,
            "field": validation_def["field"],
            "description": "Validation failed",
            "actual": rikolti_value,
            "expected": comparison_value
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
        "type": str,
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "identifier",
        "type": Validator.list_of(str),
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "title",
        "type": Validator.list_of(str),
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "type",
        "type": Validator.list_of(str),
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "rights",
        "type": Validator.list_of(str),
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "rights_uri",
        "type": Validator.list_of(str),
        "validations": [
                        Validator.required_field,
                        Validator.full_match,
                        lambda d, r, c: isinstance(r, list) and len(r) == 1
                        ]
    },
    {
        "field": "isShownAt",
        "type": str,
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    {
        "field": "isShownBy",
        "type": str,
        "validations": [
                        Validator.required_field,
                        Validator.full_match
                        ]
    },
    # Partial fidelity fields
    # Content match required; nulls okay
    {
        "field": "alternative_title",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "contributor",
        "type": Validator.list_of(str),
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "coverage",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "creator",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "date",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "description",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "extent",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "format",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "genre",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "language",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "location",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "provenance",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "publisher",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "relation",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_holder",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_note",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "rights_date",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "source",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "spatial",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "subject",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "temporal",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    },
    {
        "field": "transcription",
        "type": str,
        "validations": [Validator.content_match],
        "level": ValidationLogLevel.WARNING
    }
]

valid_types: list[str] = [
    "collection",
    "dataset",
    "event",
    "image",
    "interactive resource",
    "moving image",
    "physical object",
    "service",
    "software",
    "sound",
    "text",
]

repository_fields: list[str] = [
    'campus_data',
    'campus_name',
    'campus_url',
    'collection_data',
    'collection_name',
    'collection_url',
    'repository_data',
    'repository_name',
    'repository_url',
    'sort_collection_data',
]

excluded_fields: list[str] = [
    'reference_image_md5',
    'reference_image_dimensions',
    'structmap_url',
    'url_item',
    'harvest_id_s'
]

search_fields: list[str] = [
    'facet_decade',
    'sort_date_end',
    'sort_date_start',
    'sort_title',
]

harvest_fields: list[str] = [
    '_version_',
    'harvest_id_s',
    'timestamp',
]

enrichment_fields: list[str] = (
    repository_fields +
    excluded_fields +
    search_fields +
    harvest_fields
)
