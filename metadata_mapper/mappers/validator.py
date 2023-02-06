from typing import Any, Callable, IO, Union

import itertools


class ValidationErrors:
    CSV_FIELDS: dict[str, str] = {
        "type": "Type",
        "field": "Field",
        "error": "Description",
        "expected": "Expected Value",
        "actual": "Actual Value"
    }

    def __init__(self):
        self.errors: list[dict[str, Any]] = []

    def has_errors(self):
        return True if len(self.errors) else False

    def is_empty(self):
        return not self.has_errors()

    def add(self, type: str, field: str, description: str,
            expected_value: Any, actual_value: Any, **context) -> None:
        self.errors.append({
            "type": type,
            "description": description,
            "field": field,
            "expected": expected_value,
            "actual": actual_value,
            **context
        })

    def output(self, format: str, file: IO[str] = None,
               **opts) -> list[Union[list, dict]]:
        """
        Generic method to output data.

        TODO: Expand types of output (i.e. to console for testing)
        """
        if format == "stdout":
            return print(self.errors)
        elif format == "csv" and file:
            return self.output_csv(file)

    def output_csv(self, file: IO[str], append: bool = False,
                   include_fields: list[str] = None) -> None:
        """
        Given a file, generates a CSV with error output.
        """
        if include_fields:
            headers = [
                        h for (f, h) in self.CSV_FIELDS
                        if f in include_fields
                      ]
            fields = [
                        f for f in self._default_fields
                        if f in include_fields
                     ]
        else:
            headers = self._default_headers
            fields = self._default_fields

        with open(file, "a" if append else "w") as f:
            if not append:
                self._write_csv_row(f, list(headers))
            for row in self.errors:
                self._write_csv_row(f,
                                    [val for (key, val) in row
                                     if key in fields]
                                    )

    def _write_csv_row(self, open_file: IO,
                       content: list[str]) -> None:
        open_file.write(f"{','.join(content)}\\n")

    @property
    def _default_fields(self) -> list[str]:
        return list(self.CSV_FIELDS.keys())

    @property
    def _default_headers(self) -> list[str]:
        return list(self.CSV_FIELDS.values())


class Validator:

    def __init__(self, mapped_metadata, comparison_metadata):
        self.mapped_metadata = mapped_metadata
        self.comparison_metadata = comparison_metadata
        self.errors = ValidationErrors()
        self.validatable_fields = default_validatable_fields

    def validate(self) -> ValidationErrors:
        """
        Performs validation of mapped_metadata against comparison_metadata.

        For each entry in validatable_fields, the defined validations
        are provided a key/value pair from both datasets and invoked.
        If validation fails, an error entry is added to self.errors,
        which is ultimately returned.

        Returns: ValidationErrors
        """
        for field in self.validatable_fields:
            self._perform_validations(field)

        return self.errors

    def set_validatable_fields(self, fields: list[dict],
                               merge: bool = True) -> list[dict]:
        """
        Set and/or overrides fields to be validated.

        If merge is True, merges provided fields into
        default_validatable_fields, overriding same-named keys.

        If merge is False, simply sets the fields to the provided list.

        Parameters:
            fields: list[dict]
                A list of dictionaries containing validation definitions.
            merge: bool (default: True)
                Should `fields` be merged into `default_validatable_fields`?

        Returns list[dict]
        """
        if merge:
            self.validatable_fields = {**default_validatable_fields, **fields}
        else:
            self.validatable_fields = fields

        return self.validatable_fields

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
    def full_match(validation_def: dict, mapped_value: Any,
                   comparison_value: Any) -> Union[list, None]:
        """
        Validates that both type and content match
        """
        result = [
            Validator.type_match(validation_def, mapped_value, mapped_value),
            Validator.content_match(validation_def, mapped_value, mapped_value)
        ]

        return [r for r in result if r is not None]

    @staticmethod
    def type_match(validation_def: dict, mapped_value: Any,
                   comparison_value: Any) -> Union[str, None]:
        """
        Validates that the value is of the expected type.
        """
        type_ref = validation_def["type"]

        if isinstance(type_ref, Callable):
            result = type_ref(mapped_value)
        elif isinstance(type_ref, type):
            result = isinstance(mapped_value, type_ref)
        elif isinstance(type_ref, list):
            result = any(isinstance(mapped_value, type) for type in type_ref)

        if not result:
            return "Type mismatch"

    @staticmethod
    def content_match(validation_def: dict, mapped_value: Any,
                      comparison_value: Any) -> None:
        """
        Validates that the content of the provided values is the equal.
        """
        if not mapped_value == comparison_value:
            return "Content mismatch"

    @staticmethod
    def required_field(self, validation_def: dict, mapped_value: Any,
                       comparison_value: Any) -> Union[str, None]:
        """
        Validates that a mapped value exists for a given field.

        In this context, anything Falsey will fail, such as empty iterables.
        If an empty iterable is acceptable, use #not_null.
        """
        if not mapped_value:
            return "Required field is null or empty"

    @staticmethod
    def not_null(self, validation_def: dict, mapped_value: Any,
                 comparison_value: Any) -> Union[str, None]:
        """
        Validates that a mapped value is not None.

        In this context, empty iterables or any other Falsey value
        except None will pass.
        """
        if mapped_value is None:
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
        return lambda value: Validator.nested_value(list, types, value)

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
        return lambda value: Validator.nested_value(dict, types, value)

    # Private

    def _perform_validations(self, validation_def: dict[str, Any]) -> None:
        """
        Runs validations for a given validation definition and adds to
        the errors list.
        """
        if not validation_def.get("validations"):
            return

        field = validation_def["field"]
        mapped_value = self.mapped_metadata.get(field)
        comp_value = self.comparison_data.get(field)

        for validator in validation_def["validations"]:
            raw_results = validator(validation_def, mapped_value, comp_value)
            normalized_results = self._normalize_validator_results(raw_results)

            for result in normalized_results:
                self._build_errors(validation_def, result,
                                   mapped_value, comp_value)

    def _build_errors(self, validation_def: dict, validation_results: list,
                      mapped_value: Any, comp_value: Any) -> None:
        """
        Given a validation failure result, adds to self.errors.

        Builds additional default keys/values as needed.
        """
        for result in validation_results:
            error = self._build_default_error(validation_def, result,
                                              mapped_value, comp_value)
            self.errors.add(**error)

    def _default_error(self, validation_def: dict[str, Any],
                       error_dict: dict[str, Any], mapped_value: Any,
                       comparison_value: Any) -> None:
        return {
            "field": validation_def["field"],
            "description": "Validation failed",
            "actual": mapped_value,
            "expected": comparison_value
        }

    @staticmethod
    def _normalize_validator_results(results: Any) -> list[Any]:
        """
        Accepts any arbitrary output from a validator function and
        turns it into a flat list.
        """
        if not isinstance(results, list):
            return [results]
        else:
            nested_result_lists = list(map(Validator._ensure_is_list, results))
            return itertools.chain(*nested_result_lists)

    @staticmethod
    def _ensure_is_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else [value]


default_validatable_fields: list[dict] = [
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
                        lambda l: len(l) == 1
                        ]
    },
    # Partial fidelity fields
    # Content match required; nulls okay
    {
        "field": "alternative_title",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "contributor",
        "type": Validator.list_of(str),
        "validations": [Validator.content_match]
    },
    {
        "field": "coverage",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "creator",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "date",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "description",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "extent",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "format",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "genre",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "language",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "location",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "provenance",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "publisher",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "relation",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "rights_holder",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "rights_note",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "rights_date",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "source",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "spatial",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "subject",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "temporal",
        "type": str,
        "validations": [Validator.content_match]
    },
    {
        "field": "transcription",
        "type": str,
        "validations": [Validator.content_match]
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
