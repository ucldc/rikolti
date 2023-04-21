import itertools

from datetime import datetime
from typing import Any, Callable, IO, Union

import utilities


class ValidationErrors:
    CSV_FIELDS: dict[str, str] = {
        "harvest_id": "Harvest ID",
        "level": "Level",
        "field": "Field",
        "description": "Description",
        "expected": "Expected Value",
        "actual": "Actual Value"
    }

    def __init__(self):
        self.errors: list[dict[str, Any]] = []

    def has_errors(self):
        return True if len(self.errors) else False

    def is_empty(self):
        return not self.has_errors()

    def add(self, key: str, field: str, description: str, expected: Any,
            actual: Any, level: str = "ERROR", **context) -> None:
        """
        Adds an error to the errors list.

        Parameters:
            key: str
                The harvest ID (or other key) used to join the Rikolti
                and comparison record.
            field: str
                The field on which the error occurred
            description: str
                Error description
            expected: Any
                Expected value (probably from Solr)
            actual: Any
                Actual value (from Rikolti mapper)
            type: str
                Error type (ERROR, WARN, etc)
            context: dict
                Additional values to be added (will not have headers)
        """
        self.errors.append({
            "harvest_id": key,
            "level": level,
            "description": description,
            "field": field,
            "expected": str(expected),
            "actual": str(actual),
            **context
        })

    def merge(self, other_errors: "ValidationErrors") -> "ValidationErrors":
        """
        Merges errors list with another.

        Parameters:
            other_errors: ValidationErrors
                Another ValidationErrors instance to merge in

        Returns: ValidationErrors
        """
        if not other_errors.errors:
            return

        self.errors = self.errors + other_errors.errors
        return self.errors

    def output_csv_to_file(self, file: IO[str], append: bool = False,
                           include_fields: list[str] = None) -> None:
        """
        Given a file, generates a CSV with error output.

        Parameters:
            file: IO[str]
                File path to write to
            append: bool (default: False)
                Should errors be appended to the file?
            include_fields: list[str] (default: None)
                List of fields to include in the output
        """
        with open(file, "a" if append else "w") as f:
            f.write(self._csv_content_string(include_fields, append))

    def output_csv_to_bucket(self, collection_id: int, filename: str = None,
                             include_fields: list[str] = None) -> None:
        """
        Writes a CSV to the env-appropriate bucket (local or S3).

        Parameters:
            collection_id: int
                The collection ID (for finding appropriate folder)
            filename: str (default: None)
                The name of the created file. If not provided, defaults to
                timestamp
            include_fields: list[str] (default: None)
                A list of fields to include in the CSV. Defaults to all.
        """
        if not filename:
            filename = f"{datetime.now().strftime('%m-%d-%YT%H:%M:%S')}.csv"

        utilities.write_to_bucket("validation", collection_id, filename,
                                  self._csv_content_string(include_fields))

    def _csv_content(self, include_fields: list[str] = None,
                     include_headers: bool = True) -> list[list[str]]:
        """
        Generates a list from errors suitable for generating a CSV.

        Parameters:
            include_fields: list[str] (default: None)
                List of fields to include in the CSV. Defaults to all.
            include_headers: bool (default: True)
                Should a list of header values be added at the top?

        Returns: list[list[str]]
        """
        def escape_csv_value(value):
            return '"' + str(value).replace('"', '""') + '"'

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

        ret = [headers] if include_headers else []
        for row in self.errors:
            ret.append(
                [
                    escape_csv_value(val) for key, val in row.items()
                    if key in fields
                ]
            )

        return ret

    def _csv_content_string(self, include_fields: list[str] = None,
                            include_headers: bool = True) -> str:
        """
        Generates a string of CSV content suitable for writing to a file.

        Parameters:
            include_fields: list[str] (default: None)
                List of fields to include in the CSV. Defaults to all.
            include_headers: bool (default: True)
                Should a list of header values be added at the top?

        Returns: str
        """
        content = self._csv_content(include_fields, include_headers)
        content_strings = [",".join(row) for row in content]
        return "\n".join(content_strings)

    @property
    def _default_fields(self) -> list[str]:
        return list(self.CSV_FIELDS.keys())

    @property
    def _default_headers(self) -> list[str]:
        return list(self.CSV_FIELDS.values())


class Validator:

    def __init__(self, fields: list[dict[str, Any]] = None):
        self.errors = ValidationErrors()
        self.set_validatable_fields(fields or {})

    def validate(self, key: str, rikolti_data: dict,
                 comparison_metadata: dict) -> ValidationErrors:
        """
        Performs validation of mapped_metadata against comparison_metadata.

        For each entry in validatable_fields, the defined validations
        are provided a key/value pair from both datasets and invoked.
        If validation fails, an error entry is added to self.errors,
        which is ultimately returned.

        Parameters:
            rikolti_data: dict[str, Any]
                Rikolti record data
            comparison_data: dict[str, Any]
                Comparison (usually Solr) data

        Returns: ValidationErrors
        """
        self.key = key
        self.rikolti_data = rikolti_data
        self.comparison_data = comparison_metadata

        if not self.rikolti_data or not self.comparison_data:
            missing_data_desc = "mapped Rikolti" if not self.rikolti_data else "Solr"
            error = {
                "key": self.key,
                "level": "ERROR",
                "description": f"No {missing_data_desc} data found for key"
            }
            self.errors.add(**error)

        self.before_validation()

        for field in self.validatable_fields:
            self._perform_validations(field)

        return self.errors

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
                   comparison_value: Any) -> Union[str, None]:
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
        if not rikolti_value == comparison_value:
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
        the errors list.
        """
        if not validation_def.get("validations"):
            return

        field = validation_def["field"]
        rikolti_value = self.rikolti_data.get(field)
        comp_value = self.comparison_data.get(field)

        validations = self._normalize_validations(validation_def["validations"], validation_def["level"])

        for validator, level in validations.items():
            raw_results = validator(validation_def, rikolti_value, comp_value)
            normalized_results = self._normalize_validator_results(raw_results)

            for result in normalized_results:
                self._build_errors(validation_def, result, level,
                                   rikolti_value, comp_value)

    def _normalize_validations(self, validations: Union[
                                                        Callable,
                                                        list[Callable],
                                                        dict[Callable, str]
                                                        ],
                               default_level: str = "ERROR"
                               ) -> dict[Callable, str]:
        if isinstance(validations, Callable):
            return {validations: default_level}
        elif isinstance(validations, list):
            return {callable: default_level for callable in validations}
        elif isinstance(validations, dict):
            return validations

    def _build_errors(self, validation_def: dict, validation_result: Any,
                      level: str, rikolti_value: Any, comp_value: Any) -> None:
        """
        Given a validation failure result, adds to self.errors.

        Builds additional default keys/values as needed.
        """
        if isinstance(validation_result, str):
            error_dict = {
                "description": validation_result,
                "level": level
            }
        elif isinstance(validation_result, dict):
            error_dict = {**validation_result, "level": level}
        elif not validation_result:
            error_dict = {
                "description": "Validation failed",
                "level": level
            }

        error = {
            **self._default_error(validation_def,
                                  rikolti_value,
                                  comp_value),
            **error_dict
        }

        self.errors.add(**error)

    def _default_error(self, validation_def: dict[str, Any],
                       rikolti_value: Any, comparison_value: Any) -> None:
        return {
            "key": self.key,
            "level": "ERROR",
            "field": validation_def["field"],
            "description": "Validation failed",
            "actual": rikolti_value,
            "expected": comparison_value
        }

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
    # Partial fidelity fields
    # Content match required; nulls okay
    {
        "field": "alternative_title",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "contributor",
        "type": Validator.list_of(str),
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "coverage",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "creator",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "date",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "description",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "extent",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "format",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "genre",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "language",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "location",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "provenance",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "publisher",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "relation",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "rights_holder",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "rights_note",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "rights_date",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "source",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "spatial",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "subject",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "temporal",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
    },
    {
        "field": "transcription",
        "type": str,
        "validations": [Validator.content_match],
        "level": "WARN"
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
