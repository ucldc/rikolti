from typing import Any, Callable, IO, Union

# Validators


def full_match(validation_def: dict, mapped_value: Any,
               comparison_value: Any) -> None:
    """
    Validates that both type and content match
    """
    Validator.type_match(validation_def, mapped_value, mapped_value)
    Validator.content_match(validation_def, mapped_value, mapped_value)


def type_match(validation_def: dict, mapped_value: Any,
               comparison_value: Any) -> None:
    """
    Validates that the value is of the expected type.
    """
    type_or_method = validation_def["type"]
    if isinstance(type_or_method, Callable):
        type_or_method(mapped_value)
    elif isinstance(type_or_method, type):
        isinstance(mapped_value, type_or_method)
    elif isinstance(type_or_method, list):
        any(isinstance(mapped_value, type) for type in type_or_method)


def content_match(validation_def: dict, mapped_value: Any,
                  comparison_value: Any) -> None:
    """
    Validates that the content of the provided values is the equal.
    """
    pass

# Type helpers


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


default_validatable_fields = [
    # Full fidelity fields
    # Full type and content matches required
    {
        "field": "id",
        "type": str,
        "validations": [full_match],
        "required": True
    },
    {
        "field": "identifier",
        "type": list_of(str),
        "validations": [full_match],
        "required": True
    },
    {
        "field": "title",
        "type": list_of(str),
        "validations": [full_match],
        "required": True
    },
    {
        "field": "type",
        "type": list_of(str),
        "validations": [full_match],
        "required": True
    },
    {
        "field": "rights",
        "type": list_of(str),
        "validations": [full_match],
        "required": True
    },
    {
        "field": "rights_uri",
        "type": list_of(str),
        "validations": [
                        full_match,
                        lambda l: len(l) == 1
                        ],
        "required": True
    },
    # Partial fidelity fields
    # Content match required; nulls okay
    {
        "field": "alternative_title",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "contributor",
        "type": list_of(str),
        "validations": [content_match]
    },
    {
        "field": "coverage",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "creator",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "date",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "description",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "extent",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "format",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "genre",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "language",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "location",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "provenance",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "publisher",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "relation",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "rights_holder",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "rights_note",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "rights_date",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "source",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "spatial",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "subject",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "temporal",
        "type": str,
        "validations": [content_match]
    },
    {
        "field": "transcription",
        "type": str,
        "validations": [content_match]
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

repository_fields = [
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

excluded_fields = [
    'reference_image_md5',
    'reference_image_dimensions',
    'structmap_url',
    'url_item',
    'harvest_id_s'
]

search_fields = [
    'facet_decade',
    'sort_date_end',
    'sort_date_start',
    'sort_title',
]

harvest_fields = [
    '_version_',
    'harvest_id_s',
    'timestamp',
]

enrichment_fields = (
    repository_fields +
    excluded_fields +
    search_fields +
    harvest_fields
)


class Validator:

    class ValidationErrors:
        CSV_HEADER_ROWS: list[str] = [
            "Type",
            "Error",
            "Description",
            "Expected Value",
            "Actual Value"
        ]

        def __init__(self):
            self.errors: list[dict[str, Any]] = []

        def has_errors(self):
            return True if len(self.errors) else False

        def is_empty(self):
            return not self.has_errors()

        def add(self, type: str, field: str, expected_value: Any,
                actual_value: Any, **context) -> None:
            self.errors.append({
                "type": type,
                "field": field,
                "expected": expected_value,
                "actual": actual_value,
                **context
            })

        def output_csv(self, file: IO[str], append: bool = False) -> None:
            with open(file, "a" if append else "w") as f:
                if not append:
                    self._write_csv_row(f, self.CSV_HEADER_ROWS)
                for row in self.errors:
                    self._write_csv_row(f, [
                        row["type"],
                        row["field"],
                        row["description"],
                        row["expected"],
                        row["actual"]
                    ])

        def _write_csv_row(self, open_file: IO,
                           content: list[str]) -> None:
            open_file.write(f"{','.join(content)}\\n")

    def __init__(self, mapped_metadata, comparison_metadata):
        self.mapped_metadata = mapped_metadata
        self.comparison_metadata = comparison_metadata
        self.errors = Validator.ValidationErrors()
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
            self._validate_required(field)
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

    # Private

    def _validate_required(self, validation: dict[str, Any]) -> None:
        if not validation.get("required"):
            return

        if not self.mapped_metadata.get(validation["field"]):
            self.errors.add_error(validation["field"],
                                  "is required but value is blank."
                                  )

    def _perform_validations(self, validation: dict[str, Any]) -> None:
        if not validation.get("validations"):
            return

        [method(validation) for method in validation["validations"]]
