from datetime import datetime
from enum import Enum
from typing import IO, Any

from .. import utilities


class ValidationLogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class ValidationLog:    
    CSV_FIELDS: dict[str, str] = {
        "harvest_id": "Harvest ID",
        "level": "Level",
        "field": "Field",
        "description": "Description",
        "expected": "Expected Value",
        "actual": "Actual Value"
    }

    def __init__(self, log_level: ValidationLogLevel = ValidationLogLevel.WARNING):
        self.log: list[dict[str, Any]] = []
        self.level = log_level

    def set_log_level(self, log_level: ValidationLogLevel) -> None:
        self.level = log_level

    def has_entries(self):
        return True if len(self.log) else False

    def is_empty(self):
        return not self.has_entries()

    def add(self, key: str, field: str, description: str, expected: Any = "",
            actual: Any = "", level: ValidationLogLevel = ValidationLogLevel.ERROR,
            **context) -> None:
        """
        Adds an entry to the log.

        Parameters:
            key: str
                The harvest ID (or other key) used to join the Rikolti
                and comparison record.
            field: str
                The field on which the entry occurred
            description: str
                Entry description
            expected: Any
                Expected value (probably from Solr)
            actual: Any
                Actual value (from Rikolti mapper)
            level: ValidationLogLevel
                Entry log level (ERROR, WARN, etc)
            context: dict
                Additional values to be added (will not have headers)
        """
        if not self._should_log(level):
            return
        
        self.log.append({
            "harvest_id": key,
            "level": level,
            "description": description,
            "field": field,
            "expected": str(expected),
            "actual": str(actual),
            **context
        })

    def merge(self, other_log: "ValidationLog") -> "ValidationLog":
        """
        Merges log list with another.

        Parameters:
            other_log: ValidationLog
                Another ValidationLog instance to merge in

        Returns: ValidationLog
        """
        if not other_log.log:
            return self.log

        self.log = self.log + other_log.log
        return self.log

    def output_csv_to_file(self, file: IO[str], append: bool = False,
                           include_fields: list[str] = None) -> None:
        """
        Given a file, generates a CSV with log output.

        Parameters:
            file: IO[str]
                File path to write to
            append: bool (default: False)
                Should this log be appended to the file?
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
        
        return filename

    def _csv_content(self, include_fields: list[str] = None,
                     include_headers: bool = True) -> list[list[str]]:
        """
        Generates a list from log entries suitable for generating a CSV.

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
        for row in self.log:
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

    def _should_log(self, log_level) -> bool:
        for level in list(ValidationLogLevel.__members__):
            if level == self.level.value:
                return True
            elif level == log_level.value:
                return False

    @property
    def _default_fields(self) -> list[str]:
        return list(self.CSV_FIELDS.keys())

    @property
    def _default_headers(self) -> list[str]:
        return list(self.CSV_FIELDS.values())