#!/usr/bin/env python3
"""Psteal to Parquet utility.

Extracts events from a source and outputs them to Parquet format without deduplication.
This mimics the behavior of psteal but forces deduplication off and uses a custom
Parquet output module that summarizes the fields similarly to jsonl.
"""

import argparse
import json
import logging
import multiprocessing
import os
import sys
from datetime import datetime, timezone

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("Error: pyarrow is required. Please install it using 'pip install pyarrow'")
    sys.exit(1)

# Ensure Plaso is available in the path
from plaso import dependencies
from plaso.cli import psteal_tool
from plaso.cli import logger as cli_logger
from plaso.cli.helpers import manager as helpers_manager
from plaso.lib import errors
from plaso.lib import loggers
from plaso.output import manager as output_manager
from plaso.output import shared_json


class ParquetOutputModule(shared_json.SharedJSONOutputModule):
    """Output module for the Parquet format."""

    NAME = "parquet"
    DESCRIPTION = "Saves the events into a Parquet format."

    def __init__(self):
        """Initializes a Parquet output module."""
        super().__init__()
        self._batch = []
        self._batch_size = 50000
        self._writer = None
        self._parquet_file_path = None
        self._schema = pa.schema(
            [
                ("timestamp", pa.timestamp("us", tz="UTC")),
                ("data_type", pa.string()),
                ("parser", pa.string()),
                ("message", pa.string()),
                ("display_name", pa.string()),
                ("file_name_lower", pa.string()),
                ("tag", pa.string()),
                ("details", pa.string()),  # JSON string
            ]
        )

    def Open(self, path=None, **kwargs):
        """Opens the Parquet output writer.

        Args:
          path (Optional[str]): path of the output file.

        Raises:
          OSError: if the specified output file already exists.
          ValueError: if path is not set.
        """
        if not path:
            raise ValueError("Missing path.")

        if os.path.isfile(path):
            raise OSError(
                f"Unable to use an already existing file for output [{path:s}]"
            )

        self._parquet_file_path = path
        self._writer = pq.ParquetWriter(
            self._parquet_file_path, self._schema, compression="snappy"
        )

    def Close(self):
        """Closes the Parquet output writer."""
        if self._batch and self._writer:
            table = pa.Table.from_pylist(self._batch, schema=self._schema)
            self._writer.write_table(table)
            self._batch = []

        if self._writer:
            self._writer.close()
            self._writer = None

    def WriteFieldValues(self, output_mediator, field_values):
        """Writes field values to the output.

        Args:
          output_mediator (OutputMediator): mediates interactions between output
              modules and other components, such as storage and dfVFS.
          field_values (dict[str, str]): output field values per name.
        """
        timestamp_val = field_values.get("timestamp")
        try:
            if timestamp_val is not None:
                ts = datetime.fromtimestamp(timestamp_val / 1000000, tz=timezone.utc)
            else:
                ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        except (ValueError, OSError, OverflowError, TypeError):
            ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

        data_type = field_values.get("data_type", "N/A")
        parser = field_values.get("parser", "N/A")
        message = field_values.get("message", "N/A")
        display_name = field_values.get("display_name", "N/A")

        filename = field_values.get("filename", None)
        file_name_lower = (filename or display_name or "").lower()

        tag_dict = field_values.get("tag", {})
        tag_labels = tag_dict.get("labels", []) if isinstance(tag_dict, dict) else []
        tag_str = ",".join(tag_labels) if tag_labels else ""

        # Exclude common fields already in the top level of the schema
        excluded = {
            "__container_type__",
            "__type__",
            "timestamp",
            "data_type",
            "parser",
            "message",
            "filename",
            "display_name",
            "tag",
        }

        details_dict = {}
        for k, v in field_values.items():
            if k not in excluded and not k.startswith("_"):
                details_dict[k] = v

        details_json = json.dumps(details_dict, default=str)

        self._batch.append(
            {
                "timestamp": ts,
                "data_type": str(data_type),
                "parser": str(parser),
                "message": str(message),
                "display_name": str(display_name),
                "file_name_lower": str(file_name_lower),
                "tag": str(tag_str),
                "details": details_json,
            }
        )

        if len(self._batch) >= self._batch_size and self._writer:
            table = pa.Table.from_pylist(self._batch, schema=self._schema)
            self._writer.write_table(table)
            self._batch = []


# Register the module so output_manager can find it
output_manager.OutputManager.RegisterOutput(ParquetOutputModule)


class PstealParquetTool(psteal_tool.PstealTool):
    """Psteal to Parquet CLI tool."""

    NAME = "psteal_parquet"

    def __init__(self, input_reader=None, output_writer=None):
        """Initializes the CLI tool object.

        Args:
          input_reader (Optional[InputReader]): input reader, where None indicates
              that the stdin input reader should be used.
          output_writer (Optional[OutputWriter]): output writer, where None
              indicates that the stdout output writer should be used.
        """
        super().__init__(input_reader=input_reader, output_writer=output_writer)
        # Disable deduplication as requested
        self._deduplicate_events = False

    def ParseArguments(self, arguments):
        """Parses the command line arguments.

        Args:
          arguments (list[str]): command line arguments.

        Returns:
          bool: True if the arguments were successfully parsed.
        """
        loggers.ConfigureLogging()

        argument_parser = argparse.ArgumentParser(
            description=self.DESCRIPTION,
            epilog=self.EPILOG,
            add_help=False,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        self.AddBasicOptions(argument_parser)

        data_location_group = argument_parser.add_argument_group(
            "data location arguments"
        )

        argument_helper_names = ["artifact_definitions", "data_location"]
        helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
            data_location_group, names=argument_helper_names
        )

        extraction_group = argument_parser.add_argument_group("extraction arguments")

        # We add "artifact_filters" here to support --artifact-filters
        argument_helper_names = [
            "archives",
            "artifact_filters",
            "extraction",
            "hashers",
            "parsers",
        ]
        helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
            extraction_group, names=argument_helper_names
        )

        self.AddStorageOptions(extraction_group)
        self.AddStorageMediaImageOptions(extraction_group)
        self.AddExtractionOptions(extraction_group)
        self.AddVSSProcessingOptions(extraction_group)
        self.AddCredentialOptions(extraction_group)

        info_group = argument_parser.add_argument_group("informational arguments")

        self.AddInformationalOptions(info_group)

        info_group.add_argument(
            "--no_dependencies_check",
            "--no-dependencies-check",
            dest="dependencies_check",
            action="store_false",
            default=True,
            help="Disable the dependencies check.",
        )

        helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
            info_group, names=["status_view"]
        )

        input_group = argument_parser.add_argument_group("input arguments")
        input_group.add_argument(
            "--source",
            dest="source",
            action="store",
            type=str,
            help="The source to process",
        )

        output_group = argument_parser.add_argument_group("output arguments")

        self.AddOutputOptions(output_group)

        output_format_group = argument_parser.add_argument_group(
            "output format arguments"
        )

        helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
            output_format_group, names=["output_modules"]
        )

        processing_group = argument_parser.add_argument_group("processing arguments")

        self.AddPerformanceOptions(processing_group)
        self.AddProcessingOptions(processing_group)

        try:
            options = argument_parser.parse_args(arguments)
        except UnicodeEncodeError:
            # If we get here we are attempting to print help in a non-Unicode
            # terminal.
            self._output_writer.Write("\n")
            self._output_writer.Write(argument_parser.format_help())
            return False

        # Properly prepare the attributes according to local encoding.
        if self.preferred_encoding == "ascii":
            self._PrintUserWarning(
                (
                    "the preferred encoding of your system is ASCII, which is not "
                    "optimal for the typically non-ASCII characters that need to be "
                    "parsed and processed. This will most likely result in an error."
                )
            )

        try:
            self.ParseOptions(options)
        except errors.BadConfigOption as exception:
            self._output_writer.Write(f"ERROR: {exception!s}\n")
            self._output_writer.Write("\n")
            self._output_writer.Write(argument_parser.format_usage())
            return False

        self._WaitUserWarning()

        loggers.ConfigureLogging(
            debug_output=self._debug_mode,
            filename=self._log_file,
            quiet_mode=self._quiet_mode,
        )

        return True

    def ParseOptions(self, options):
        """Parses tool specific options.

        Args:
          options (argparse.Namespace): command line arguments.
        """
        # Force the output format to be our parquet module
        setattr(options, "output_format", "parquet")

        # Parse the artifact_filters option so self._artifact_filters is populated
        helpers_manager.ArgumentHelperManager.ParseOptions(
            options, self, names=["artifact_filters"]
        )

        super().ParseOptions(options)


def Main():
    """Entry point of console script to extract and output events to parquet.

    Returns:
      int: exit code that is provided to sys.exit().
    """
    tool = PstealParquetTool()

    if not tool.ParseArguments(sys.argv[1:]):
        return 1

    if tool.show_troubleshooting:
        print(f"Using Python version {sys.version!s}")
        print()
        print(tool.GetVersionInformation())
        print()
        dependencies.CheckDependencies(verbose_output=True)
        return 0

    try:
        tool.CheckOutDated()
    except KeyboardInterrupt:
        return 1

    have_list_option = False
    if tool.list_archive_types:
        tool.ListArchiveTypes()
        have_list_option = True

    if tool.list_hashers:
        tool.ListHashers()
        have_list_option = True

    if tool.list_language_tags:
        tool.ListLanguageTags()
        have_list_option = True

    if tool.list_output_modules:
        tool.ListOutputModules()
        have_list_option = True

    if tool.list_parsers_and_plugins:
        tool.ListParsersAndPlugins()
        have_list_option = True

    if tool.list_time_zones:
        tool.ListTimeZones()
        have_list_option = True

    if have_list_option:
        return 0

    if tool.dependencies_check and not dependencies.CheckDependencies(
        verbose_output=False
    ):
        return 1

    try:
        tool.ExtractEventsFromSources()
        tool.ProcessStorage()

    # Writing to stdout and stderr will raise BrokenPipeError if it
    # receives a SIGPIPE.
    except BrokenPipeError:
        pass

    except (KeyboardInterrupt, errors.UserAbort):
        logging.warning("Aborted by user.")
        return 1

    except errors.SourceScannerError as exception:
        logging.warning(exception)
        return 1

    return 0


if __name__ == "__main__":
    multiprocessing.freeze_support()
    sys.exit(Main())
