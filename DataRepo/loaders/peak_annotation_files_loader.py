import re
from collections import namedtuple
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
    UnicorrLoader,
)
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.models.archive_file import ArchiveFile, DataFormat, DataType
from DataRepo.utils.exceptions import RollbackException
from DataRepo.utils.file_utils import get_sheet_names, read_from_file


class PeakAnnotationFilesLoader(TableLoader):
    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    FILE_KEY = "FILE"
    FORMAT_KEY = "FORMAT"
    SEQNAME_KEY = "SEQNAME"

    DataSheetName = "Peak Annotation Files"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "FILE",
            "FORMAT",
            "SEQNAME",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        FILE="Peak Annotation File",
        FORMAT="File Format",
        SEQNAME="Default Sequence Name",
    )

    # List of required header keys
    DataRequiredHeaders = [FILE_KEY]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        FILE_KEY: str,
        FORMAT_KEY: str,
    }

    # DataDefaultValues not needed

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[FILE_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        ArchiveFile.__name__: {
            "filename": FILE_KEY,
            "data_format": FORMAT_KEY,
        }
    }

    DataColumnMetadata = DataTableHeaders(
        FILE=TableColumn.init_flat(
            name=DataHeaders.FILE,
            help_text="Peak annotation file, e.g. AccuCor, IsoCorr, etc.",
            guidance="Include a path if the file will not be in the top level of the submission directory.",
        ),
        FORMAT=TableColumn.init_flat(
            name=DataHeaders.FORMAT,
            value_required=False,
            help_text="Peak annotation file format.  Default: automatically detected.",
            static_choices=[
                (code, code) for code in PeakAnnotationsLoader.get_supported_formats()
            ],
        ),
        SEQNAME=TableColumn.init_flat(
            name=DataHeaders.SEQNAME,
            help_text=(
                f"The default MSRun Sequence to use when loading {DataHeaders.FILE}.  Overridden by values supplied in "
                f"{MSRunsLoader.DataHeaders.SEQNAME} in the {MSRunsLoader.DataSheetName} sheet."
            ),
            guidance=(
                "Use the dropdowns to select values in this column.  If the dropdowns are empty or the "
                f"sequence is missing, add a row for it to the {SequencesLoader.DataSheetName} sheet."
            ),
            type=str,
            format=(
                "Comma-delimited string combining the values from these columns from the "
                f"{SequencesLoader.DataSheetName} sheet in this order:\n"
                f"- {SequencesLoader.DataHeaders.OPERATOR}\n"
                f"- {SequencesLoader.DataHeaders.LCNAME}\n"
                f"- {SequencesLoader.DataHeaders.INSTRUMENT}\n"
                f"- {SequencesLoader.DataHeaders.DATE}"
            ),
            dynamic_choices=ColumnReference(
                loader_class=SequencesLoader,
                loader_header_key=SequencesLoader.SEQNAME_KEY,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [ArchiveFile]

    def load_data(self):
        """Loads the ArchiveFile table from the dataframe and calls the PeakAnnotationsLoader for each file.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for _, row in self.df.iterrows():
            # Determine the format
            filepath, format_code = self.get_file_and_format(row)

            # Load the ArchiveFile entry for the peak annotations file
            try:
                self.get_or_create_annot_file(filepath, format_code)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

            # Load the peak annotations
            self.load_peak_annotations(row, filepath, format_code)

    def get_file_and_format(self, row):
        """Gets the file path and determines the file format.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            filepath (str)
            format_code (str)
        """
        filepath = self.get_row_val(row, self.headers.FILE)
        format_code = self.get_row_val(row, self.headers.FORMAT)

        matching_formats = PeakAnnotationsLoader.determine_matching_formats(
            # Do not enforce column types when we don't know what columns exist yet
            read_from_file(filepath, sheet=None)
        )

        if format_code is not None:
            if format_code not in matching_formats and len(matching_formats) > 0:
                self.buffer_infile_exception(
                    (
                        f"The supplied {self.headers.FORMAT}: '{format_code}' does not match any of the automatically "
                        f"determined matching formats: {matching_formats}."
                    ),
                    is_error=False,
                )
        else:
            if len(matching_formats) == 1:
                format_code = matching_formats[0]
            elif len(matching_formats) == 0:
                self.buffer_infile_exception(
                    f"No matching formats.  Must be one of {PeakAnnotationsLoader.get_supported_formats()}.",
                    column=self.headers.FORMAT,
                )
            else:
                self.buffer_infile_exception(
                    f"Multiple matching formats: {matching_formats}.  Please enter one.",
                    column=self.headers.FORMAT,
                )

        return filepath, format_code

    @transaction.atomic
    def get_or_create_annot_file(self, filepath, format_code):
        """Gets or creates an ArchiveFile record from self.file.

        Args:
            filepath (str)
            format_code (str)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                DataType.DoesNotExist
                DataFormat.DoesNotExist
        Returns:
            rec (ArchiveFile)
            created (boolean)
        """
        rec = None
        created = False

        if filepath is None or format_code is None:
            self.skipped(ArchiveFile.__name__)
            return rec, created

        # Get or create the ArchiveFile record for the mzXML
        try:
            rec_dict = {
                # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "is_binary": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "file_location": filepath,  # Intentionally a string and not a File object
                "data_type": DataType.objects.get(code="ms_peak_annotation"),
                "data_format": DataFormat.objects.get(code=format_code),
            }

            rec, created = ArchiveFile.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()
                self.created(ArchiveFile.__name__)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, rec_dict)
            self.errored(ArchiveFile.__name__)
            raise RollbackException()

        return rec, created

    def load_peak_annotations(self, row, filepath, format_code):
        """Loads the peak annotations file reference in the row and supplies a default sequence, if supplied.

        Args:
            row (pd.Series)
            filepath (str)
            format_code (str)
        Exceptions:
            None
        Returns:
            None
        """
        sequence_name = self.get_row_val(row, self.headers.SEQNAME)

        if (
            filepath is None
            or format_code is None
            or format_code not in PeakAnnotationsLoader.get_supported_formats()
        ):
            self.buffer_infile_exception(
                (
                    f"Skipping load of peak annotations file {filepath}.  Unrecognized format code: {format_code}.  "
                    f"Must be one of {PeakAnnotationsLoader.get_supported_formats()}."
                ),
                column=self.headers.FORMAT,
                is_error=False,
            )
            return

        if format_code == AccucorLoader.format_code:
            peak_annot_loader_class = AccucorLoader
        elif format_code == IsocorrLoader.format_code:
            peak_annot_loader_class = IsocorrLoader
        elif format_code == IsoautocorrLoader.format_code:
            peak_annot_loader_class = IsoautocorrLoader
        elif format_code == UnicorrLoader.format_code:
            peak_annot_loader_class = UnicorrLoader

        # Get the peak annotation details
        peak_annotation_details_file = None
        peak_annotation_details_sheet = None
        peak_annotation_details_df = None

        if MSRunsLoader.DataSheetName in get_sheet_names(self.file):
            peak_annotation_details_file = self.file
            peak_annotation_details_sheet = MSRunsLoader.DataSheetName
            peak_annotation_details_df = read_from_file(
                peak_annotation_details_file,
                peak_annotation_details_sheet,
                # TODO: Add dtypes argument here
            )

        # Get the default sequence (if any).  This can be overridden by peak_annotation_details_df
        default_operator = None
        default_date = None
        default_lc_protocol_name = None
        default_instrument = None

        if sequence_name is not None:
            (
                default_operator,
                default_date,
                default_lc_protocol_name,
                default_instrument,
            ) = re.split(r",\s*", sequence_name)

        # Create an instance of the specific peak annotations loader for this format
        peak_annot_loader = peak_annot_loader_class(
            peak_annotation_details_file=peak_annotation_details_file,
            peak_annotation_details_sheet=peak_annotation_details_sheet,
            peak_annotation_details_df=peak_annotation_details_df,
            operator=default_operator,
            date=default_date,
            lc_protocol_name=default_lc_protocol_name,
            instrument=default_instrument,
        )

        # Load this peak annotations file
        peak_annot_loader.load_data()
