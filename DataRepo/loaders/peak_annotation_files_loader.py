import os
from collections import defaultdict, namedtuple
from typing import Dict

from django.db import transaction

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
    UnicorrLoader,
)
from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.models.archive_file import ArchiveFile, DataFormat, DataType
from DataRepo.models.msrun_sequence import MSRunSequence
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AggregatedErrorsSet,
    FileFromInputNotFound,
    RollbackException,
)
from DataRepo.utils.file_utils import get_sheet_names, is_excel, read_from_file


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
        SEQNAME="Default Sequence",
    )

    # List of required header keys
    DataRequiredHeaders = [FILE_KEY]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # The type of data in each column (used by pandas to not, for example, turn "1" into an integer then str is set)
    DataColumnTypes: Dict[str, type] = {
        FILE_KEY: str,
        FORMAT_KEY: str,
        SEQNAME_KEY: str,
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

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        FILE=TableColumn.init_flat(
            name=DataHeaders.FILE,
            help_text="Peak annotation file, e.g. AccuCor, IsoCorr, etc.",
            guidance="Include a path if the file will not be in the top level of the submission directory.",
            # TODO: Replace "Peak Annotation Details" and "Peak Annotation File" below with a reference to its loader's
            # DataSheetName and the corresponding column, respectively.
            # Cannot reference the MSRunsLoader here (to include the name of its sheet and its file column) due to
            # circular import.
            reference=ColumnReference(
                sheet="Peak Annotation Details",
                header="Peak Annotation File Name",
            ),
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
            # TODO: Replace "Peak Annotation Details" and "Sequence" below with a reference to its loader's
            # DataSheetName and the corresponding column, respectively.
            # Cannot reference the MSRunsLoader here (to include the name of its sheet and its file column) due to
            # circular import.
            help_text=(
                f"The default Sequence to use when loading {DataHeaders.FILE}.  Overridden by values supplied in "
                f"the 'Peak Annotation File Name' column in the 'Peak Annotation Details' sheet."
            ),
            type=str,
            format=(
                f"Refer to the {SequencesLoader.DataHeaders.SEQNAME} column in the {SequencesLoader.DataSheetName} "
                "sheet for format details."
            ),
            dynamic_choices=ColumnReference(
                loader_class=SequencesLoader,
                loader_header_key=SequencesLoader.SEQNAME_KEY,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [ArchiveFile]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Limitations:
            Custom headers for the peak annotation details file are not (yet) supported.  Only the class defaults of the
                MSRunsLoader are allowed.

        *NOTE: This constructor requires the file argument (which is an optional argument to the superclass) if the df
        argument is supplied.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]): Sheet name (for error reporting).
                defaults_sheet (Optional[str]): Sheet name (for error reporting).
                file (Optional[str]): File path.
                filename (Optional[str]): Filename (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]): Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
                _validate (bool): If true, runs in validate mode, perhaps better described as "non-curator mode".  This
                    is intended for use by the web validation interface.  It's similar to dry-run mode, in that it never
                    commits anything, but it also raises warnings as fatal (so they can be reported through the web
                    interface and seen by researchers, among other behaviors specific to non-privileged users).
            Derived (this) class Args:
                annot_files_dict (Optional[Dict[str, str]]): This is a dict of peak annotation file paths keyed on peak
                    annotation file basename.  This is not necessary on the command line.  It is only provided for the
                    purpose of web forms, where the name of the actual file is a randomized hash string at the end of a
                    temporary path.  This dict associates the user's readable filename parsed from the infile (the key)
                    with the actual file (the value).
                Sample, MSRunSequence, and mzXML data:
                    peak_annotation_details_file (Optional[str]): The name of the file that the Peak Annotation Details
                        came from.
                    peak_annotation_details_sheet (Optional[str]): The name of the sheet that the Peak Annotation
                        Details came from (if it was an excel file).
                    peak_annotation_details_df (Optional[pandas DataFrame]): The DataFrame of the Peak Annotation
                        Details sheet/file that will be supplied to the MSRunsLoader class (that is an instance meber of
                        this instance)
                PeakGroup conflicts (a.k.a. "multiple representations"):
                    peak_group_conflicts_file (Optional[str]): The name of the file that the Peak Group conflict
                        resolutions came from.
                    peak_group_conflicts_sheet (Optional[str]): The name of the sheet that the Peak Group conflict
                        resolutions came from (if it was an excel file).
                    peak_group_conflicts_df (Optional[pandas DataFrame]): The DataFrame of the Peak Group conflict
                        resolutions sheet/file that will be supplied to the PeakGroupConflicts class (that is an
                        instance member of this instance) and is used to skip peak groups based on user selections.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        # Custom options for the MSRunsLoader member instance.
        # NOTE: We COULD make a friendly version of this file for the web interface, but we don't accept this separate
        # file in that interface, so it will currently always be set using self.file.  These options will only
        # effectively be used on the command line, where self.file *is* already friendly.
        self.annot_files_dict = kwargs.pop("annot_files_dict", None)

        self.peak_annotation_details_file = kwargs.pop(
            "peak_annotation_details_file", None
        )
        self.peak_annotation_details_sheet = kwargs.pop(
            "peak_annotation_details_sheet", None
        )
        self.peak_annotation_details_df = kwargs.pop("peak_annotation_details_df", None)

        self.peak_group_conflicts_file = kwargs.pop("peak_group_conflicts_file", None)
        self.peak_group_conflicts_sheet = kwargs.pop("peak_group_conflicts_sheet", None)
        self.peak_group_conflicts_df = kwargs.pop("peak_group_conflicts_df", None)

        super().__init__(*args, **kwargs)

        # For tracking exceptions of the individual peak annotation loaders
        self.aggregated_errors_dict = {}

    def load_data(self):
        """Loads the ArchiveFile table from the dataframe and calls the PeakAnnotationsLoader for each file.

        Args:
            None
        Exceptions:
            Raises:
                AggregatedErrorsSet
            Buffers:
                None
        Returns:
            None
        """
        for _, row in self.df.iterrows():
            if self.is_skip_row():
                continue

            # Determine the format
            filename, filepath, format_code = self.get_file_and_format(row)

            # Load the ArchiveFile entry for the peak annotations file
            try:
                self.get_or_create_annot_file(filepath, format_code, filename=filename)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

            (
                operator,
                lc_protocol_name,
                instrument,
                date,
            ) = self.get_default_sequence_details(row)

            # Load the peak annotations
            self.load_peak_annotations(
                filepath,
                format_code,
                operator=operator,
                lc_protocol_name=lc_protocol_name,
                instrument=instrument,
                date=date,
                filename=filename,
            )

        # If any of the PeakAnnotationLoaders had any fatal error, raise an AggregatedErrorsSet exception.  This
        # loader's aggregated_errors_object will be incorporated in TableLoader's load wrapper, after summarizing and
        # processing its exceptions.
        for aes in self.aggregated_errors_dict.values():
            if aes.should_raise():
                raise AggregatedErrorsSet(self.aggregated_errors_dict)

    def get_file_and_format(self, row):
        """Gets the file path and determines the file format.

        Args:
            row (pd.Series)
        Exceptions:
            InfileError
        Returns:
            filename (str): The user's given name for the file (in case the path has a hashed temp name from a web form)
            filepath (str)
            format_code (str)
        """
        filepath_str = self.get_row_val(row, self.headers.FILE)
        format_code = self.get_row_val(row, self.headers.FORMAT)

        filename = os.path.basename(filepath_str)

        # Determine the actual filepath.  It can be obtained from self.annot_files_dict if this can from the web
        # interface, and it will be an ugly temp path.
        if (
            self.annot_files_dict is not None
            and filename in self.annot_files_dict.keys()
        ):
            # The paths in self.annot_files_dict are assumed to be absolute
            filepath = self.annot_files_dict[filename]
        else:
            # Check the path relative to the folder self.file is in
            study_file = self.file
            study_dir = None if self.file is None else os.path.dirname(study_file)

            if study_dir is not None:
                if os.path.isabs(filepath_str):
                    # In case the path is absolute
                    filepath = filepath_str
                elif os.path.isfile(os.path.join(study_dir, filepath_str)):
                    # In case the path is relative to the study doc
                    filepath = os.path.join(study_dir, filepath_str)
                elif os.path.isfile(os.path.join(study_dir, filename)):
                    # Check the joined the path of the study doc's directory with the supplied filename
                    filepath = os.path.join(study_dir, filename)
                elif os.path.isfile(filepath_str):
                    # In case the path is relative to the current directory
                    filepath = filepath_str
                else:
                    # Make the forthcoming error show the path relative to the study doc, which we should encourange
                    # users to use.
                    filepath = os.path.join(study_dir, filepath_str)
            else:
                # We will look relative to the current directory
                filepath = filepath_str

        if not os.path.isfile(filepath):
            self.buffer_infile_exception(
                FileFromInputNotFound(filepath_str, tmpfile=filepath),
                suggestion="Skipping load.",
                column=self.headers.FILE,
            )
            self.add_skip_row_index()
            return filename, None, format_code

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

        return filename, filepath, format_code

    @transaction.atomic
    def get_or_create_annot_file(self, filepath, format_code, filename=None):
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
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "is_binary": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "filename": filename,  # In case the file is a tmp file from a web form with a nonsense name
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

    def get_default_sequence_details(self, row):
        """Retrieves the Sequence Name and parses it into its parts.

        Args:
            row (pd.Series)
        Exceptions:
            None
        Returns:
            default_operator (Optional[str])
            default_lc_protocol_name (Optional[str])
            default_instrument (Optional[str])
            default_date (Optional[str])
        """
        sequence_name = self.get_row_val(row, self.headers.SEQNAME)

        (
            default_operator,
            default_lc_protocol_name,
            default_instrument,
            default_date,
        ) = MSRunSequence.parse_sequence_name(sequence_name)

        return (
            default_operator,
            default_lc_protocol_name,
            default_instrument,
            default_date,
        )

    def load_peak_annotations(
        self,
        filepath,
        format_code,
        operator=None,
        lc_protocol_name=None,
        instrument=None,
        date=None,
        filename=None,
    ):
        """Loads the peak annotations file reference in the row and supplies a default MS Run Sequence, if supplied.

        Args:
            filepath (str)
            format_code (str)
            operator (str): Default researcher
            date (str): Default date
            lc_protocol_name (str): Default LC protocol name
            instrument (str): Default instrument
        Exceptions:
            None
        Returns:
            None
        """
        if filename is None and filepath is not None:
            filename = os.path.basename(filepath)

        if filepath is not None and (
            format_code is None
            or format_code not in PeakAnnotationsLoader.get_supported_formats()
        ):
            self.buffer_infile_exception(
                (
                    f"Skipping load of peak annotations file '{filename}'.  Unrecognized format code: {format_code}.  "
                    f"Must be one of {PeakAnnotationsLoader.get_supported_formats()}."
                ),
                column=self.headers.FORMAT,
                is_error=False,
            )
            return

        if self.is_skip_row():
            return

        if filepath is None:
            self.buffer_infile_exception(
                "Peak annotations file is undefined.",
                column=self.headers.FILE,
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

        # TODO: Replace "Peak Annotation Details" with a reference to its loader's DataSheetName.
        # Cannot reference the MSRunsLoader here (to include the name of its sheet and its file column) due to circular
        # import.
        if self.peak_annotation_details_df is not None:
            peak_annotation_details_file = self.peak_annotation_details_file
            peak_annotation_details_sheet = self.peak_annotation_details_sheet
            peak_annotation_details_df = self.peak_annotation_details_df
        elif is_excel(self.file) and "Peak Annotation Details" in get_sheet_names(
            self.file
        ):
            peak_annotation_details_file = self.file
            peak_annotation_details_sheet = "Peak Annotation Details"
            peak_annotation_details_df = read_from_file(
                peak_annotation_details_file,
                peak_annotation_details_sheet,
                # TODO: Add dtypes argument here
            )

        # Get the peak group conflict resolutions
        peak_group_conflicts_file = None
        peak_group_conflicts_sheet = None
        peak_group_conflicts_df = None

        if self.peak_group_conflicts_df is not None:
            peak_group_conflicts_file = self.peak_group_conflicts_file
            peak_group_conflicts_sheet = self.peak_group_conflicts_sheet
            peak_group_conflicts_df = self.peak_group_conflicts_df
        elif is_excel(
            self.file
        ) and PeakGroupConflicts.DataSheetName in get_sheet_names(self.file):
            peak_group_conflicts_file = self.file
            peak_group_conflicts_sheet = PeakGroupConflicts.DataSheetName
            peak_group_conflicts_df = read_from_file(
                peak_group_conflicts_file,
                peak_group_conflicts_sheet,
            )

        # Create an instance of the specific peak annotations loader for this format
        peak_annot_loader = peak_annot_loader_class(
            # These are the essential arguments
            df=read_from_file(filepath, sheet=None),
            file=filepath,
            filename=filename,  # In case filepath is a temp file with a nonsense name
            # Then we need either these 3 peak annotation details inputs
            peak_annotation_details_file=peak_annotation_details_file,
            peak_annotation_details_sheet=peak_annotation_details_sheet,
            peak_annotation_details_df=peak_annotation_details_df,
            # Or... these default sequence inputs (as long as the sample headers = sample DB names)
            operator=operator,
            date=date,
            lc_protocol_name=lc_protocol_name,
            instrument=instrument,
            # Then we need the peak group conflict resolutions the researcher selected
            peak_group_conflicts_file=peak_group_conflicts_file,
            peak_group_conflicts_sheet=peak_group_conflicts_sheet,
            peak_group_conflicts_df=peak_group_conflicts_df,
            # Pass-alongs
            _validate=self.validate,
            defer_rollback=self.defer_rollback,
        )

        try:
            # Load this peak annotations file
            peak_annot_loader.load_data()
        except AggregatedErrors as aes:
            # Log the peak annot loader's exceptions by file
            self.aggregated_errors_dict[filename] = aes
        finally:
            # Just in case the raised AggregatedErrors exception above wasn't in the loader object, add the object,
            # because it can contain warnings that were not raised
            if filename not in self.aggregated_errors_dict.keys():
                self.aggregated_errors_dict[filename] = (
                    peak_annot_loader.aggregated_errors_object
                )
            self.update_load_stats(peak_annot_loader.get_load_stats())

    def get_dir_to_sequence_dict(self):
        """This traverses self.df to return a dict that maps the peak annotation file's directory path to a list of
        sequence names.

        This is intended to be used by the MSRunsLoader to associate an mzXML file with the sequence it came from by
        determining that the mzXML file's path contains the peak annotation file's path (because peak annotation files
        are required to be co-located with the mzXML files of a sequence).

        Args:
            None
        Exceptions:
            None
        Returns:
            dir_to_sequence_dict (Dict[str, List[str]]): E.g. {"/path/to/peakannot/dir": ["sequence name"]}
        """
        dir_to_sequence_dict = defaultdict(list)

        # Since load_data is not being called...
        self.check_dataframe()

        # Save the current row index
        save_row_index = self.row_index
        # Initialize the row index
        self.set_row_index(None)

        for _, row in self.df.iterrows():
            file = self.get_row_val(row, self.headers.FILE)
            seqname = self.get_row_val(row, self.headers.SEQNAME)
            if file is None or seqname is None:
                continue
            dir: str
            dir = os.path.dirname(file)
            if (
                dir not in dir_to_sequence_dict.keys()
                or seqname not in dir_to_sequence_dict[dir]
            ):
                dir_to_sequence_dict[dir].append(seqname)

        # Restore the original row index
        self.set_row_index(save_row_index)

        return dir_to_sequence_dict
