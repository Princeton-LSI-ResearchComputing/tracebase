import os
import re
from collections import defaultdict, namedtuple
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import xmltodict
from django.db import ProgrammingError, transaction
from django.db.models import Max, Min, Q

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.models import (
    ArchiveFile,
    DataFormat,
    DataType,
    MaintainedModel,
    MSRunSample,
    MSRunSequence,
    PeakGroup,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AmbiguousMzxmlSampleMatch,
    AssumedMzxmlSampleMatch,
    ConditionallyRequiredArgs,
    DefaultSequenceNotFound,
    FileFromInputNotFound,
    InfileError,
    InvalidMSRunName,
    MissingSamples,
    MixedPolarityErrors,
    MultipleDefaultSequencesFound,
    MultipleRecordsReturned,
    MutuallyExclusiveArgs,
    MzxmlColocatedWithMultipleAnnot,
    MzxmlNotColocatedWithAnnot,
    MzxmlParseError,
    MzxmlSampleHeaderMismatch,
    MzxmlSequenceUnknown,
    MzXMLSkipRowError,
    NoSamples,
    NoScans,
    PossibleDuplicateSample,
    RecordDoesNotExist,
    RequiredColumnValue,
    RequiredColumnValues,
    RollbackException,
    UnmatchedBlankMzXML,
    UnmatchedMzXML,
    UnskippedBlanks,
    summarize_int_list,
)
from DataRepo.utils.file_utils import is_excel, read_from_file, string_to_date


class MSRunsLoader(TableLoader):
    """Class to load the MSRunSample table."""

    # These are common labels repeatedly appended to peak annotation sample names to differentiate which mzXML file the
    # peaks came from.  Researchers tend to include these in order make the file names unique enough to collect them
    # together in a single directory.  They tend to do that by appending a string with a delimiting underscore (or dash)
    # that indicates polarities and/or scan ranges.  This is not perfect.  The full pattern into which these patterns
    # are included can be used to strip out these substrings even if they have been chained together (e.g. _pos_scan1).
    DEFAULT_SCAN_LABEL_PATTERNS = [
        r"pos",
        r"neg",
        r"scan[0-9]+",
    ]
    DEFAULT_SCAN_DELIM_PATTERN = r"[\-_]"
    SKIP_STRINGS = ["skip", "true", "t", "yes", "y"]

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    SAMPLENAME_KEY = "SAMPLENAME"
    SAMPLEHEADER_KEY = "SAMPLEHEADER"
    MZXMLNAME_KEY = "MZXMLNAME"
    ANNOTNAME_KEY = "ANNOTNAME"
    SEQNAME_KEY = "SEQNAME"
    SKIP_KEY = "SKIP"

    DataSheetName = "Peak Annotation Details"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "SAMPLENAME",
            "SAMPLEHEADER",
            "MZXMLNAME",
            "ANNOTNAME",
            "SEQNAME",
            "SKIP",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        SAMPLENAME="Sample Name",
        SAMPLEHEADER="Sample Data Header",
        MZXMLNAME="mzXML File Name",
        ANNOTNAME="Peak Annotation File Name",
        SEQNAME="Sequence",
        SKIP="Skip",
    )

    # List of required header keys
    DataRequiredHeaders = [
        [
            # Don't apply required columns (below) if the Skip column has any value
            SKIP_KEY,
            [
                SAMPLENAME_KEY,
                [
                    # Supply only the sample header from the peak annotation file (because mzXML is filled in
                    # automatically), but if there is no peak annotation file, but there are mzXML files from an
                    # unanalyzed run, supply the mzXML file name
                    SAMPLEHEADER_KEY,
                    MZXMLNAME_KEY,
                ],
            ],
        ],
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # DataDefaultValues not needed

    DataColumnTypes: Dict[str, type] = {
        SAMPLENAME_KEY: str,
        SAMPLEHEADER_KEY: str,
        MZXMLNAME_KEY: str,
        ANNOTNAME_KEY: str,
        SEQNAME_KEY: str,
        SKIP_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        # A header must be unique per annot file.  The pair cannot repeat in the file.  It either has an mzXML file
        # associated or not and it can have a sequence or not, and can only ever link to a single sample.
        # Multiple different annot files of the same name are not supported.
        [SAMPLEHEADER_KEY, ANNOTNAME_KEY],
        # Since the annotation file is optional (e.g. for unanalyzed mzxml files), and mzXML file names can be the same,
        # we need more than just the header or mzXML file name, we need the sequence.  If a user can't tell which
        # sequence to use, all we can do is add their path to differentiate them.
        # All combined must be unique, but note that duplicates of SAMPLENAME_KEY, (SAMPLEHEADER_KEY or MZXMLNAME_KEY),
        # and SEQNAME_KEY will be ignored. Duplicates can exist if the same mzXML was used in multiple peak annotation
        # files.
        [SAMPLENAME_KEY, SAMPLEHEADER_KEY, MZXMLNAME_KEY, ANNOTNAME_KEY, SEQNAME_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        MSRunSample.__name__: {
            "sample": SAMPLENAME_KEY,
            "msrun_sequence": SEQNAME_KEY,
        },
        ArchiveFile.__name__: {
            "filename": MZXMLNAME_KEY,
        },
    }

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        SAMPLENAME=TableColumn.init_flat(
            name=DataHeaders.SAMPLENAME,
            field=MSRunSample.sample,
            dynamic_choices=ColumnReference(
                loader_class=SamplesLoader,
                loader_header_key=SamplesLoader.SAMPLE_KEY,
            ),
        ),
        SAMPLEHEADER=TableColumn.init_flat(
            name=DataHeaders.SAMPLEHEADER,
            help_text=f"Sample header from {DataHeaders.ANNOTNAME}.",
            guidance=f"Note, this column is only conditionally required with '{DataHeaders.MZXMLNAME}'.",
        ),
        MZXMLNAME=TableColumn.init_flat(
            name=DataHeaders.MZXMLNAME,
            field=MSRunSample.ms_data_file,
            header_required=False,  # Assuming can be derived from SAMPLEHEADER
            value_required=False,  # There will be an error if multiple files have the same name
            guidance=(
                f"Note, you can load any/all {DataHeaders.MZXMLNAME}s for a {DataHeaders.SAMPLENAME} *before* the "
                f"{DataHeaders.ANNOTNAME} is ready to load, in which case you can just leave this value empty.\n"
                "\n"
                f"Note, this column is only conditionally required with '{DataHeaders.SAMPLEHEADER}'.  I.e. an "
                f"{DataHeaders.MZXMLNAME} can be loaded without a '{DataHeaders.ANNOTNAME}'."
            ),
        ),
        ANNOTNAME=TableColumn.init_flat(
            name=DataHeaders.ANNOTNAME,
            help_text=(
                "Name of the peak annotation file.  If the sample on any given row was included in a "
                f"{DataHeaders.ANNOTNAME}, add the name of that file here."
            ),
            # TODO: Replace "Peak Annotation Files" and "Peak Annotation File" below with a reference to its loader's
            # DataSheetName and the corresponding column, respectively.
            # Cannot reference the PeakAnnotationFilesLoader here (to include the name of its sheet and its file column)
            # due to circular import.
            dynamic_choices=ColumnReference(
                sheet="Peak Annotation Files",
                header="Peak Annotation File",
            ),
            # ANNOTNAME is actually required, but defaults are provided by arguments to the constructor
            header_required=False,
            value_required=False,
        ),
        SEQNAME=TableColumn.init_flat(
            name=DataHeaders.SEQNAME,
            help_text=(
                f"The Sequence associated with the {DataHeaders.SAMPLENAME}, {DataHeaders.SAMPLEHEADER}, and/or "
                f"{DataHeaders.MZXMLNAME} on this row."
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
            header_required=False,  # Assuming default set in the Peak Annotation Files sheet
            value_required=False,  # Assuming default set in the Peak Annotation Files sheet
        ),
        SKIP=TableColumn.init_flat(
            name=DataHeaders.SKIP,
            help_text="Whether to load data associated with this sample, e.g. a blank sample.",
            guidance=(
                f"Enter 'skip' to skip loading of the sample and peak annotation data.  The mzXML file will be saved "
                "if supplied, but it will not be associated with an MSRunSample or MSRunSequence, since the Sample "
                f"record will not be created.  Note that the {DataHeaders.SAMPLENAME}, {DataHeaders.SAMPLEHEADER}, and "
                f"{DataHeaders.SEQNAME} columns must still have a unique combo value (for file validation, even though "
                "they won't be used)."
            ),
            format="Boolean: 'skip' or ''.",
            default=False,
            header_required=False,
            value_required=False,
            static_choices=[
                # Treated as False (easier tor the user to see what is skipped at a glance)
                ("", ""),
                ("skip", "skip"),
            ],
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [MSRunSample, PeakGroup, ArchiveFile]

    # This error is used in 2 places, so it is saved here.
    seq_defaults_error = ConditionallyRequiredArgs(
        (
            "Enough of the following arguments to accurately and uniquely identify an MSRunSequence "
            "record are required:\n\t{0}"
        ).format(
            "\n\t".join(
                [
                    "operator_default",
                    "instrument_default",
                    "date_default",
                    "lc_protocol_name_default",
                ]
            )
        )
    )

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                debug (bool) [False]: Debug mode causes all buffered exception traces to be printed.  Normally, if an
                    exception is a subclass of SummarizableError, the printing of its trace is suppressed.
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
                mzxml_files (Optional[List[str]]): Paths to mzXML files.
                mzxml_dir (Optional[str]): Path to mzXML directory.  Used to find mzxml files when mzxml_files is not
                    supplied.  This is used to determine the sequence an mzXML comes from, inferred via sharing a
                    directory with a peak annot file that has a default sequence assigned in the Peak Annotation Files
                    sheet.  NOTE: If mzxml_files is empty or not set, this argument defaults to the directory that
                    self.file is in, or if that's None: the current working directory.
                skip_mzxmls (bool) [False]: Skips the loading of mzXML file records into the ArchiveFile table.
                    Mutually exclusive with mzxml_files and mzxml_dir, which will be ignored if this argument is True.
                    Note, this also skips the creation of raw file record (but also note that raw files are never
                    actually loaded - what is skipped is the creation of the record representing the raw file).
                operator (Optional[str]): The researcher who ran the mass spec.  Mutually exclusive with defaults_df
                    (when it has a default for the operator column for the Sequences sheet).
                lc_protocol_name (Optional[str]): Name of the liquid chromatography method.  Mutually exclusive with
                    defaults_df (when it has a default for the lc_protocol_name column for the Sequences sheet).
                instrument (Optional[str]): Name of the mass spec instrument.  Mutually exclusive with defaults_df
                    (when it has a default for the instrument column for the Sequences sheet).
                date (Optional[str]): Date the Mass spec instrument was run.  Format: YYYY-MM-DD.  Mutually exclusive
                    with defaults_df (when it has a default for the date column for the Sequences sheet).
                exact_mode (bool) [False]: When False, and dynamically mapping sample headers to mzXML file names,
                    equate dashes and underscores.  (Isocorr, and possibly other software, when creating sample headers,
                    replaces dashes from the mzXML filename with underscores.  So when the code tries to look for a file
                    name that matches the sample header, it will consider dashes and underscores as equal.  When True,
                    only sample headers that exactly match the file name will be considered matches.
        Exceptions:
            Buffered:
                InfileError when a paths to a peak annotation file is absolute
                MutuallyExclusiveArgs
                DefaultSequenceNotFound
                MultipleDefaultSequencesFound
            Raised:
                AggregatedErrors
        Returns:
            None
        """
        # NOTE: We COULD make a friendly version of mzxml_files for the web interface, but we don't accept this separate
        # file in that interface, so it will only ever effectively be used on the command line, where mzxml_files *are*
        # already friendly.
        self.mzxml_files = kwargs.pop("mzxml_files", [])
        self.mzxml_dir = kwargs.pop("mzxml_dir", None)
        operator_default = kwargs.pop("operator", None)
        date_default = kwargs.pop("date", None)
        lc_protocol_name_default = kwargs.pop("lc_protocol_name", None)
        instrument_default = kwargs.pop("instrument", None)
        exact_mode = kwargs.pop("exact_mode", False)
        skip_mzxmls = kwargs.pop("skip_mzxmls", False)

        super().__init__(*args, **kwargs)

        # Check mutually exclusive options
        if skip_mzxmls:
            if len(self.mzxml_files) > 0:
                self.aggregated_errors_object.buffer_warning(
                    MutuallyExclusiveArgs(
                        f"skip_mzxmls is True, but {len(self.mzxml_files)} mzxml_files were provided.  "
                        "Note that the load of these files will be skipped."
                    )
                )
            if self.mzxml_dir is not None:
                self.aggregated_errors_object.buffer_warning(
                    MutuallyExclusiveArgs(
                        "skip_mzxmls is True, but mzxml_dir was provided.  "
                        "Note that the load of the files it may contain will be skipped."
                    )
                )
            self.mzxml_dir = None
            self.mzxml_files = []

        # If no specific mzXML files were provided
        if not skip_mzxmls and len(self.mzxml_files) == 0:
            # If the infile was provided and no mzxml_dir was provided
            if self.file is not None and self.mzxml_dir is None:
                study_file = os.path.abspath(self.file)
                study_dir = os.path.dirname(study_file)
                self.mzxml_dir = study_dir
            elif self.mzxml_dir is None:
                # Fall back to the current working directory
                self.mzxml_dir = os.getcwd()
            # Look up all the mzXML files by walking to mzxml_dir
            self.mzxml_files = self.get_mzxml_files(dir=self.mzxml_dir)

        self.annotdir_to_seq_dict = None
        # If we have mzxml files to load, we need to retrieve the paths of the peak annotation files mapped to their
        # sequences so that we can associate those sequences with the mzXML files that are on the same path
        if (
            not skip_mzxmls
            and len(self.mzxml_files) > 0
            and self.file is not None
            and is_excel(self.file)
        ):
            # If no file is provided, we will not error, but note that if there are multiple mzXML files with the same
            # name, and no default sequence is provided, we will not be able to know what sequence an mzXML belongs to,
            # so we won't be able to create an MSRunSample record for those files.  (Note: mzXML files are co-located
            # with peak annotation files and their common paths are used to infer the sequence they're derived from.
            # using the default sequence column in the Sequences sheet that associates the peak annotation file and
            # sequence.)
            # TODO: Instead of reading the file here, I should take the PAFL dataframe, sheet name, and file as input
            # arguments, like I do in other classes.
            pafl = PeakAnnotationFilesLoader(
                df=read_from_file(
                    self.file, sheet=PeakAnnotationFilesLoader.DataSheetName
                ),
                file=self.file,
            )
            self.annotdir_to_seq_dict = pafl.get_dir_to_sequence_dict()
            self.aggregated_errors_object.merge_aggregated_errors_object(
                pafl.aggregated_errors_object
            )
            # Error-check the peak annotation file paths to ensure they are relative.  We require relative paths to be
            # able to compare with mzXML file paths
            abs_paths = []
            for annot_dir in self.annotdir_to_seq_dict.keys():
                if os.path.isabs(annot_dir):
                    abs_paths.append(annot_dir)
            if len(abs_paths) > 0:
                # This load takes a long time.  Let's not waste it and raise immediately.
                nlt = "\n\t"
                raise self.aggregated_errors_object.buffer_error(
                    InfileError(
                        (
                            "Paths to peak annotation files must be relative paths, but the following absolute paths "
                            f"were found in %s:\n{nlt.join(abs_paths)}"
                        ),
                        file=self.file,
                        sheet=PeakAnnotationFilesLoader.DataSheetName,
                    ),
                )
            # Since we have annotation file paths, we should make sure we have a directory for the mzXML files
            if self.mzxml_dir is None:
                if self.file is None:
                    # If we don't have a study file path (we only have the dataframe), all we can do is get the common
                    # path of the mzXML files, which could be a problem.
                    # TODO: An alternative could be to look at the paths of the peak annotation files.
                    self.mzxml_dir = os.path.commonpath(self.mzxml_files)
                else:
                    study_file = os.path.abspath(self.file)
                    study_dir = os.path.dirname(study_file)
                    all_files = [study_file]
                    all_files.extend(
                        [os.path.abspath(mzxf) for mzxf in self.mzxml_files]
                    )
                    common_dir = os.path.commonpath(all_files)
                    if study_dir in common_dir:
                        # The mzxml files are under the study directory
                        self.mzxml_dir = study_dir
                    else:
                        # The mzxml files are outside the study directory, so use the common directory of the mzXML
                        # files
                        self.mzxml_dir = os.path.commonpath(self.mzxml_files)

        # We are going to use defaults from the SequencesLoader if no dataframe (i.e. --infile) was provided
        seqloader = SequencesLoader(
            defaults_df=self.defaults_df,
            defaults_file=self.defaults_file,
        )
        seqdefaults = seqloader.get_user_defaults()
        # TODO: Figure out a better way to handle buffered exceptions from another class that are only raised from a
        # specific method, so that methods raise them as a group instead of needing to incorporate instance loaders like
        # this for buffered errors
        self.aggregated_errors_object.merge_aggregated_errors_object(
            seqloader.aggregated_errors_object
        )

        if seqdefaults is not None:
            mutex_arg_errs = []
            mutex_def_errs = []

            # get the operator_default from either the defaults sheet/file or from the arg
            self.operator_default = seqdefaults[seqloader.DataHeaders.OPERATOR]
            if (
                self.operator_default is not None
                and operator_default is not None
                and self.operator_default != operator_default
            ):
                mutex_arg_errs.append("operator")
                mutex_def_errs.append(seqloader.DataHeaders.OPERATOR)
            elif self.operator_default is None and operator_default is not None:
                self.operator_default = operator_default

            # get the date_default from either the defaults sheet/file or from the arg
            self.date_default = seqdefaults[seqloader.DataHeaders.DATE]
            if (
                self.date_default is not None
                and date_default is not None
                and self.date_default != date_default
            ):
                mutex_arg_errs.append("date")
                mutex_def_errs.append(seqloader.DataHeaders.DATE)
            elif self.date_default is None and date_default is not None:
                self.date_default = date_default

            # get the lc_protocol_name_default from either the defaults sheet/file or from the arg
            self.lc_protocol_name_default = seqdefaults[seqloader.DataHeaders.LCNAME]
            if (
                self.lc_protocol_name_default is not None
                and lc_protocol_name_default is not None
                and self.lc_protocol_name_default != lc_protocol_name_default
            ):
                mutex_arg_errs.append("lc_protocol_name")
                mutex_def_errs.append(seqloader.DataHeaders.LCNAME)
            elif (
                self.lc_protocol_name_default is None
                and lc_protocol_name_default is not None
            ):
                self.lc_protocol_name_default = lc_protocol_name_default

            # get the instrument_default from either the defaults sheet/file or from the arg
            self.instrument_default = seqdefaults[seqloader.DataHeaders.INSTRUMENT]
            if (
                self.instrument_default is not None
                and instrument_default is not None
                and self.instrument_default != instrument_default
            ):
                mutex_arg_errs.append("instrument")
                mutex_def_errs.append(seqloader.DataHeaders.INSTRUMENT)
            elif self.instrument_default is None and instrument_default is not None:
                self.instrument_default = instrument_default

            if len(mutex_arg_errs) > 0:
                raise self.aggregated_errors_object.buffer_error(
                    MutuallyExclusiveArgs(
                        (
                            f"Multiple conflicting defaults defined via both arguments {mutex_arg_errs} and via the "
                            f"{seqloader.DataSheetName} defaults sheet/file {mutex_def_errs}: %s.  Please use either "
                            f"the {seqloader.DataSheetName} defaults sheet/file or the command line arguments (not "
                            "both)."
                        ),
                        file=(
                            self.defaults_file
                            if self.defaults_file is not None
                            else self.friendly_file
                        ),
                        sheet=self.sheet,
                        column=seqloader.DefaultsHeaders.DEFAULT_VALUE,
                    )
                )
        else:
            self.operator_default = operator_default
            self.date_default = date_default
            self.lc_protocol_name_default = lc_protocol_name_default
            self.instrument_default = instrument_default

        self.msrun_sequence_dict = {}
        # Save the default MSRunSequence record (if any) in the self.msrun_sequence_dict:
        default_sequence = self.get_msrun_sequence()
        if default_sequence is None and (
            operator_default is not None
            or date_default is not None
            or lc_protocol_name_default is not None
            or instrument_default is not None
        ):
            # Remove the error from the buffer and replace it with a more specific error (because this may not have been
            # from an infile and without the context of the query originating from the arguments in the constructor for
            # the "default" sequence, it is confusing)
            if self.aggregated_errors_object.exception_type_exists(RecordDoesNotExist):
                rdne = list(
                    self.aggregated_errors_object.remove_exception_type(
                        RecordDoesNotExist
                    )
                )[0]
                self.aggregated_errors_object.buffer_error(
                    DefaultSequenceNotFound(
                        operator_default,
                        date_default,
                        lc_protocol_name_default,
                        instrument_default,
                    ),
                    orig_exception=rdne,
                )
            if self.aggregated_errors_object.exception_type_exists(
                MultipleRecordsReturned
            ):
                mrr = list(
                    self.aggregated_errors_object.remove_exception_type(
                        MultipleRecordsReturned
                    )
                )[0]
                self.aggregated_errors_object.buffer_error(
                    MultipleDefaultSequencesFound(
                        operator_default,
                        date_default,
                        lc_protocol_name_default,
                        instrument_default,
                    ),
                    orig_exception=mrr,
                )
        elif default_sequence is None and self.df is not None:
            self.check_seqname_column()

        # This will contain the created ArchiveFile records for mzXML files
        self.created_mzxml_archive_file_recs = []

        # If this is False, when building the mzxml_dict below, the key will have dashes replaced with underscores and
        # lookups using sample headers or sample names will also have dashes replaced with underscores.
        self.exact_mode = exact_mode

        # This will contain metadata parsed from the mzXML files (and the created ArchiveFile records to be added to
        # MSRunSample records).  It's first key is the sample header (not necessarily the mzXML basename) and the second
        # key is the relative directory path from the study directory (which can be can be '', to mean *any* directory
        # under which a file of this name can be found).  This is explicitly to allow entry of the file names (which is
        # very labor intensive) to be optional.
        self.mzxml_dict_by_header = defaultdict(lambda: defaultdict(list))

        # This will contain the sample header mapped to the sample name in the database.
        self.header_to_sample_name = defaultdict(lambda: defaultdict(list))
        # This will contain the mzXML basename mapped to the sample name in the database.  It will only be used to map
        # multiple leftover mzXML files with the same name to a sample (when they could not be mapped to a specific row,
        # but all map to the same sample).  Filling in the mzXML File Name column is optional.  When it is empty, the
        # Sample Data Header is used, as it is assumed to be the same.  The mzXML File Name should be filled in when it
        # differs.
        self.mzxml_to_sample_name = defaultdict(lambda: defaultdict(list))

        # This will prevent creation of MSRunSample records for mzXMLs associated with (e.g.) blanks when leftover
        # mzXMLs are handled (a leftover being an mzXML unassociated with an MSRunSample record).
        self.skip_msrunsample_by_mzxml = defaultdict(lambda: defaultdict(list))

    # There are maintained fields in the models involved, so deferring autoupdates will make this faster
    @MaintainedModel.defer_autoupdates(
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def load_data(self):
        """Loads the MSRunSample table from the dataframe.

        Args:
            None
        Exceptions:
            Buffers:
                ConditionallyRequiredArgs
                MzXMLSkipRowError
            Raises:
                None
        Returns:
            None
        """
        # Catch issues with files versus the Peak Annotation Details sheet early, before spending tons of time reading
        # the files and loading them.
        self.check_mzxml_files()

        # Both PeakGroup and MSRunSample models are associated with cache updates.  Not only does it slow the running
        # time, but it currently produces a lot of console output, so disable caching updates for the duration of this
        # load, then clear the cache.
        # TODO: Remove this after implementing issue #1387
        disable_caching_updates()

        # 1. Traverse the supplied mzXML files
        #    - create ArchiveFile records.
        #    - Extract data from the mzxML files
        #    - store extracted metadata and ArchiveFile record objects in self.mzxml_dict_by_header, a 4D dict:
        #      {mzXML_name: {mzXML_dir: [{**metadata},...]}}
        # We need the directory to match the mzXML in the infile with the MSRunSequence name on the same row.  mzXML
        # files can easily have the same name and all users can reasonably be expected to know their location and the
        # sequence they were a part of.  Normally, all that's needed is a filename, but if that filename is not unique,
        # and there are multiple sequences in the file, we need a way to distinguish them, and the path is that way.
        for mzxml_file in self.mzxml_files:
            try:
                self.get_or_create_mzxml_and_raw_archive_files(mzxml_file)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass
        print(f"AAA self.mzxml_dict_by_header: {self.mzxml_dict_by_header}")

        # 2. Traverse the infile
        #    - create MSRunSample records
        #    - associate them with the mzXML ArchiveFile records and Sample and MSRunSequence records, (keeping track of
        #      which mzXMLs have been added to MSRunSample records)
        if self.df is not None:
            for _, row in self.df.iterrows():
                if self.is_row_empty(row):
                    continue
                try:
                    self.get_or_create_msrun_sample_from_row(row)
                except RollbackException:
                    # Exception handling was handled
                    # Continue processing rows to find more errors
                    pass
        self.check_sample_headers()

        # 3. Traverse leftover mzXML/ArchiveFile records unassociated with those processed in step 2, using:
        #    - The name of the mzXML automatically mapped to a sample name
        #    - The default researcher, date, lc-protocol-name, instrument supplied (if any were not supplied, error).
        # We don't want to complain about no sequence defaults being defined if the the sheet had everything, so let's
        # see if any leftover mzxml files actually exist first.
        if self.unpaired_mzxml_files_exist():

            print(
                (
                    "\nProcessing mzXML files either not in or could not be paired 1:1 with rows in the "
                    f"'{self.DataSheetName}' sheet."
                ),
                flush=True,
            )

            # Get the sequence defined by the defaults (for researcher, protocol, instrument, and date)
            default_msrun_sequence = self.get_msrun_sequence()
            # If there's no default sequence and no way to associate an mzXML with a sequence (because
            # annotdir_to_seq_dict is None) and not all default sequence data was provided (because it's only required
            # if we couldn't associate these files with rows in the infile)
            if (
                default_msrun_sequence is None
                and (
                    self.annotdir_to_seq_dict is None
                    or len(self.annotdir_to_seq_dict.keys()) == 0
                )
                and (
                    self.operator_default is None
                    or self.instrument_default is None
                    or self.date_default is None
                    or self.lc_protocol_name_default is None
                )
                and not self.aggregated_errors_object.exception_type_exists(
                    ConditionallyRequiredArgs
                )
            ):
                self.aggregated_errors_object.buffer_error(
                    self.seq_defaults_error.set_formatted_message(
                        suggestion=(
                            "This is necessary because there is at least 1 mzXML file that could not be paired with a "
                            f"row in the '{self.DataSheetName}' sheet, thus cannot be loaded with an association to a "
                            "sequence unless enough defaults described above are provided to uniquely identify a "
                            "sequence.  Alternatively, you can supply a default sequence in the "
                            f"'{SequencesLoader.DataHeaders.SEQNAME}' column of the '{SequencesLoader.DataSheetName}' "
                            "sheet."
                        ),
                    )
                )
                # Otherwise, we errored about it not being found already

            for mzxml_name_no_ext in self.mzxml_dict_by_header.keys():

                # We will skip creating MSRunSample records for rows marked with 'skip' (e.g. blanks), because to have
                # an MSRunSample record, you need a Sample record, and we don't create those for blank samples.
                if mzxml_name_no_ext in self.skip_msrunsample_by_mzxml.keys():
                    # There can exist rows in the sheet with the sample mzxml name that are not marked with 'skip', so
                    # we check each dir to see if it is present among the skipped files
                    dirs = [
                        dir
                        for dir in self.mzxml_dict_by_header[mzxml_name_no_ext].keys()
                        if dir
                        not in self.skip_msrunsample_by_mzxml[mzxml_name_no_ext].keys()
                    ]
                else:
                    dirs = list(self.mzxml_dict_by_header[mzxml_name_no_ext].keys())

                if mzxml_name_no_ext in self.skip_msrunsample_by_mzxml.keys():
                    if len(dirs) == 0:
                        # The sample (header) / mzXML file has been explicitly skipped by having added the directory
                        # path to the mzXML file name column of the infile
                        continue
                    if (
                        # There is a single skipped directory entry indicated in the infile
                        len(self.skip_msrunsample_by_mzxml[mzxml_name_no_ext].keys())
                        == 1
                        # The directory path is an empty string (meaning it's either in the root directory or an mzXML
                        # filename was not provided in the infile)
                        and ""
                        in self.skip_msrunsample_by_mzxml[mzxml_name_no_ext].keys()
                        # The number of rows in the infile indicating that this 'sample header' should be skipped (i.e.
                        # the length of the list of rownums in self.skip_msrunsample_by_mzxml[mzxml_name][""]) is equal
                        # to the number of files with this name (inferred by the number of directory paths)
                        and len(dirs)
                        == len(self.skip_msrunsample_by_mzxml[mzxml_name_no_ext][""])
                    ):
                        # We will skip the files that matches the number of infile rows where the sample header was
                        # marked as a skip row even though we haven't confirmed any explicit directory matches.
                        continue
                    elif "" in self.skip_msrunsample_by_mzxml[mzxml_name_no_ext].keys():
                        skip_files = []
                        for dr in self.mzxml_dict_by_header[mzxml_name_no_ext].keys():
                            for dct in self.mzxml_dict_by_header[mzxml_name_no_ext][dr]:
                                skip_files.append(
                                    os.path.join(dr, dct["mzxml_filename"])
                                )
                        self.aggregated_errors_object.buffer_warning(
                            MzXMLSkipRowError(
                                mzxml_name_no_ext,
                                skip_files,
                                self.skip_msrunsample_by_mzxml[mzxml_name_no_ext],
                                file=self.friendly_file,
                                sheet=self.DataSheetName,
                                column=self.DataHeaders.MZXMLNAME,
                                rownum=(
                                    None
                                    if ""
                                    not in self.skip_msrunsample_by_mzxml[
                                        mzxml_name_no_ext
                                    ].keys()
                                    else ", ".join(
                                        [
                                            str(n)
                                            for n in sorted(
                                                self.skip_msrunsample_by_mzxml[
                                                    mzxml_name_no_ext
                                                ][""]
                                            )
                                        ]
                                    )
                                ),
                                suggestion=(
                                    "The mzXML files listed above will not be loaded.  If all mzXML files named "
                                    f"'{mzxml_name_no_ext}' must be skipped, please ensure that the number of rows in "
                                    f"the '{self.DataSheetName}' sheet equals the number of files above "
                                    f"({len(skip_files)}), or if not all should be skipped, the directory paths should "
                                    "be included for each mzXML file in the mzXML file name column so that we can tell "
                                    "which ones to load and which to skip)."
                                ),
                            ),
                            is_fatal=self.validate,
                        )
                        continue

                # Guess the sample based on the mzXML file's basename
                # mzxml_name_no_ext may or may not have had dashes converted to underscores based on exact_mode
                exact_sample_header_from_mzxml = mzxml_name_no_ext

                # We want the exact filename (without the extension), regardless of exact_mode, to supply to
                # guess_sample_name
                if not self.exact_mode:
                    # All of the file names should be the same, so we're going to arbitrarily grab the first one
                    arbitrary_key = next(
                        iter(self.mzxml_dict_by_header[mzxml_name_no_ext])
                    )
                    mzxml_filename = self.mzxml_dict_by_header[mzxml_name_no_ext][
                        arbitrary_key
                    ][0]["mzxml_filename"]
                    exact_sample_header_from_mzxml = (os.path.splitext(mzxml_filename))[
                        0
                    ]

                sample_name = None

                # If the mzXML name was recorded in the infile as mapping to multiple different peak annot file headers
                # and it only maps to 1 sample name (i.e. there are multiple files with the same filename/annot-header,
                # but they are all for the same sample)
                if (
                    mzxml_name_no_ext in self.mzxml_to_sample_name.keys()
                    and len(self.mzxml_to_sample_name[mzxml_name_no_ext].keys()) == 1
                ):
                    # Get the sample name from the infile/sheet
                    sample_name = next(
                        iter(self.mzxml_to_sample_name[mzxml_name_no_ext])
                    )
                elif (
                    mzxml_name_no_ext in self.mzxml_to_sample_name.keys()
                    and len(self.mzxml_to_sample_name[mzxml_name_no_ext].keys()) > 1
                ):
                    # There may be multiple mzXMLs with the same name that map to different samples.  We need to
                    # determine if they have been explicitly mapped to different samples in the peak annotation details
                    # sheet.

                    # The self.mzxml_dict_by_header has the sample name recorded in it.  Let's filter out the ones that
                    # have already been processed and grab the sample names that are left over, in a potentially non-
                    # unique list.
                    leftover_samples = [
                        mz_file_dict["sample_name"]
                        for dir_key in self.mzxml_dict_by_header[
                            mzxml_name_no_ext
                        ].keys()
                        for mz_file_dict in self.mzxml_dict_by_header[
                            mzxml_name_no_ext
                        ][dir_key]
                        if not mz_file_dict["added"]
                    ]

                    # BUG: NOTE: This problem was broken up into 2 PRs.  This block does not work in this PR.  It is
                    # BUG: fixed in the next PR.  See the BUG description in the comment in get_matching_mzxml_data
                    potentially_matching_samples = [
                        mz_file_dict["sample_name"]
                        for dir_key in self.mzxml_dict_by_header[
                            mzxml_name_no_ext
                        ].keys()
                        for mz_file_dict in self.mzxml_dict_by_header[
                            mzxml_name_no_ext
                        ][dir_key]
                        if mz_file_dict["added"]
                    ]

                    # Now let's make that list of sample names unique
                    infer_match = False
                    unique_leftover_samples = []
                    for smplnm in leftover_samples:
                        if smplnm is None:
                            infer_match = True
                        if smplnm is not None and smplnm not in unique_leftover_samples:
                            unique_leftover_samples.append(smplnm)

                    # BUG: NOTE: This problem was broken up into 2 PRs.  This block does not work in this PR.  It is
                    # BUG: fixed in the next PR.  See the BUG description in the comment in get_matching_mzxml_data
                    # If any of the leftovers could not assign a sample name, assume that other files by the same name
                    # that WERE added, because they were included in the peak annotation details sheet, are the matching
                    # samples, and add them as an ambiguous match.
                    if infer_match and len(potentially_matching_samples) > 0:
                        for potential_match in potentially_matching_samples:
                            if potential_match not in unique_leftover_samples:
                                unique_leftover_samples.append(potential_match)

                    if len(unique_leftover_samples) == 1:
                        # We have narrowed it down to 1 sample by process of elimination
                        sample_name = unique_leftover_samples[0]
                    elif len(unique_leftover_samples) == 0:
                        # All have already been loaded, so we can move on
                        continue
                    else:
                        # BUG: NOTE: This problem was broken up into 2 PRs.  This block does is not executed in this PR.
                        # BUG: It is fixed in the next PR.  See the BUG description in the comment in
                        # BUG: get_matching_mzxml_data
                        # BUG: Once it's fixed, this test needs to be updated: DataRepo.tests.loaders.test_msruns_loader
                        # BUG: .MSRunsLoaderTests.test_load_data_ambiguous_match
                        # We have an ambiguous mzXML sample mapping, so buffer an error
                        self.buffer_infile_exception(
                            AmbiguousMzxmlSampleMatch(
                                unique_leftover_samples,
                                mzxml_name_no_ext,
                                infer_match,
                            ),
                            is_error=True,
                        )
                else:
                    # We going to guess the sample name based on the mzXML filename (without the extension)
                    sample_name = self.guess_sample_name(exact_sample_header_from_mzxml)

                # We need all of the paths of each mzXML file with the same name.  All the files with the same name used
                # in an error when the sample was not found, to indicate that this isn't coming from the peak annotation
                # details sheet and that each file must be added to that sheet with a 'skip' value so that it does not
                # attempt to create an 'MSRunSample' record, for which a corresponding sample is required.
                mzxml_filepaths = [
                    fldct["mzxml_filepath"]
                    for pathkey in self.mzxml_dict_by_header[mzxml_name_no_ext].keys()
                    for fldct in self.mzxml_dict_by_header[mzxml_name_no_ext][pathkey]
                ]
                sample = None
                if sample_name is not None:
                    sample = self.get_sample_by_name(
                        sample_name, from_mzxmls=mzxml_filepaths
                    )

                # NOTE: The directory content of self.mzxml_dict_by_header is based on the actual supplied mzXML files,
                # not on the content of the mzxml filename column in the infile.
                for mzxml_dir in dirs:
                    mzxml_metadata: dict
                    for mzxml_metadata in self.mzxml_dict_by_header[mzxml_name_no_ext][
                        mzxml_dir
                    ]:
                        # It's possible that sample is None here.  If it is, we will try to get the sample using the
                        # sample_name saved in the mzXML dict.
                        tmp_sample = sample
                        if (
                            sample_name is None
                            and "sample_name" in mzxml_metadata.keys()
                            and mzxml_metadata["sample_name"] is not None
                        ):
                            tmp_sample = self.get_sample_by_name(
                                mzxml_metadata["sample_name"],
                                from_mzxmls=mzxml_filepaths,
                            )

                        try:
                            self.get_or_create_msrun_sample_from_mzxml(
                                tmp_sample,
                                mzxml_name_no_ext,
                                mzxml_dir,
                                mzxml_metadata,
                                default_msrun_sequence,
                            )
                        except RollbackException:
                            # Exception handling was handled
                            # Continue processing rows to find more errors
                            pass

        self.report_discrepant_headers()

        # If there were any exceptions (i.e. a rollback of everything will be triggered from the wrapper)
        if self.aggregated_errors_object.should_raise():
            self.clean_up_created_mzxmls_in_archive()

        # This assumes that if rollback is deferred, that the caller has disabled caching updates and that they should
        # remain disabled so that the caller can enable them when it is done.
        # TODO: Remove this after implementing issue #1387
        if not self.defer_rollback:
            enable_caching_updates()
            if not self.dry_run and not self.validate:
                delete_all_caches()

    def check_seqname_column(self):
        """This method checks the sequence name column.  If any values are missing (and not skipped) and there is no
        default sequence defined, it stops execution, but only when there are mzXML files (because that's the
        bottleneck).

        Args:
            None
        Exceptions:
            Buffers:
                ConditionallyRequiredArgs: Saved in self.SeqDefaultsRequiredErr
            Raises:
                AggregatedErrors
        Returns:
            None
        """

        # NOTE: Required headers are handled upon load via the load_data wrapper (self._loader), but this method is
        # called from the constructor for an early check before doing the time-consuming task of loading mzXML files.
        # Its purpose solely relates to the dataframe.  If there is no sequence column, there is nothing to check.  The
        # user can load mzXML files without any metadata.  Missing sequences for those unpaired files are handled using
        # default sequence data supplied on the command line.
        if self.DataHeaders.SEQNAME not in self.df.columns:
            return

        # Find any unskipped sequence names that have no value
        missing_seqnames = []
        missing_seqnames_warnings = []
        suggestion = (
            f"Filling in a valid '{self.DataHeaders.ANNOTNAME}' will associate a sample with the default sequence "
            f"assigned via the '{self.DataHeaders.ANNOTNAME}', or, if this sequence differs from the default, enter a "
            f"'{self.DataHeaders.SEQNAME}'."
        )
        for _, row in self.df.iterrows():

            if self.is_row_empty(row):
                continue

            # Extract the skip and sequence name data from the infile using pandas' methodology instead of get_row_val
            # to avoid incrementing the row_index/rownum.

            # Determine if the row is skipped
            if (
                self.DataHeaders.SKIP not in self.df.columns
                or str(row[self.DataHeaders.SKIP]).strip() in self.none_vals
            ):
                skip = False
            else:
                skip = (
                    str(row[self.DataHeaders.SKIP]).strip().lower() in self.SKIP_STRINGS
                )

            # If not skipped, buffer a conditionally required argument exception if there is not sequence name value
            # and raise so that a user running the load doesn't have to wait a long time before realizing they
            # didn't provide a sequence default.
            if skip is False:
                seq = row[self.DataHeaders.SEQNAME]
                if str(seq).strip() in self.none_vals:
                    missing_seqnames.append(row.name + 2)
                    if self.validate:
                        # We're not buffering using self.aggregated_errors_object.buffer_warning because we want to
                        # create our own summary exception for this specific case in order to suggest how to fix the
                        # issue (because the column values in the sequence column are not strictly required - they can
                        # be handled by the curator).
                        missing_seqnames_warnings.append(
                            RequiredColumnValue(
                                self.DataHeaders.SEQNAME,
                                rownum=row.name + 2,
                                sheet=self.DataSheetName,
                                file=self.friendly_file,
                                suggestion=suggestion,
                            )
                        )
                        # We are custom-handling these exceptions as warnings because the Sequence column is not
                        # technically required, but we want to suggest that adding a peak annotation file name will
                        # assign a default sequence.
                        missing_seqnames_warnings[-1].is_error = False
                        missing_seqnames_warnings[-1].is_fatal = True

        if len(missing_seqnames) > 0:
            if not self.validate:
                self.aggregated_errors_object.buffer_error(
                    self.seq_defaults_error.set_formatted_message(
                        sheet=self.DataSheetName,
                        file=self.friendly_file,
                        column=self.DataHeaders.SEQNAME,
                        suggestion=(
                            f"This is necessary because there exists {len(missing_seqnames)} rows "
                            f"{summarize_int_list(missing_seqnames)} that do not have a value in the "
                            f"'{self.DataHeaders.SEQNAME}' column.  You can either enter a "
                            f"'{self.DataHeaders.SEQNAME}' in the '{self.DataSheetName}' sheet on the affected rows, "
                            f"enter a '{self.DataHeaders.ANNOTNAME}' in the '{self.DataSheetName}' sheet that matches "
                            f"a '{PeakAnnotationFilesLoader.DataHeaders.FILE}' in the "
                            f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet that has a "
                            f"'{PeakAnnotationFilesLoader.DataHeaders.SEQNAME}' assigned, "
                            "or provide the default arguments described above to use for all rows."
                        ),
                    ),
                )
            else:
                self.aggregated_errors_object.buffer_warning(
                    RequiredColumnValues(
                        missing_seqnames_warnings,
                        suggestion=suggestion,
                    ),
                    is_fatal=self.validate,
                )

            # If there are mzXML files, we want to exit early so the user can correct the problem now instead of after a
            # complete load attempt
            if len(self.mzxml_files) > 0:
                raise self.aggregated_errors_object

    def check_mzxml_files(self):
        """Reviews all of the mzXML files against the Peak Annotation Details sheet (if provided).  If any mzXML files
        are totally unexpected, self.aggregated_errors_object is raised.

        Limitations:
            1. This method only looks for mzXML files that appear to reference unloaded sample records.  It does not
            check that there exists precisely 1 row for each mzXML file in the Peak Annotation Details sheet.
        Assumptions:
            1. The directory paths supplied for mzXML files in the Peak Annotation Details sheets are relative to
            self.mzxml_dir.
        Args:
            None
        Exceptions:
            Buffers:
                None
            Raises:
                AggregatedErrors
        Returns:
            None
        """
        if self.df is None or self.mzxml_files is None or len(self.mzxml_files) == 0:
            return

        expected_mzxmls = defaultdict(lambda: defaultdict(dict))
        expected_samples = []
        unexpected_sample_headers = defaultdict(list)
        explicitly_skipped_rel_mzxmls = []

        # Take an accounting of all expected samples and mzXML files.  Note that in the absence of an explicitly entered
        # mzXML file, the sample header is used as a stand-in for the mzXML file's name (minus extension).
        for _, row in self.df.iterrows():
            if self.is_row_empty(row):
                continue
            sample_name = self.get_row_val(row, self.headers.SAMPLENAME)
            sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
            mzxml_name_with_opt_path = self.get_row_val(row, self.headers.MZXMLNAME)
            skip_str = self.get_row_val(row, self.headers.SKIP)
            skip = (
                True
                if skip_str is not None and skip_str.lower() in self.SKIP_STRINGS
                else False
            )

            if mzxml_name_with_opt_path is not None and skip:
                explicitly_skipped_rel_mzxmls.append(
                    os.path.normpath(
                        os.path.relpath(mzxml_name_with_opt_path, self.mzxml_dir)
                    )
                )
                continue

            # Keep track of samples that have been accounted for
            if sample_name not in expected_samples:
                expected_samples.append(sample_name)

            # If an mzXML file has not been explicitly specified, set:
            # - dr (str): a directory that is either a path relative to the study directory or an empty string
            # - sh (str): an assumed sample header whose dashes will be replaced with underscores (unless exact_mode is
            #   True).  It is assumed that sample headers will never have a dash (unless exact_mode is True).
            if mzxml_name_with_opt_path is None:
                # We don't have a directory, so use empty string
                dr = ""
                # The mzXML file should match the recorded sample header
                sh = sample_header
            else:
                # We assume that the optionally provided directory is relative to the (specified/deduced) mzXML dir
                dr = os.path.dirname(mzxml_name_with_opt_path)
                fn = os.path.basename(mzxml_name_with_opt_path)
                sh = os.path.splitext(fn)[0]

            modded_sh = sh
            if mzxml_name_with_opt_path is not None and not self.exact_mode:
                modded_sh = sh.replace("-", "_")

            # If we haven't seen an mzXML by this name before or we haven't see its directory before
            if (
                modded_sh not in expected_mzxmls.keys()
                or dr not in expected_mzxmls[sh].keys()
            ):
                expected_mzxmls[modded_sh][dr] = {
                    "sample_header": sample_header if sample_header is not None else sh,
                    "sample_name": sample_name,
                }

        # Now go through the actual supplied mzXML files and see if any are totally unaccounted for.
        for actual_mzxml_file in self.mzxml_files:
            # If this mzxml_file is in the explicitly_skipped_rel_mzxmls, don't check it.
            # NOTE: The file does not have to exist.  The supplied paths just have to be the same.  I.e. don't cause a
            # file system error based on a row that is skipped.
            actual_rel_file = os.path.normpath(
                os.path.relpath(actual_mzxml_file, self.mzxml_dir)
            )
            if any(
                actual_rel_file == skipped_rel_file
                for skipped_rel_file in explicitly_skipped_rel_mzxmls
            ):
                continue

            dr = os.path.dirname(actual_mzxml_file)
            fn = os.path.basename(actual_mzxml_file)
            sh = str(os.path.splitext(fn)[0])

            modded_sh = sh
            if not self.exact_mode:
                modded_sh = sh.replace("-", "_")

            sn = self.guess_sample_name(modded_sh)

            if modded_sh in expected_mzxmls.keys():
                actual_rel_dir = os.path.relpath(dr, self.mzxml_dir)
                if (
                    actual_rel_dir not in expected_mzxmls[modded_sh].keys()
                    and "" not in expected_mzxmls[modded_sh].keys()
                ):
                    # If the sample derived from the mzXML file name was not expected (i.e. no sample record exists)
                    if sn not in expected_samples:
                        # NOTE: We want an error for each file with the same name, so that the user can add the complete
                        # file list to the Peak Annotation Details sheet in order to skip them.  If we only reported one
                        # error for each sample, this error would pop up in each load attempt until all same-named files
                        # were accounted for.
                        unexpected_sample_headers[modded_sh].append(actual_rel_file)
            else:
                # If the sample derived from the mzXML file name was not expected (i.e. no sample record exists)
                if sn not in expected_samples:
                    # NOTE: We want an error for each file with the same name.  See above.
                    unexpected_sample_headers[modded_sh].append(actual_rel_file)

        unmapped_samples = []
        for unexpected_sample_header in unexpected_sample_headers.keys():
            guessed_name = self.guess_sample_name(unexpected_sample_header)
            rec = self.get_sample_by_name(
                guessed_name,
                from_mzxmls=unexpected_sample_headers[unexpected_sample_header],
            )
            # We're going to ignore unmapped samples that appear to be blanks.  Warnings would have already been
            # buffered about these.
            if rec is None and not Sample.is_a_blank(unexpected_sample_header):
                unmapped_samples.append(unexpected_sample_header)

        if len(unmapped_samples) > 0:
            if not self.aggregated_errors_object.should_raise():
                self.aggregated_errors_object.buffer_error(
                    ProgrammingError(
                        "Unexpected failure.  There were no errors, but samples matching the following were not "
                        f"found: {unmapped_samples}."
                    )
                )
            # Give up looking for more errors and exit early, because loading mzXML files is too expensive.
            raise self.aggregated_errors_object

        print("No totally unexpected mzXML files found.", flush=True)

        self.set_row_index(0)

    def check_sample_headers(self):
        """This checks that all identical sample headers all map to the same sample name (database record)."""
        for sample_header in self.header_to_sample_name.keys():
            if len(self.header_to_sample_name[sample_header].keys()) > 1:
                rows = []
                sample_names = []
                for sn in self.header_to_sample_name[sample_header].keys():
                    sample_names.append(sn)
                    rows.extend(self.header_to_sample_name[sample_header][sn])
                self.aggregated_errors_object.buffer_exception(
                    PossibleDuplicateSample(
                        sample_header,
                        sample_names,
                        file=self.friendly_file,
                        sheet=self.sheet,
                        column=f"{self.headers.SAMPLENAME} and {self.headers.SAMPLEHEADER}",
                        rownum=rows,
                    ),
                    is_error=False,
                    is_fatal=self.validate,
                )

    def report_discrepant_headers(self):
        """This removes RecordDoesNotExist exceptions (from the aggregated errors) about missing Sample records in the
        peak annotation details sheet and replaces them.  Among those not found in the peak annotation details, it
        breaks them up into a MissingSamples (or NoSamples) and an UnskippedBlanks.

        If a sample record was missing, but the sample name looks like a "blank" sample (according to
        Sample.is_a_blank), the error is converted to a warning and the MSRunSample record is just skipped.

        Args:
            None
        Exceptions:
            Buffers:
                UnskippedBlanks
                MissingSamples
                NoSamples
            Raises:
                None
        Returns:
            None
        """
        # Extract exceptions about missing Sample records
        sample_dnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Sample, is_error=True
        )

        # Separate the exceptions based on whether they appear to be blanks or not
        possible_blank_dnes = []
        likely_missing_dnes = []
        likely_missing_sample_names = []
        for sdne in sample_dnes:
            sample_name = sdne.query_obj["name"]
            if Sample.is_a_blank(sample_name):
                possible_blank_dnes.append(sdne)
            else:
                likely_missing_dnes.append(sdne)
                if sample_name not in likely_missing_sample_names:
                    likely_missing_sample_names.append(sample_name)

        # Buffer an error about missing samples (that are not blanks)
        if len(likely_missing_dnes) > 0:
            # See if *any* samples were found (i.e. the MSRunSample record existed)
            num_samples = len(
                dict(
                    (s, 0)
                    for sh in self.header_to_sample_name.keys()
                    for s in self.header_to_sample_name[sh].keys()
                    if not Sample.is_a_blank(s)
                ).keys()
            )

            if num_samples == len(likely_missing_sample_names):
                self.aggregated_errors_object.buffer_exception(
                    NoSamples(
                        likely_missing_dnes,
                        suggestion=(
                            f"Did you forget to include these {self.headers.SAMPLENAME}s in the "
                            f"{SamplesLoader.DataHeaders.SAMPLE} column of the {SamplesLoader.DataSheetName} sheet?"
                        ),
                    ),
                    is_error=any(
                        e.is_error
                        for e in likely_missing_dnes
                        if hasattr(e, "is_error")
                    ),
                    is_fatal=any(
                        e.is_fatal
                        for e in likely_missing_dnes
                        if hasattr(e, "is_fatal")
                    ),
                )
            else:
                self.aggregated_errors_object.buffer_exception(
                    MissingSamples(
                        likely_missing_dnes,
                        suggestion=(
                            f"Did you forget to include these {self.headers.SAMPLENAME}s in the "
                            f"'{SamplesLoader.DataHeaders.SAMPLE}' column of the {SamplesLoader.DataSheetName} sheet?"
                        ),
                    ),
                    is_error=any(
                        e.is_error
                        for e in likely_missing_dnes
                        if hasattr(e, "is_error")
                    ),
                    is_fatal=any(
                        e.is_fatal
                        for e in likely_missing_dnes
                        if hasattr(e, "is_fatal")
                    ),
                )

        if len(possible_blank_dnes) > 0:
            self.aggregated_errors_object.buffer_warning(
                UnskippedBlanks(
                    possible_blank_dnes,
                    suggestion=(
                        f"Rows for these {self.headers.SAMPLEHEADER}s either need to be added to the "
                        f"'{SamplesLoader.DataHeaders.SAMPLE}' column of the '{SamplesLoader.DataSheetName}' sheet or "
                        f"must have 'skip' in the '{self.headers.SKIP}' column."
                    ),
                ),
                is_fatal=self.validate,
            )

    def get_loaded_msrun_sample_dict(self, peak_annot_file: str) -> dict:
        """This method is only intended to be called after a load has been performed.

        Using self.df, this returns a dict of mzxml metadata and MSRunSample records keyed on sample header for the
        supplied peak_annot_file.  For any sample key in the dict, it will either be mapped to a concrete MSRunSample
        record populated with metadata parsed from the mzXML file (when a sample unambiguously maps to a single mzXML
        file) or a placeholder MSRunSample record when there is either no mzXML file or there are multiple matching
        mzXML files (and in both cases, there will be no metadata populated in that record).

        Sample headers are *no longer* assumed to be unique per peak_annot_file, due to the DataUniqueColumnConstraints.
        Instead, what is assumed is that there will only ever be 1 placeholder MSRunSample record for a sample and
        either 0 or more than 1 concrete MSRunSample records linked to the same sample.

        If an MSRunSample record does not exist, the value in the dict will be null and an error will be buffered (via
        called methods (not directly in this method)).

        Args:
            peak_annot_file (str): Name of a single peak annotation file found in the dataframe
        Exceptions:
            Raises:
                None
            Buffers:
                RecordDoesNotExist
        Returns:
            msrun_sample_dict (dict): A dict of Peak Annotation Details metadata and MSRunSample records for the
                supplied peak_annot_file keyed on sample_header
        """
        target_annot_name = os.path.basename(peak_annot_file)
        msrun_sample_dict: dict = {}

        # Save the current row index
        save_row_index = self.row_index
        # Initialize the row index
        self.set_row_index(None)

        for _, row in self.df.iterrows():
            if self.is_row_empty(row):
                continue
            sample_name = self.get_row_val(row, self.headers.SAMPLENAME)
            sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
            mzxml_path = self.get_row_val(row, self.headers.MZXMLNAME)
            sequence_name = self.get_row_val(row, self.headers.SEQNAME)
            tmp_annot_name = self.get_row_val(row, self.headers.ANNOTNAME)
            skip_str = self.get_row_val(row, self.headers.SKIP)
            skip = (
                True
                if skip_str is not None and skip_str.lower() in self.SKIP_STRINGS
                else False
            )

            # An MSRunSample record is not created when there is no peak annotation file
            if tmp_annot_name is None:
                continue

            # No need to document a skipped row when it does not contain a sample_header, since the row is invalid
            # (which is allowed when skipped).  NOTE: Intentionally treating sample_header (an Optional[str]) as a bool.
            if skip and not sample_header:
                continue

            # If the sample header belongs to a peak annot file other than the target annot file, skip it.  We are
            # explicitly gathering headers from a specific annot file.
            annot_name = os.path.basename(tmp_annot_name)
            if target_annot_name != annot_name:
                continue

            # Default value
            # TODO: Consolidate the strategy.  I had made a quick change to the SKIP value coming from the file due to a
            # pandas quirk about dtype and empty excel cells, but the value returned by this method converts it to a
            # boolean, looked up by the header.  This can lead to confusion, so pick one strategy and go with it.
            msrun_sample_dict[sample_header] = {
                MSRunSample.__name__: None,
                self.headers.SAMPLENAME: sample_name,
                self.headers.SAMPLEHEADER: sample_header,
                self.headers.MZXMLNAME: mzxml_path,
                self.headers.SEQNAME: sequence_name,
                self.headers.ANNOTNAME: tmp_annot_name,
                self.headers.SKIP: skip,
            }

            # If this sample is being skipped, we don't need to retrieve the MSRunSample record.  It shouldn't exist
            # anyway (e.g. blank samples are not created).
            if skip is True:
                continue

            sample = self.get_sample_by_name(sample_name)
            msrun_sequence = self.get_msrun_sequence(name=sequence_name)

            if sample is None or msrun_sequence is None:
                continue

            mzxml_metadata, _ = self.get_matching_mzxml_metadata(
                sample_name,
                sample_header,
                mzxml_path,
            )

            # If this will not be a placeholder record
            if mzxml_metadata["mzaf_record"] is not None:
                # Concrete record query dict
                query_dict = {
                    "msrun_sequence": msrun_sequence,
                    "sample": sample,
                    "polarity": mzxml_metadata["polarity"],
                    "mz_min": mzxml_metadata["mz_min"],
                    "mz_max": mzxml_metadata["mz_max"],
                    "ms_raw_file": mzxml_metadata["rawaf_record"],
                    "ms_data_file": mzxml_metadata["mzaf_record"],
                }
            else:
                # Placeholder record query dict
                query_dict = {
                    "msrun_sequence": msrun_sequence,
                    "sample": sample,
                    "ms_data_file__isnull": True,
                }

            try:
                msrun_sample_dict[sample_header][MSRunSample.__name__] = (
                    MSRunSample.objects.get(**query_dict)
                )
            except MSRunSample.DoesNotExist as dne:
                tmp_msrs = None
                if mzxml_metadata["mzaf_record"] is None:
                    # It's possible that a single concrete MSRunSample record exists that the PeakGroups are linked to.
                    # The above query was for a placeholder.  This can happen because the PeakAnnotationsLoader doesn't
                    # have access to the mzXML files (nor do we want to have to supply them), and when it calls this
                    # method, the mzxml_dict is empty.  The above command will often succeed in this case because there
                    # is usually a placeholder record (because there are usually multiple files with the same name).
                    # But when there's 1 file, the PeakGroups will link to the concrete record, so:
                    try:
                        concrete_query_dict = {
                            "msrun_sequence": msrun_sequence,
                            "sample": sample,
                        }
                        tmp_msrs = MSRunSample.objects.get(**concrete_query_dict)
                        msrun_sample_dict[sample_header][
                            MSRunSample.__name__
                        ] = tmp_msrs
                    except Exception:
                        pass
                if tmp_msrs is None:
                    self.aggregated_errors_object.buffer_error(
                        RecordDoesNotExist(
                            MSRunSample,
                            query_dict,
                            file=self.friendly_file,
                            sheet=self.sheet,
                            rownum=self.rownum,
                        ),
                        orig_exception=dne,
                    )

        # Restore the original row index
        self.set_row_index(save_row_index)

        return msrun_sample_dict

    @transaction.atomic
    def get_or_create_mzxml_and_raw_archive_files(self, mzxml_file):
        """Get or create ArchiveFile records for an mzXML file and a record for its raw file.  Updates
        self.mzxml_dict_by_header (via self.set_mzxml_metadata).

        Args:
            mzxml_file (str or Path object)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                DataType.DoesNotExist
                DataFormat.DoesNotExist
        Returns:
            mzaf_rec (Optional[ArchiveFile])
            mzaf_created (boolean)
            rawaf_rec (Optional[ArchiveFile])
            rawaf_created (boolean)
        """

        # Set the row index / rownum to None.  We haven't started reading the sheet yet, so clear the index so that
        # buffer_infile_exception does not inaccurately report row numbers.
        self.set_row_index(None)

        # Parse out the polarity, mz_min, mz_max, raw_file_name, and raw_file_sha1
        default_suggestion = "The mzXML file will be skipped."
        raised = False
        mzxml_metadata = None
        errs: AggregatedErrors
        try:
            mzxml_metadata, errs = self.parse_mzxml(mzxml_file)
        except FileNotFoundError as fnfe:
            self.buffer_infile_exception(fnfe)
            raised = True
        except NoScans as ns:
            # There's no MSRunSample to load, but the mzXML_metadata is used for metadata other than just the polarity
            # and scan range, so this fills in that data before the eventual return.  A call to this method is made
            # below, but this exception will cause a return before it gets there.  Filling this data in will prevent bad
            # lookups of actual samples/files, because the user may have added this file to the peak annotation details
            # sheet.
            self.set_mzxml_metadata(mzxml_metadata, mzxml_file)

            self.buffer_infile_exception(
                ns, is_error=False, suggestion=default_suggestion
            )
            raised = True
        finally:
            if raised:
                errs = AggregatedErrors()
                mzxml_metadata = None
        for exc in errs.exceptions:
            suggestion = None
            if (
                mzxml_metadata is None
                and isinstance(exc, InfileError)
                and exc.is_error is False
            ):
                suggestion = default_suggestion
            self.buffer_infile_exception(exc, suggestion=suggestion)

        if errs.num_errors > 0:
            self.errored(ArchiveFile.__name__)
            # No need to raise, because we haven't tried to create a record yet
            return None, False, None, False

        if mzxml_metadata is None:
            # mzxml_metadata can be None if the file did not exist or the scan file was empty
            self.skipped(ArchiveFile.__name__)
            # No need to raise, because errors and warnings have been buffered above
            return None, False, None, False

        # Get or create the ArchiveFile record for the mzXML
        try:
            mz_rec_dict = {
                # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "file_location": mzxml_file,  # Intentionally a string and not a File object
                "data_type": DataType.objects.get(code="ms_data"),
                "data_format": DataFormat.objects.get(code="mzxml"),
            }
            mzaf_rec, mzaf_created = ArchiveFile.objects.get_or_create(**mz_rec_dict)
            if mzaf_created:
                self.created(ArchiveFile.__name__)
                # Save the path to the file in the archive in case of rollback
                self.created_mzxml_archive_file_recs.append(mzaf_rec)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            self.skipped(ArchiveFile.__name__)  # Skipping raw file below
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, mz_rec_dict)
            self.errored(ArchiveFile.__name__)
            self.skipped(ArchiveFile.__name__)  # Skipping raw file below
            raise RollbackException()

        # Get or create an ArchiveFile record for a raw file
        try:
            raw_rec_dict = {
                "filename": mzxml_metadata["raw_file_name"],
                "checksum": mzxml_metadata["raw_file_sha1"],
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                # "file_location": xxx,  # We do not store raw files
                "data_type": DataType.objects.get(code="ms_data"),
                "data_format": DataFormat.objects.get(code="ms_raw"),
            }
            rawaf_rec, rawaf_created = ArchiveFile.objects.get_or_create(**raw_rec_dict)
            if rawaf_created:
                self.created(ArchiveFile.__name__)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            # Skipping mzXML file above (rolled back)
            self.skipped(ArchiveFile.__name__)
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, raw_rec_dict)
            self.errored(ArchiveFile.__name__)
            raise RollbackException()

        self.set_mzxml_metadata(
            mzxml_metadata,
            mzxml_file,
            mzaf_rec=mzaf_rec,
            rawaf_rec=rawaf_rec,
        )

        return (
            mzaf_rec,
            mzaf_created,
            rawaf_rec,
            rawaf_created,
        )

    def set_mzxml_metadata(
        self,
        mzxml_metadata: Optional[dict],
        mzxml_file: str,
        mzaf_rec: Optional[ArchiveFile] = None,
        rawaf_rec: Optional[ArchiveFile] = None,
    ):
        """Updates mzxml_metadata and adds it to self.mzxml_dict_by_header.

        If the mzxml_metadata hasn't been fully populated (such as would be the case if the mzXML file had no scans, it
        initializes the keys with None values.  It also adds extra metadata to the mzxml_metadata dict that will be
        appended to the list stored in self.mzxml_dict_by_header[mzxml_name][mzxml_dir].

        self.mzxml_dict_by_header[mzxml_name][mzxml_dir] (List[dict]): Each dict contains:
            mzaf_record (ArchiveFile): To be added to a concrete MSRunSample record
            rawaf_record (ArchiveFile): To be added to a concrete MSRunSample record
            mzxml_dir (str): The relative path up to but not including the mzXML file.
            mzxml_filename (str): The unmodified name of the mzXML file.
            mzxml_filepath (str): Path relative to the study doc to the mzXML files.
            sample_name (Optional[str]): Initialized to None.  Set elsewhere.
            added (bool): This tracks whether the corresponding concrete MSRunSample record has been created or not.

        This is a helper method to get_or_create_mzxml_and_raw_archive_files.

        Args:
            mzxml_metadata (Optional[dict]): A dict obtained by parsing the mzXML file, but supplemented with other
                metadata.  See above.
            mzxml_file (str)
            mzaf_rec (Optional[ArchiveFile])
            rawaf_rec (Optional[ArchiveFile])
        Exceptions:
            None
        Returns:
            None
        """

        mzxml_dir, mzxml_filename = os.path.split(mzxml_file)

        abs_mzxml_dir = os.path.abspath(mzxml_dir)
        # Make the mzxml_dir be relative to self.mzxml_dir
        mzxml_dir = os.path.relpath(abs_mzxml_dir, self.mzxml_dir)

        if mzxml_metadata is None:
            mzxml_metadata = {}

        # Carefully initialize with None values when the keys do not yet exist in the dict passed in
        if "raw_file_name" not in mzxml_metadata.keys():
            mzxml_metadata["raw_file_name"] = None
        if "raw_file_sha1" not in mzxml_metadata.keys():
            mzxml_metadata["raw_file_sha1"] = None
        if "polarity" not in mzxml_metadata.keys():
            mzxml_metadata["polarity"] = None
        if "mz_min" not in mzxml_metadata.keys():
            mzxml_metadata["mz_min"] = None
        if "mz_max" not in mzxml_metadata.keys():
            mzxml_metadata["mz_max"] = None

        # Add in the ArchiveFile record objects, if supplied (and not already set)
        if "mzaf_record" not in mzxml_metadata.keys() or mzaf_rec is not None:
            mzxml_metadata["mzaf_record"] = mzaf_rec
        if "rawaf_record" not in mzxml_metadata.keys() or rawaf_rec is not None:
            mzxml_metadata["rawaf_record"] = rawaf_rec

        # These are safe to always set.  We are assuming this won't change and that this method is called only when they
        # need to be set.  These values are primarily used for error reporting.
        mzxml_metadata["mzxml_dir"] = mzxml_dir
        mzxml_metadata["mzxml_filename"] = mzxml_filename
        # Set a filepath relative to the mzXML dir
        mzxml_metadata["mzxml_filepath"] = os.path.relpath(mzxml_file, self.mzxml_dir)
        # No sample from the input file is associated with this mzXML (yet)
        mzxml_metadata["sample_name"] = None

        # No sample from the input file is associated with this mzXML (yet)
        if "sample_name" not in mzxml_metadata.keys():
            mzxml_metadata["sample_name"] = None

        # We will use this to know when to add leftovers that were not in the infile.  There are 2 methods by which the
        # files are processed and this is the means by which those processes don't step on each other's toes.  The 2
        # methods are via processing the Peak Annotation Details sheet where the mzXML File has been explicitly assigned
        # to a sample by the user and the other is via a directory walk (to catch unaccounted for files, and to make it
        # unnecessary for the user to have to fill in the optional mzXML File Name column, which is a huge reduction in
        # labor required by the user).  The thing that makes this setting useful is that there are almost always files
        # by the same name and they almost always are incompletely represented in the Peka Annotation Details sheet for
        # multiple reasons.  Knowing which were added by the sheet and which are left over is what this value is for.
        if "added" not in mzxml_metadata.keys():
            mzxml_metadata["added"] = False

        # Save the metadata by mzxml name (which may not be unique, so we're using the record ID as a second key, so
        # that we can later associate a sample header (with the same non-unique issue) to its multiple mzXMLs).
        mzxml_name = self.make_sample_header_from_mzxml_name(mzxml_filename)

        # This is the end goal of this method, to add the metadata to self.mzxml_dict_by_header
        self.mzxml_dict_by_header[mzxml_name][mzxml_dir].append(mzxml_metadata)

    @transaction.atomic
    def get_or_create_msrun_sample_from_row(self, row):
        """Takes a row from the Peak Annotation Details sheet/file and gets or creates MSRunSample records.

        Calls check_reassign_peak_groups, which shuffles PeakGroups around and potentially deletes placeholder
        MSRunSample records whose PeakGroups have been reassigned to a concrete MSRunSample record.

        Updates self.mzxml_dict_by_header (via get_matching_mzxml_metadata) to denote which mzXML files were included in
        MSRunSample records identified from the row data.  This is later used to process leftover mzXML files that were
        not denoted in the peak annotation details file/sheet.

        Updates self.header_to_sample_name, which is used to check for possible duplicate samples.

        Also updates self.mzxml_to_sample_name, which is used to associate mzXML files with the samples they belong to
        (when they could be assigned to a specific row of the infile, due to multiple files with the same name).

        Args:
            row (pandas dataframe row)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                RecordDoesNotExist
        Returns:
            rec (Optional[MSRunSample])
            created (boolean)
        """
        created = False
        rec = None

        try:
            msrs_rec_dict = None
            sample_name = self.get_row_val(row, self.headers.SAMPLENAME)
            sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
            mzxml_path = self.get_row_val(row, self.headers.MZXMLNAME)
            sequence_name = self.get_row_val(row, self.headers.SEQNAME)
            skip_str = self.get_row_val(row, self.headers.SKIP)
            skip = (
                True
                if skip_str is not None and skip_str.lower() in self.SKIP_STRINGS
                else False
            )
            # Annotation file name is not used in the load of this data.  It is only used when the PeakAnnotationsLoader
            # retrieves metadata for a particular peak annotations file by calling get_loaded_msrun_sample_dict.

            # Check the mzXML path, if one is given and contains a subfolder.  If the mzXML File Name column value
            # contains a path (which can be supplied to resolve file name collisions that map to different samples) and
            # the actual file doesn't exist, buffer an error and abort the lookup.
            mzxml_dir = None
            mzxml_filename = None
            if mzxml_path is not None:
                mzxml_dir, mzxml_filename = os.path.split(mzxml_path)
                if (
                    mzxml_dir != ""
                    and not os.path.samefile(mzxml_dir, self.mzxml_dir)
                    and not os.path.exists(mzxml_path)
                ):
                    self.errored(MSRunSample.__name__)
                    self.buffer_infile_exception(FileFromInputNotFound(mzxml_path))
                    return rec, False

            if skip is True:
                self.skipped(MSRunSample.__name__)
                if mzxml_path is not None:
                    if os.path.isabs(mzxml_dir):
                        mzxml_dir = os.path.relpath(mzxml_dir, self.mzxml_dir)
                    mzxml_name = self.make_sample_header_from_mzxml_name(mzxml_filename)
                    self.skip_msrunsample_by_mzxml[mzxml_name][mzxml_dir].append(
                        self.rownum
                    )
                elif sample_header is not None:
                    # If there happen to be mzXMLs supplied, but not in the mzXML column, add the sample header to cause
                    # the skip (fingers crossed, there's not some difference - but we can't necessarily know that).
                    # TODO: Account for the dash/underscore issue here.  Isocorr changes dashes in the mzXML name to
                    # underscores, and we haven't accounted for that here...
                    self.skip_msrunsample_by_mzxml[sample_header][""].append(
                        self.rownum
                    )

            # We must skip erroneous rows after having updated self.skip_msrunsample_by_mzxml, because self.mzxml_files
            # aren't skipped
            if self.is_skip_row() or skip is True:
                if skip:
                    print(
                        f"Skipping row: {[sample_name, sample_header, mzxml_path, sequence_name, skip_str]}",
                        flush=True,
                    )
                else:
                    print(
                        f"Erroneous row: {[sample_name, sample_header, mzxml_path, sequence_name, skip_str]}",
                        flush=True,
                    )
                self.skipped(MSRunSample.__name__)
                return rec, created

            # This assumes that either sample header or mzXML name is required (otherwise self.is_skip_row() or skip
            # would be True).  NOTE: Intentionally treating sample_header (an Optional[str]) as a bool.
            if sample_header:
                self.header_to_sample_name[sample_header][sample_name].append(
                    self.rownum
                )

            sample = self.get_sample_by_name(sample_name)
            msrun_sequence = self.get_msrun_sequence(name=sequence_name)

            if mzxml_path is None:
                # Assume that the mzXML name matches the sample header (as it would only differ if the user explicitly
                # edited it).  This is safe to assume because we're only going to use it if we could not pair up an
                # mzXML file with a row of the peak annotation details sheet, in which case the user would get an error
                # and be instructed to add the file to the appropriate row manually.
                self.mzxml_to_sample_name[sample_header][sample_name].append(
                    self.rownum
                )
            else:
                mzxml_name = self.make_sample_header_from_mzxml_name(
                    os.path.basename(mzxml_path)
                )

                # The leftover mzXMLs code uses self.mzxml_to_sample_name to lookup the DB sample name.  NOTE: The peak
                # correction software does not allow sample names to start with a number, so even though the sample data
                # header is usually identical to the mzXML basename, this mapping is tracked separately.  This is
                # maintained separately from self.header_to_sample_name also in case the headers distinctly map
                # intentionally to different samples, but the mzXML names do not.
                self.mzxml_to_sample_name[mzxml_name][sample_name].append(self.rownum)

                if sample_header is not None:
                    # We're going to check to see if the sample_header and mzxml_name differ and issue a warning for
                    # users to double-check things in case they accidentally mismatches an mzXML and a sample header in
                    # the peak annotation details sheet.
                    sample_header_name = self.make_sample_header_from_mzxml_name(
                        sample_header
                    )
                    if sample_header_name != mzxml_name and (
                        # mzxmls that start with a digit must be changed due to peak correction software requirements
                        not mzxml_name[0].isdigit()
                        or mzxml_name not in sample_header_name
                    ):
                        self.aggregated_errors_object.buffer_warning(
                            MzxmlSampleHeaderMismatch(sample_header, mzxml_path),
                            # This exception will be fatal/raised in validate mode (but only printed in curator mode).
                            # I.e. This can be ignored by a curator, but it should be brought to the attention of an
                            # unprivileged user.
                            is_fatal=self.validate,
                        )
                        self.warned(MSRunSample.__name__)

            if sample is None or msrun_sequence is None:
                print(
                    "Skipping because sample or sequence not found: "
                    f"{[sample_name, sample_header, mzxml_path, sequence_name, skip_str]}"
                )
                self.skipped(MSRunSample.__name__)
                return rec, created

            # 1. Determine what actual mzXML file (which may be none) and mzXML metadata matches this row/sample_header
            #    and annot file, and whether multiple files match
            mzxml_metadata, multiple_matches = self.get_matching_mzxml_metadata(
                sample_name,
                sample_header,
                mzxml_path,  # Might just be the filename (no directory included)
            )

            # 2. If there already exists an MSRunSample record (a concrete record for an actual file or placeholder
            #    record when there is no 1 file) for this mzxml file
            if mzxml_metadata["added"] is True and not multiple_matches:
                self.existed(MSRunSample.__name__)

            # 3. Mark this particular mz ArchiveFile record as having been added to an MSRunSample record (even though
            #    retrieval and/or creation may fail below [which will buffer an error that eventually will be raised],
            #    because the point is that we don't try and get or create it again as a "leftover" from the infile)
            mzxml_metadata["added"] = True
            # The input file has explicitly associated this sample name with this mzXML file
            mzxml_metadata["sample_name"] = sample_name

            # 4. Create a record dict to be used for creating an MSRunSample record
            msrs_rec_dict = {
                "msrun_sequence": msrun_sequence,
                "sample": sample,
                "polarity": mzxml_metadata["polarity"],
                "mz_min": mzxml_metadata["mz_min"],
                "mz_max": mzxml_metadata["mz_max"],
                "ms_raw_file": mzxml_metadata["rawaf_record"],
                "ms_data_file": mzxml_metadata["mzaf_record"],
            }

            is_placeholder = mzxml_metadata["mzaf_record"] is None

            # NOTE: At this point, we have the right sample and sequence, and we either have no mzXML file (either
            # because none matched or because multiple matched) or we have an mzXML file with extracted metadata.

            # 5. If we're getting/creating a placeholder MSRunSample record (because there's no 1 mzXML file associated
            #    with this row)
            if is_placeholder:
                msrs_placeholder_query_dict = {
                    "msrun_sequence": msrun_sequence,
                    "sample": sample,
                    "ms_data_file__isnull": True,
                }

                rec, created = MSRunSample.objects.get_or_create(
                    **msrs_placeholder_query_dict, defaults=msrs_rec_dict
                )
            else:  # We're getting/creating a concrete MSRunSample record
                rec, created = MSRunSample.objects.get_or_create(**msrs_rec_dict)

            # 6. Check the MSRunSample records the PeakGroup records link to and re-arrange them if necessary.  Note,
            # this could result in the creation of a placeholder MSRunSample record.
            self.check_reassign_peak_groups(
                sample,
                msrun_sequence,
                multiple_matches,
            )

            if created:
                self.created(MSRunSample.__name__)
            else:
                self.existed(MSRunSample.__name__)

        except Exception as e:
            self.handle_load_db_errors(e, MSRunSample, msrs_rec_dict)
            self.errored(MSRunSample.__name__)
            raise RollbackException()

        return rec, created

    def check_reassign_peak_groups(
        self,
        sample,
        msrun_sequence,
        multiple_matches,
    ):
        """This method enforces a simple rule:  If a Sample/MSRunSequence has multiple (or no) concrete^ MSRunSample
        records, all PeakGroup records must link to a placeholder^ record (for the same sample/sequence).  If a
        Sample/MSRunSequence has only 1 concrete^ MSRunSample record, all PeakGroups must link to that concrete^
        MSRunSample record.

        ^concrete = An MSRunSample whose mzXML ArchiveFile record is not None
        ^placeholder = An MSRunSample record whose mzXML ArchiveFile record is None

        Args:
            sample (Sample)
            msrun_sequence (MSRunSequence)
            multiple_matches (bool)
        Exceptions:
            None
        Returns:
            None
        """
        # See if there exists a matching placeholder record (there can be only 1)
        placeholder_msrs_rec = MSRunSample.objects.filter(
            msrun_sequence=msrun_sequence,
            sample=sample,
            ms_data_file__isnull=True,
        ).first()

        # See if there exist any matching concrete records
        concrete_msrs_qs = MSRunSample.objects.filter(
            msrun_sequence=msrun_sequence,
            sample=sample,
            ms_data_file__isnull=False,
        )

        if concrete_msrs_qs.count() == 0:
            return

        if concrete_msrs_qs.count() == 1:
            concrete_msrs_rec = concrete_msrs_qs.first()
            if multiple_matches:
                # If there were multiple matches, it means that even though there's currently only 1 MSRunSample record,
                # there are more coming, so re-link any peak groups from a previous load to the placeholder MSRunSample
                # record
                if concrete_msrs_rec.peak_groups.count() > 0:
                    # If a placeholder record does not exist, create one
                    if placeholder_msrs_rec is None:
                        placeholder_msrs_rec = MSRunSample.objects.create(
                            sample=sample,
                            msrun_sequence=msrun_sequence,
                        )
                        self.created(MSRunSample.__name__)

                    # Create a list so that the updates to the record avoid issues with the queryset (which is based on
                    # the link we're changing)
                    pg_recs = list(concrete_msrs_rec.peak_groups.all())
                    for pg_rec in pg_recs:
                        pg_rec.msrun_sample = placeholder_msrs_rec
                        pg_rec.full_clean()
                        pg_rec.save()
                        self.updated(PeakGroup.__name__)

            elif placeholder_msrs_rec is not None:
                # This case is when an mzXML is being added after-the-fact.  If there exist peak groups only for the
                # same peak annotation file, we can link those peak groups to the concrete record and delete the
                # placeholder.

                pg_recs = list(placeholder_msrs_rec.peak_groups.all())
                pg_rec: PeakGroup
                for pg_rec in pg_recs:
                    pg_rec.msrun_sample = concrete_msrs_rec
                    pg_rec.full_clean()
                    pg_rec.save()
                    self.updated(PeakGroup.__name__)

                # Now the placeholder record is empty, so there's no need to keep it around
                placeholder_msrs_rec.delete()
                self.deleted(MSRunSample.__name__)

        if concrete_msrs_qs.count() > 1:
            for concrete_msrs_rec in concrete_msrs_qs.all():
                pg_recs = list(concrete_msrs_rec.peak_groups.all())
                for pg_rec in pg_recs:
                    # If a placeholder record does not exist (and peak groups to move, exist), create one
                    if placeholder_msrs_rec is None:
                        placeholder_msrs_rec = MSRunSample.objects.create(
                            sample=sample,
                            msrun_sequence=msrun_sequence,
                        )
                        self.created(MSRunSample.__name__)

                    # Re-link all PeakGroups to the placeholder record
                    pg_rec.msrun_sample = placeholder_msrs_rec
                    pg_rec.full_clean()
                    pg_rec.save()
                    self.updated(PeakGroup.__name__)

    def get_sample_by_name(
        self, sample_name: str, from_mzxmls: Optional[List[str]] = None
    ):
        """Get a Sample record by name.

        Assumptions:
            1. All of the files supplied in from_mzxmls have the same name.
        Args:
            sample_name (str)
            from_mzxmls (Optional[List[str]]): A list of file paths from which the sample header was assumed to have
                been derived.  This tells the RecordDoesNotExist exception whether to use the Peak Annotation Details
                sheet location or the mzXML filepaths as a reference to what we were searching for.  (Only one of these
                searches was performed, but we report every file of the same name so that the researcher can append them
                to the sheet to be skipped, or provide the missing peak annotation file to cover the unanalyzed data).
        Exceptions:
            Raises:
                None
            Buffers:
                RecordDoesNotExist
                MultipleRecordsReturned
        Returns:
            Optional[Sample]
        """
        rec = None
        try:
            rec = Sample.objects.get(name=sample_name)
        except Sample.DoesNotExist as dne:
            # If this sample was derived from an mzXML filename
            if from_mzxmls:
                orig_mzxml_sample_name = os.path.splitext(
                    os.path.basename(from_mzxmls[0])
                )[0]

                # Let's see if this is a "dash" issue
                sample_name_nodash = self.make_sample_header_from_mzxml_name(
                    sample_name
                )
                if sample_name_nodash != sample_name:
                    try:
                        return Sample.objects.get(name=sample_name_nodash)
                    except Sample.DoesNotExist:
                        # Ignore this attempt and press on with processing the original exception
                        pass
                else:
                    # It's possible the dash was already replaced in sample_name, but the researcher manually included
                    # the dash in the DB sample name, so try WITH the dash.
                    if orig_mzxml_sample_name != sample_name:
                        try:
                            return Sample.objects.get(name=orig_mzxml_sample_name)
                        except Sample.DoesNotExist:
                            # Ignore this attempt and press on with processing the original exception
                            pass

                if sample_name[0].isdigit():
                    sample_name_used = sample_name
                    try:
                        # Some peak correction tools disallow sample names that start with a number, so let's
                        # see if the user potentially prepended the sample name and the rest of it happens to be
                        # unique
                        rec = Sample.objects.get(name__endswith=sample_name)
                    except Sample.DoesNotExist:
                        # It's possible the dash was already replaced in sample_name, but the researcher manually
                        # included the dash in the DB sample name, so try WITH the dash.
                        if orig_mzxml_sample_name != sample_name:
                            try:
                                rec = Sample.objects.get(
                                    name__endswith=orig_mzxml_sample_name
                                )
                                sample_name_used = orig_mzxml_sample_name
                            except Sample.DoesNotExist:
                                # Ignore this attempt and press on with processing the original exception
                                pass
                            except Sample.MultipleObjectsReturned:
                                matching_samples = list(
                                    Sample.objects.filter(
                                        name__endswith=orig_mzxml_sample_name
                                    ).values_list("name", flat=True)
                                )
                                self.aggregated_errors_object.buffer_exception(
                                    MultipleRecordsReturned(
                                        Sample,
                                        {"name": sample_name},
                                        file=self.friendly_file,
                                        sheet=self.sheet,
                                        column=self.headers.MZXMLNAME,
                                        rownum="no row - sample name was derived from an mzXML filename",
                                        message=(
                                            f"{Sample.__name__} record matching the exact mzXML file's basename "
                                            f"[{sample_name_used}] does not exist, but the filename starts with a "
                                            "number, which means that the peak annotation software would have "
                                            "required the name to be prepended with a letter.  There are multiple "
                                            f"samples that end with this name: {matching_samples}.  Please "
                                            f"identify the associated sample(s) and add the files {from_mzxmls} to "
                                            "%s.  If no such row exists and this is an unanalyzed mzXML file, add a "
                                            f"row and fill in the '{self.headers.SKIP}' column."
                                        ),
                                    ),
                                    is_error=not Sample.is_a_blank(sample_name),
                                    is_fatal=not Sample.is_a_blank(sample_name)
                                    or self.validate,
                                    orig_exception=dne,
                                )
                                return None
                    except Sample.MultipleObjectsReturned:
                        matching_samples = list(
                            Sample.objects.filter(
                                name__endswith=sample_name
                            ).values_list("name", flat=True)
                        )
                        self.aggregated_errors_object.buffer_exception(
                            MultipleRecordsReturned(
                                Sample,
                                {"name": sample_name},
                                file=self.friendly_file,
                                sheet=self.sheet,
                                column=self.headers.MZXMLNAME,
                                rownum="no row - sample name was derived from an mzXML filename",
                                message=(
                                    f"{Sample.__name__} record matching the exact mzXML file's basename "
                                    f"[{sample_name}] does not exist, but the filename starts with a number, which "
                                    "means that the peak annotation software would have required the name to be "
                                    "prepended with a letter.  There are multiple samples that end with this name: "
                                    f"{matching_samples}.  Please identify the associated sample(s) and add the "
                                    f"files {from_mzxmls} to %s.  If no such row exists and this is an unanalyzed "
                                    f"mzXML file, add a row and fill in the '{self.headers.SKIP}' column."
                                ),
                            ),
                            is_error=not Sample.is_a_blank(sample_name),
                            is_fatal=not Sample.is_a_blank(sample_name)
                            or self.validate,
                            orig_exception=dne,
                        )
                        return None
                    finally:
                        if rec is not None:

                            # Buffer a warning that says that we're going to proceed assuming the found sample is a
                            # match
                            self.aggregated_errors_object.buffer_warning(
                                AssumedMzxmlSampleMatch(
                                    sample_name=rec.name,
                                    mzxml_file=from_mzxmls[0],  # 1 Example
                                    file=self.friendly_file,
                                    sheet=self.sheet,
                                    column=self.headers.MZXMLNAME,
                                    rownum="no row - sample name was derived from an mzXML filename",
                                ),
                                is_fatal=self.validate,
                                orig_exception=dne,
                            )

                            return rec

                # The mzXMLs need to be iterated to create or `UnmatchedMzXML` or `UnmatchedBlankMzXML` exceptions for
                # each file so that this script doesn't need to be run multiple times to add files to the 'Peak
                # Annotation Details' sheet
                for from_mzxml in from_mzxmls:
                    if Sample.is_a_blank(sample_name):
                        # This warning may already exist from the check_mzxml_files method.  This is different from the
                        # errors buffered below.  (Errors stop the load so that they are not encountered twice.)
                        if not self.aggregated_errors_object.exception_exists(
                            UnmatchedBlankMzXML, "mzxml_file", from_mzxml
                        ):
                            self.aggregated_errors_object.buffer_warning(
                                UnmatchedBlankMzXML(
                                    from_mzxml,
                                    self.headers.SAMPLEHEADER,
                                    self.headers.SKIP,
                                    file=self.friendly_file,
                                    sheet=self.sheet,
                                    column=self.headers.MZXMLNAME,
                                    rownum="no row - sample name was derived from an mzXML filename",
                                    suggestion="This file will not be linked to any sample.",
                                ),
                                is_fatal=self.validate,
                                orig_exception=dne,
                            )
                    else:
                        self.aggregated_errors_object.buffer_error(
                            UnmatchedMzXML(
                                from_mzxml,
                                self.headers.SAMPLEHEADER,
                                self.headers.SKIP,
                                file=self.friendly_file,
                                sheet=self.sheet,
                                column=self.headers.MZXMLNAME,
                                rownum="no row - sample name was derived from an mzXML filename",
                            ),
                            orig_exception=dne,
                        )
            else:
                self.aggregated_errors_object.buffer_exception(
                    RecordDoesNotExist(
                        Sample,
                        {"name": sample_name},
                        file=self.friendly_file,
                        sheet=self.sheet,
                        column=self.headers.SAMPLENAME,
                        rownum=self.rownum,
                    ),
                    is_error=not Sample.is_a_blank(sample_name),
                    is_fatal=not Sample.is_a_blank(sample_name) or self.validate,
                    orig_exception=dne,
                )

        return rec

    @transaction.atomic
    def get_or_create_msrun_sample_from_mzxml(
        self,
        sample,
        mzxml_name,
        mzxml_dir,
        mzxml_metadata,
        default_msrun_sequence,
    ):
        """Takes a sample record, default msrun_sequence record, the name of the mzXML file (without the extension),
        the directory path to the mzXML file, and metadata parsed from the mzxml (including ArchiveFile records created
        from the file) and gets or creates MSRunSample records.  It assumes that the mzxml_metadata contains an
        ArchiveFile record for an mzXML file.

        See get_or_create_msrun_sample_from_row to add mzXML files to MSRunSample records while updating PeakGroup
        records.

        Args:
            sample (Optional[Sample])
            mzxml_name (str): Basename of an mzXML file *without* the extension.
            mzxml_dir (str): Directory path of an mzXML file.
            default_msrun_sequence (Optional[MSRunSequence])
            mzxml_metadata (dict): This dict contains metadata parsed from the mzXML file.  Example structure:
                {
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 100.0,
                    "raw_file_name": "sample1.raw",
                    "raw_file_sha1": "KJCWVQUWEKENF",
                    "mzaf_record": mzxml_rec,
                    "rawaf_record": raw_rec,
                    "mzxml_dir": "some/path/to/file",
                    "mzxml_filename": "sample1.mzXML",
                    "added": False,
                }
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                RecordDoesNotExist
        Returns:
            rec (MSRunSample)
            created (boolean)
        """
        rec = None
        created = False

        # This doesn't return a "gotten" record (which is "wrong", seeing as how this methis is a *get*_or_create
        # method), but returning here keeps the created/skipped stats accurate.
        if mzxml_metadata["added"] is True or sample is None:
            # If sample is None, get_sample_by_name will have already buffered an error
            return rec, created

        print(
            (
                f"Loading MSRunSamples from mzXML not paired with a row from the input file: "
                f"{os.path.join(mzxml_metadata['mzxml_dir'], mzxml_metadata['mzxml_filename'])}"
            ),
            flush=True,
        )

        # Now determine the sequence
        msrun_sequence = self.get_msrun_sequence_from_dir(
            mzxml_name,
            mzxml_dir,
            default_msrun_sequence,
            mzxml_filename=mzxml_metadata["mzxml_filename"],
        )

        if msrun_sequence is None:
            self.errored(MSRunSample.__name__)
            return rec, created

        msrs_rec_dict = {}
        try:
            msrs_rec_dict = {
                "msrun_sequence": msrun_sequence,
                "sample": sample,
                "polarity": mzxml_metadata["polarity"],
                "mz_min": mzxml_metadata["mz_min"],
                "mz_max": mzxml_metadata["mz_max"],
                "ms_raw_file": mzxml_metadata["rawaf_record"],
                "ms_data_file": mzxml_metadata["mzaf_record"],
            }

            rec, created = MSRunSample.objects.get_or_create(**msrs_rec_dict)

            # Update the fact this one has been handled
            mzxml_metadata["added"] = True
            mzxml_metadata["sample_name"] = sample.name

            if created:
                self.created(MSRunSample.__name__)
            else:
                self.existed(MSRunSample.__name__)

        except Exception as e:
            self.handle_load_db_errors(e, MSRunSample, msrs_rec_dict)
            self.errored(MSRunSample.__name__)
            raise RollbackException()

        return rec, created

    def get_msrun_sequence_from_dir(
        self,
        mzxml_name,
        mzxml_dir,
        default_msrun_sequence: Optional[MSRunSequence],
        mzxml_filename=None,
    ):
        """Uses the self.annotdir_to_seq_dict to assign the peak annotation file's sequence to mzXML files that have a
        common path.

        Args:
            mzxml_name (str): Basename of the mzXML file, potentially modified to adhere to sample header strictures.
            mzxml_dir (str): Path to directory of the mzXML file.
            default_msrun_sequence (Optional[MSRunSequence]): Default sequence to use if there are no or multiple
                sequences.
            mzxml_filename (Optional[str]): Used for error reporting.
        Exceptions:
            Raises:
                None
            Buffers:
                MzxmlNotColocatedWithAnnot
                MzxmlColocatedWithMultipleAnnot
        Returns:
            msrun_sequence (Optional[MSRunSequence]): The MSRunSequence record that the mzXML file belongs to.
        """
        if (
            self.annotdir_to_seq_dict is None
            or len(self.annotdir_to_seq_dict.keys()) == 0
        ):
            return default_msrun_sequence

        msrun_sequence = None
        msrun_sequence_names = []
        nearest_annot_dir: str
        nearest_annot_dir_assigned = False

        if mzxml_filename is None:
            mzxml_filename = mzxml_name

        # The paths in self.annotdir_to_seq_dict are relative paths, so make the mzXML path relative as well
        if os.path.isabs(mzxml_dir):
            mzxml_dir = os.path.relpath(mzxml_dir, start=self.mzxml_dir)

        # Build the msrun_sequence_names list containing unique sequence names of peak annotation files found nearest to
        # the mzXML file (along its path) (by iterating over the paths of the peak annotation files from longest to
        # shortest)
        for annot_dir in sorted(
            self.annotdir_to_seq_dict.keys(),
            key=lambda p: len(str(p).split(os.sep)),
            reverse=True,
        ):
            common_dir = os.path.commonpath([mzxml_dir, annot_dir])
            norm_common_dir = os.path.normpath(common_dir)
            norm_annot_dir = os.path.normpath(annot_dir)
            if norm_annot_dir == norm_common_dir:
                # We want the sequence associated with the peak annotation file that is the closest to the mzXML.
                if nearest_annot_dir_assigned is False:
                    nearest_annot_dir = annot_dir
                    nearest_annot_dir_assigned = True
                if nearest_annot_dir == annot_dir:
                    for seqname in self.annotdir_to_seq_dict[annot_dir]:
                        # If the current annot_dir (which corresponds to a peak annotation file with a default sequence)
                        # matches the nearest_annot_dir (which is the closest dir to the mzXML in question), and its
                        # assigned default sequence hasn't already been added to the list of sequence names associated
                        # with this directory
                        if seqname not in msrun_sequence_names:
                            msrun_sequence_names.append(seqname)

        # If none are found
        if len(msrun_sequence_names) == 0:
            if default_msrun_sequence is not None:
                self.aggregated_errors_object.buffer_warning(
                    MzxmlNotColocatedWithAnnot(
                        annot_dirs=list(self.annotdir_to_seq_dict.keys()),
                        file=os.path.join(mzxml_dir, mzxml_filename),
                        suggestion=f"Using the default sequence '{default_msrun_sequence.sequence_name}'.",
                    ),
                    is_fatal=self.validate,
                )
                msrun_sequence = default_msrun_sequence
            else:
                self.aggregated_errors_object.buffer_error(
                    MzxmlNotColocatedWithAnnot(
                        annot_dirs=list(self.annotdir_to_seq_dict.keys()),
                        file=os.path.join(mzxml_dir, mzxml_filename),
                        suggestion=(
                            "Either fill in default sequences for the peak annotation files in the "
                            f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet or move "
                            f"'{os.path.join(mzxml_dir, mzxml_filename)}' and its peak annotation file(s) into a "
                            "common directory (and don't forget to update path in the "
                            f"'{PeakAnnotationFilesLoader.DataHeaders.FILE}' column in the "
                            f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet)."
                        ),
                    )
                )
        elif len(msrun_sequence_names) > 1:  # If multiple are found
            suggestion = None
            is_error = False
            is_fatal = False
            if default_msrun_sequence is not None:
                suggestion = f"Using the default sequence '{default_msrun_sequence.sequence_name}'."
                msrun_sequence = default_msrun_sequence
                is_fatal = self.validate
            else:
                nlt = "\n\t"
                suggestion = (
                    "Move the peak annotation files associated with these sequences:\n\t"
                    f"{nlt.join(msrun_sequence_names)}\ninto a directory containing all of their mzXML files (and "
                    f"don't forget to update path in the '{PeakAnnotationFilesLoader.DataHeaders.FILE}' column in the "
                    f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet)."
                )
                is_error = True
                is_fatal = True

            self.aggregated_errors_object.buffer_exception(
                MzxmlColocatedWithMultipleAnnot(
                    msrun_sequence_names,
                    nearest_annot_dir,
                    file=os.path.join(mzxml_dir, mzxml_filename),
                    suggestion=suggestion,
                ),
                is_error=is_error,
                is_fatal=is_fatal,
            )
        else:  # One was found
            # This intentionally might return None (but buffer an error)
            msrun_sequence = self.get_msrun_sequence(msrun_sequence_names[0])

        return msrun_sequence

    def get_msrun_sequence(self, name: Optional[str] = None) -> Optional[MSRunSequence]:
        """Retrieves an MSRunSequence record using either the value in the supplied SEQNAME column or via defaults for
        the Sequences sheet.

        The SEQMANE column is a comma-delimited string, which has the following values in this order:
        - Operator
        - LC Protocol Name
        - Instrument
        - Date (in the format YYYY-MM-DD)

        Args:
            name (string): Optional comma-delimited string from the SEQNAME column (if an infile/df was supplied.  If
                None, the default behavior is to use the Defaults file/sheet.
        Exceptions:
            Raises:
                None
            Buffers:
                RequiredColumnValue
                InfileError
        Returns:
            msrseq (Optional[MSRunSequence])
        """
        rec = None
        query_dict = {}
        missing_defaults = []
        # The origin of the sequence data used to retrieve the sequence
        origin = (
            "the default arguments: [operator, date, instrument, and lc_protocol_name]"
        )
        lookup_key = name if name is not None else "default"

        if lookup_key in self.msrun_sequence_dict.keys():
            # We have already computed the value for this search before, so just return it from the dict
            return self.msrun_sequence_dict[lookup_key]

        try:
            if name is not None:
                # If we have a name, that means that the value is from the data sheet (not the defaults file/sheet)
                # Record where any possible errors will come from for the catch below
                origin = "infile"
                error_source = self.friendly_file
                sheet = self.sheet
                column = self.DataHeaders.SEQNAME
                rownum = self.rownum

                (
                    operator,
                    lcprotname,
                    instrument,
                    date_str,
                ) = MSRunSequence.parse_sequence_name(name)

                date = string_to_date(
                    date_str,
                    # The following arguments are for error reporting
                    file=error_source,
                    sheet=sheet,
                    column=column,
                    rownum=rownum,
                )

                query_dict = {
                    "researcher": operator,
                    "date": date,
                    "lc_method__name": lcprotname,
                    "instrument": instrument,
                }
            else:
                if self.df is None:
                    # There is no Peak Annotation Details infile present
                    if self.defaults_file is not None:
                        origin = "defaultsfile"
                        error_source = self.defaults_file
                    else:
                        # Overloading the file variable to reference the defaults (to present in errors)
                        error_source = origin
                    sheet = None
                    column = None
                else:
                    if self.defaults_file is not None:
                        origin = "defaultsfile"
                        error_source = self.defaults_file
                        sheet = None
                        column = self.DefaultsHeaders.DEFAULT_VALUE
                    elif self.friendly_file is not None:
                        origin = "defaultsfile"  # Really, the sheet in the --infile, but that doesn't matter
                        error_source = self.friendly_file
                        sheet = self.defaults_sheet
                        column = self.DefaultsHeaders.DEFAULT_VALUE
                    else:
                        error_source = origin
                        sheet = None
                        column = None
                # We aren't processing rows here.  If there was a file, it was done by the sequences loader, so we never
                # have a row number.
                rownum = None

                if self.operator_default is not None:
                    query_dict["researcher"] = self.operator_default
                else:
                    missing_defaults.append(SequencesLoader.DataHeaders.OPERATOR)

                if self.lc_protocol_name_default is not None:
                    query_dict["lc_method__name"] = self.lc_protocol_name_default
                else:
                    missing_defaults.append(SequencesLoader.DataHeaders.LCNAME)

                if self.instrument_default is not None:
                    query_dict["instrument"] = self.instrument_default
                else:
                    missing_defaults.append(SequencesLoader.DataHeaders.INSTRUMENT)

                if self.date_default is not None:
                    query_dict["date"] = string_to_date(
                        self.date_default,
                        # The following arguments are for error reporting
                        file=error_source,
                        sheet=sheet,
                        column=column,
                        # We didn't save this when reading the defaults sheet, so we're going to name the row by the
                        # sequences loader's date column header
                        rownum=SequencesLoader.DataHeaders.DATE,
                    )
                else:
                    missing_defaults.append(SequencesLoader.DataHeaders.DATE)

            # Don't perform the query if no query exists (i.e. None will be returned)
            if len(query_dict.keys()) > 0:
                rec = MSRunSequence.objects.get(**query_dict)

        except MSRunSequence.DoesNotExist as dne:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    MSRunSequence,
                    query_dict,
                    file=error_source,
                    sheet=sheet,
                    column=column,
                    rownum=rownum,
                ),
                orig_exception=dne,
            )
        except MSRunSequence.MultipleObjectsReturned as mor:
            if len(missing_defaults) > 0 and origin != "infile":
                self.aggregated_errors_object.buffer_error(
                    RequiredColumnValues(
                        [
                            RequiredColumnValue(
                                file=error_source,
                                sheet=sheet,
                                column=column,
                                rownum=f"{self.DefaultsSheetName} {rowname}",
                            )
                            for rowname in missing_defaults
                        ]
                    ),
                    orig_exception=mor,
                )
            else:
                self.aggregated_errors_object.buffer_error(
                    MultipleRecordsReturned(
                        MSRunSequence,
                        query_dict,
                        file=error_source,
                        sheet=sheet,
                        column=column,
                        rownum=rownum,
                    ),
                    orig_exception=mor,
                )
        except InvalidMSRunName as isn:
            self.aggregated_errors_object.buffer_error(
                isn.set_formatted_message(
                    file=error_source,
                    sheet=sheet,
                    column=column,
                    rownum=rownum,
                )
            )
        except InfileError as ie:
            self.aggregated_errors_object.buffer_error(ie)
        except Exception as e:
            self.aggregated_errors_object.buffer_error(
                InfileError(
                    f"{type(e).__name__}: {e}",
                    file=error_source,
                    sheet=sheet,
                    column=column,
                    rownum=rownum,
                ),
                orig_exception=e,
            )
        finally:
            self.msrun_sequence_dict[lookup_key] = rec

        return rec

    def get_matching_mzxml_metadata(
        self,
        sample_name: str,
        sample_header: Optional[str],
        mzxml_path: Optional[str],
    ) -> Tuple[dict, bool]:
        """Identifies and retrieves the mzXML file (and metadata) that matches a row of the Peak Annotation Details
        sheet so that it is associated with the MSRunSample record belonging to the correct MSRunSequence.  To do this,
        it tries looking up the metadata by (in the following order of precedence): mzXML name, sample header, or sample
        name to match a set of files (each with that name).  It then optionally (if there is more than 1 file with the
        same name) matches the path of the mzXML file from the mzXML name column with the actual directory path of the
        actual files.

        It does not take into account previously loaded data, thus the only information for making the association
        between a sample, a peak annotation file, and a specific mzXML file is what we have during this load.  After the
        load, that information is gone, because we do not record (and cannot record) the sample header that suggests
        which mzXML file any peak data is derived from.  There is no link between the Peak Annotation Files and the
        mzXML files that were used to produce it.  Even if we had a path for a specific mzXML file during the load to
        know which mzXML goes with the Peak Annotation File and Sample, that is not saved.  See issue #1238 for more
        info.  That said...

        Note, the user may not have added a path to the mzXML column, and we don't want to have to force them to fill
        that out, since most times, the name is sufficient.  If no match is found, an error will be printed with the
        available (matching) paths (this will be all files with the same name if no path was supplied).  The user is
        then instructed to edit the mzXML name on the indicated row to include one of the displayed file paths.

        Uses self.mzxml_dict_by_header, which contains data parsed from mzXML files indexed by mzXML basename and
        directory.

        Args:
            sample_name (str): Name of a sample in the database.
            sample_header (Optional[str]): Value header string from a peak annotation file.
            mzxml_path (Optional[str]): Path and/or name of an mzXML file.
        Exceptions:
            None
        Returns:
            (dict): A single dict of mzXML metadata from self.mzxml_dict_by_header[mzxml basename][mzxml dir]
            (bool): Whether there were multiple matching mzXML files
        """
        mzxml_string_dir = ""
        mzxml_basename = ""
        mzxml_name = None
        multiple_mzxml_dict = None
        # Placeholder MSRunSample records have no polarity/mz/archivefile recs/file, but we do want to track if they've
        # been added or not in the load.
        placeholder_mzxml_metadata = {
            "polarity": None,
            "mz_min": None,
            "mz_max": None,
            "raw_file_name": None,
            "raw_file_sha1": None,
            "mzaf_record": None,
            "rawaf_record": None,
            "mzxml_dir": None,
            "mzxml_filename": None,
            "added": False,
            "sample_name": None,
        }

        # If we have an mzXML filename, that trumps any mzxml we might match using the sample header
        if mzxml_path is not None:
            mzxml_string_dir, mzxml_filename = os.path.split(mzxml_path)
            mzxml_basename = (os.path.splitext(mzxml_filename))[0]

            # BUG: This is getting the wrong multiple_mzxml_dict, or rather that dict does not include same-named mzxmls
            # BUG: in directories where the sample header differs (i.e. has been edited).  This issue is separate from
            # BUG: the inaccurate possible dupe samples issue, so I will fix this in a separate PR and delete this
            # BUG: comment.  The problem is that the keys in self.mzxml_dict_by_header are not the actual file name -
            # BUG: they are the sample data headers (or the best guess at what it is).  That's why this calls
            # BUG: make_sample_header_from_mzxml_name.
            # I need to be looking at self.mzxml_files.  That's just a list, so I'll need to figure out how to get the
            # metadata...
            mzxml_name = self.make_sample_header_from_mzxml_name(mzxml_filename)
            multiple_mzxml_dict = self.mzxml_dict_by_header.get(mzxml_name)

            # Identify the mzxml metadata by matching the mzxml path that was explicitly supplied in the peak annotation
            # details sheet.  If there is a match, return it.
            if multiple_mzxml_dict is not None and mzxml_string_dir != "":
                # Check for an exact match
                for dir in multiple_mzxml_dict.keys():
                    if len(multiple_mzxml_dict[dir]) == 1 and os.path.normpath(
                        mzxml_string_dir
                    ) == os.path.normpath(dir):
                        return multiple_mzxml_dict[dir][0], False

        # If we have a sample_header, that trumps any mzxml we might match using the sample name
        if multiple_mzxml_dict is None and sample_header is not None:
            multiple_mzxml_dict = self.mzxml_dict_by_header.get(sample_header)
            mzxml_name = str(sample_header)
            if mzxml_path is None:
                mzxml_basename = mzxml_name

        # As a last resort, we use the sample name itself
        if multiple_mzxml_dict is None:
            multiple_mzxml_dict = self.mzxml_dict_by_header.get(sample_name)
            mzxml_name = str(sample_name)
            if mzxml_path is None:
                mzxml_basename = mzxml_name

        # If we could not find an mzXML file('s metadata) that matches the provided input (e.g. taken from the infile),
        # return a dict with "Nones" as values (because this method is called from having read the infile and from going
        # through leftover files, and we need to track whether those leftover files have been already added or not).
        if multiple_mzxml_dict is None or len(multiple_mzxml_dict.keys()) == 0:
            return placeholder_mzxml_metadata, False

        # Now we have a multiple_mzxml_dict with potentially multiple mzXML files' metadata
        # If there's only 1, return it:
        if len(multiple_mzxml_dict.keys()) == 1:  # The keys are directories
            single_dir_key = list(multiple_mzxml_dict.keys())[0]
            single_dir_list_of_dicts = multiple_mzxml_dict[single_dir_key]
            if len(single_dir_list_of_dicts) == 1:
                return single_dir_list_of_dicts[0], False

        # Otherwise, we need to try and match it using the directory portion of the mzXML file reported in the infile.
        # We need to do this in order to match the sequence.  There may be multiple sequences in the infile.
        matches = []
        # For every directory key of metadata for files with the current (base)name
        for real_dir in multiple_mzxml_dict.keys():
            # If the directory parsed from the mzXML in the infile is a portion of the path of the real file
            if mzxml_string_dir in real_dir:
                # The number of matching metadata dicts is the number of ArchiveFile primary keys (with the same
                # name and directory) (this is mainly for the case that a directory path wasn't provided in the
                # mzXML name column in the infile)
                matches.extend(multiple_mzxml_dict[real_dir])

        if len(matches) > 1:
            match_files = [
                # Not using dr["mzaf_record"].file_location because Django appends a randomized hash string
                os.path.join(dr["mzxml_dir"], dr["mzxml_filename"])
                for dr in matches
            ]
            self.buffer_infile_exception(
                MzxmlSequenceUnknown(mzxml_basename, match_files),
                is_error=False,
                is_fatal=self.validate,
            )
            self.warned(MSRunSample.__name__)

            # Since there are multiple matching mzXML files to the sample in question, we must return mzxml_metadata
            # that can be used for a placeholder MSRunSample record that will be linked to by all peakgroups of this
            # sample.
            # NOTE: This is only relevant to MSRunSample records created by the calling method
            # 'get_or_create_msrun_sample_from_row'.  The method 'get_or_create_msrun_sample_from_mzxml' (called
            # in a loop from 'load_data') will create the leftover MSRunSample records in 'matches' that we are not
            # returning here.
            return placeholder_mzxml_metadata, True

        if len(matches) == 0:
            self.buffer_infile_exception(
                ValueError(
                    "Unable to find mzXML metadata that matches the values on this row of the peak annotation details "
                    f"sheet: sample_name: {sample_name} sample_header: {sample_header} mzxml_path: {mzxml_path}"
                ),
                is_error=True,
                is_fatal=True,
            )
            return placeholder_mzxml_metadata, True

        return matches[0], False

    # Deprecated
    # TODO: This method is flawed, but that's because we simply don't have enough information to identify which mzXML a
    # peakgroup should link to.  Before, this method was used because a sample header (which is reasonably assumed to
    # match the mzXML filename) is not saved anywhere in any of the models, and even if it was, the only association in
    # the PeakGroup record is the name of the peak annotation file, and there's no link between that file and its sample
    # headers, so if data on the same sample is loaded later, there's no way to know if it should be reassigned.  We
    # only know that during that load process, so if an mzXML file with the same name is loaded later, we don't really
    # know why a previously loaded PeakGroup was linked to any particular mzXML.  So there needs to be a refactor.
    def separate_placeholder_peak_groups(
        self,
        rec_dict,
        annot_name,
        rec,
    ):
        """Deprecated.  This method retrieves peakgroups from the provided MSRunSample record and separates them based
        on the peak annotation file name (annot_name) the dict is associated with compared with the one in the PeakGroup
        record.

        Note that technically (though not in practice), multiple different mzXML files for the same sample/sequence can
        be used in a single peak annotation file.  One peak group could originate from one such mzXML file and the other
        from another mzXML file, so mz_min and mz_max are used as an additional check, however, since polarity isn't
        saved in the peak data from the peak annotation file data, this is not perfect.  Thus, this method could
        associate some peak groups with the wrong mzXML file.  It's not perfect.

        It error-checks that the msrun_sequence and sample in both the rec_dict and the rec are the same.

        The basic idea is that the rec_dict represents a proposed/potential MSRunSample record that was found in a row
        of the Peak Annotation Details sheet and that the supplied rec is an MSRunSample record that is a placeholder
        that has various PeakGroup records linked to it, that may have originated from the mzXML file that is being
        added in the rec_dict.  This method compares the metadata in the PeakGroup's data with the annot_name and the
        metadata extracted from the mzXML file that populated the rec_dict.  While this metadata contains polarity and
        the polarity was available in the peak annotation file (inferred from the adductName of the PARENT), those
        values are/were not saved in the peak data.  So if the annot_name is the same and the medMZ is between mz_min
        and mz_max, then the peak group matches the new file.  The peak group records linked to the placeholder record
        are thus returned in 2 groups: matching and unmatching.

        Note that since scan ranges can overlap, this could also work with a non-placeholder record to shift peak groups
        from one to the other MSRunSample record.

        The record represented by rec_dict is assumed to be different from the supplied record.

        Args:
            rec_dict (dict of MSRunSample field values): Field value pairs of an MSRunSample record that may or may not
                exist in the database, WITH values for polarity, mz_min, and mz_max.
            annot_name (str): Peak annotation file name associated with the data that populated the rec_dict (i.e. the
                value from the Peak Annotation File column of the Peak Annotation Details sheet/file that the researcher
                has explicitly associated the mzXML, sample, sample header, and ms run sequence with.
            rec (MSRunSample): An MSRunSample record.  This is intended to be a placeholder record without an
                ms_data_file value, but either way, the metadata (ms_data_file, polarity, mz_min, and mz_max) in the
                record is ignored.
        Exceptions:
            None
        Returns:
            matching (QuerySet of PeakGroup records)
            unmatching (QuerySet of PeakGroup records)
        """
        # Do not associate any peak group records with the proposed record in rec_dict, if there is no associated
        # annot_name.
        if annot_name is None:
            return PeakGroup.objects.none(), rec.peak_groups.all()

        # Make sure that the sequence and sample are the same
        if rec_dict["sample"] != rec.sample:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    "separate_placeholder_peak_groups called with MSRunSample record data not from the same sample: "
                    f"{rec_dict['sample']} (ID: {rec_dict['sample'].id}) != {rec.sample} (ID: {rec.sample.id})"
                )
            )
            # All peak groups are unmatching
            return PeakGroup.objects.none(), rec.peak_groups.all()
        elif rec_dict["msrun_sequence"] != rec.msrun_sequence:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    "separate_placeholder_peak_groups called with MSRunSample record data not from the same "
                    f"MSRunSequence: {rec_dict['msrun_sequence']} (ID: {rec_dict['msrun_sequence'].id}) != "
                    f"{rec.msrun_sequence} (ID: {rec.msrun_sequence.id})"
                )
            )
            # All peak groups are unmatching
            return PeakGroup.objects.none(), rec.peak_groups.all()

        # Annotate the peak groups with the min and max med_mz of its peak_data
        qs = PeakGroup.objects.annotate(
            min_med_mz=Min("peak_data__med_mz", default=None),
            max_med_mz=Max("peak_data__med_mz", default=None),
        )

        # Filtering criteria
        matching_q_exp = (
            Q(peak_annotation_file__filename=annot_name)
            & Q(Q(min_med_mz__isnull=True) | Q(min_med_mz__gte=rec_dict["mz_min"]))
            & Q(Q(max_med_mz__isnull=True) | Q(max_med_mz__lte=rec_dict["mz_max"]))
        )

        # Generate the matching and unmatching PeakGroup QuerySets
        matching_qs = qs.filter(matching_q_exp)
        unmatching_qs = qs.filter(~matching_q_exp)

        # Clear out the annotations so that .update can be called on the querysets
        matching_qs.query.annotations.clear()
        unmatching_qs.query.annotations.clear()

        return matching_qs, unmatching_qs

    @classmethod
    def parse_mzxml(cls, mzxml_path, full_dict=False):
        """Creates a dict of select data parsed from an mzXML file

        This extracts the raw file name, raw file's sha1, and the polarity from an mzxml file and returns a condensed
        dictionary of only those values (for simplicity).  The construction of the condensed dict will perform
        validation and conversion of the desired values, which will not occur when the full_dict is requested.  If
        full_dict is True, it will return the uncondensed version.

        If not all polarities of all the scans are the same, an error will be buffered.

        Args:
            mzxml_path (str or Path): mzXML file path
            full_dict (boolean): Whether to return the raw/full dict of the mzXML file
        Exceptions:
            Raises:
                FileNotFoundError
                NoScans
            Buffers:
                MixedPolarityErrors
                ValueError
                MzxmlParseError
        Returns:
            If mzxml_path is not a real existing file:
                None
            If full_dict=False:
                {
                    "raw_file_name": <raw file base name parsed from mzXML file>,
                    "raw_file_sha1": <sha1 string parsed from mzXML file>,
                    "polarity": "positive" or "negative" (based on first polarity parsed from mzXML file),
                    "mz_min": <float parsed from lowMz from the mzXML file>,
                    "mz_max": <float parsed from highMz from the mzXML file>,
                }
            If full_dict=True:
                xmltodict.parse(xml_content)
        """
        raw_file_name = None
        raw_file_sha1 = None
        polarity = None
        mz_min = None
        mz_max = None

        # Assume Path object
        mzxml_path_obj = mzxml_path
        if isinstance(mzxml_path, str):
            mzxml_path_obj = Path(mzxml_path)

        if not mzxml_path_obj.is_file():
            # mzXML files are optional, but the file names are supplied in a file, in which case, we may have a name,
            # but not the file, so just return None if what we have isn't a real file.
            raise FileNotFoundError(f"File not found: {mzxml_path}")

        # Parse the xml content
        with mzxml_path_obj.open(mode="r") as f:
            xml_content = f.read()
        mzxml_dict = xmltodict.parse(xml_content)

        if "scan" not in mzxml_dict["mzXML"]["msRun"].keys():
            raise NoScans(mzxml_path)

        # In order to use this as a class method, we will buffer the errors in a one-off AggregatedErrors object
        errs_buffer = AggregatedErrors()

        if full_dict:
            return mzxml_dict, errs_buffer

        try:
            raw_file_type = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileType"]
            raw_file_name = Path(
                mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileName"]
            ).name
            raw_file_sha1 = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileSha1"]
            if raw_file_type != "RAWData":
                errs_buffer.buffer_error(
                    ValueError(
                        f"Unsupported file type [{raw_file_type}] encountered in mzXML file [{str(mzxml_path_obj)}].  "
                        "Expected: [RAWData]."
                    )
                )
                raw_file_name = None
                raw_file_sha1 = None

            symbol_polarity = ""
            mixed_polarities = {}
            # mzXML files can have 0 scans
            if "scan" in mzxml_dict["mzXML"]["msRun"].keys():
                for entry_dict in mzxml_dict["mzXML"]["msRun"]["scan"]:
                    # Parse the mz_min
                    tmp_mz_min = float(entry_dict["@lowMz"])
                    # Get the min of the mins
                    if mz_min is None or tmp_mz_min < mz_min:
                        mz_min = tmp_mz_min
                    # Parse the mz_max
                    tmp_mz_max = float(entry_dict["@highMz"])
                    # Get the max of the maxes
                    if mz_max is None or tmp_mz_max > mz_max:
                        mz_max = tmp_mz_max
                    # Parse the polarity
                    # If we haven't run into a polarity conflict (yet)
                    if str(mzxml_path_obj) not in mixed_polarities.keys():
                        if symbol_polarity == "":
                            symbol_polarity = entry_dict["@polarity"]
                        elif symbol_polarity != entry_dict["@polarity"]:
                            mixed_polarities[str(mzxml_path_obj)] = {
                                "first": symbol_polarity,
                                "different": entry_dict["@polarity"],
                                "scan": entry_dict["@num"],
                            }

                if len(mixed_polarities.keys()) > 0:
                    errs_buffer.buffer_exception(
                        MixedPolarityErrors(mixed_polarities),
                    )

                if symbol_polarity == "+":
                    polarity = MSRunSample.POSITIVE_POLARITY
                elif symbol_polarity == "-":
                    polarity = MSRunSample.NEGATIVE_POLARITY
                elif symbol_polarity != "":
                    errs_buffer.buffer_error(
                        ValueError(
                            f"Unsupported polarity value [{symbol_polarity}] encountered in mzXML file "
                            f"[{str(mzxml_path_obj)}]."
                        )
                    )

        except KeyError as ke:
            errs_buffer.buffer_error(
                MzxmlParseError(
                    f"Missing key [{ke}] encountered in mzXML file [{str(mzxml_path_obj)}]."
                ).with_traceback(ke.__traceback__)
            )

        return {
            "raw_file_name": raw_file_name,
            "raw_file_sha1": raw_file_sha1,
            "polarity": polarity,
            "mz_min": mz_min,
            "mz_max": mz_max,
        }, errs_buffer

    def unpaired_mzxml_files_exist(self):
        """Traverse self.mzxml_dict_by_header and return True if any mzXML files have not yet been added to an
        MSRunSample record (meaning, it was not listed in the Peak Annotation Details sheet/file or there were multiple
        files with the same name, not all paths were provided in the input file, and thus, the file could not be paired
        with a row).

        This method exists in order to avoid errors when trying to retrieve default values, if they are not needed, e.g.
        when the infile is complete and all mzXMLs were included in it.

        Args:
            None
        Exceptions:
            None
        Returns
            boolean
        """
        for mzxml_name in self.mzxml_dict_by_header.keys():
            for mzxml_dir in self.mzxml_dict_by_header[mzxml_name].keys():
                for mzxml_metadata in self.mzxml_dict_by_header[mzxml_name][mzxml_dir]:
                    if mzxml_metadata["added"] is False and (
                        # TODO: Also check if a skip exists without the directory having been added.
                        mzxml_name not in self.skip_msrunsample_by_mzxml.keys()
                        or mzxml_dir
                        not in self.skip_msrunsample_by_mzxml[mzxml_name].keys()
                    ):
                        return True
        return False

    @classmethod
    def get_scan_pattern(cls, scan_patterns=None, add_patterns=True):
        r"""Create a regular expression that can be used to strip scan identifiers from a sample header.

        NOTE: Each pattern is prepended and appended with:
            r"[\-_]"  # dash or underscore (also removed)
            r"(?=[\-_]|$)"  # followed by dash or underscore or end of string (not removed)

        Args:
            scan_patterns (list of regular expression strings)
            add_patterns (boolean): Whether to add the supplied patterns to the defaults or replace them.
        Exceptions:
            None
        Returns:
            pattern (compiled re)
        """
        delim = cls.DEFAULT_SCAN_DELIM_PATTERN
        scan_labels = cls.DEFAULT_SCAN_LABEL_PATTERNS

        pre_pat = delim
        post_pat = r"(?=" + delim + r"|$)"

        if scan_patterns is not None:
            if add_patterns:
                scan_labels.extend(scan_patterns)
            else:
                scan_labels = scan_patterns

        # Examples, if scan_patterns = ["pos", "neg", "scan[0-9]+"]:
        #   "sample3_pos_scan25" -> "sample3"
        #   "sample5-neg-mouse2" -> "sample5-mouse2"
        return re.compile(
            r"(" + "|".join([pre_pat + pat + post_pat for pat in scan_labels]) + r")+"
        )

    @classmethod
    def guess_sample_name(cls, mzxml_basename, scan_patterns=None, add_patterns=True):
        """Strips scan labels from an accucor/isocorr sample header (or mzXML file basename) using
        self.DEFAULT_SAMPLE_HEADER_SUFFIXES and/or the supplied scan labels.  The result is usually the name of the
        sample as it appears in the database.

        Use caution.  This doesn't guarantee the resulting sample name is accurate.  If the resulting sample name is not
        unique, you may end up with conflict errors at some later point in the processing of a study submission.  To
        resolve this, you must add the header/mzXML to the input file associated with the correct database sample name
        (containing any required prefix).

        Args:
            mzxml_bamename (string): The basename of an mzXML file (or accucor/isocorr sample header).
            scan_patterns (list of regular expression strings) [cls.DEFAULT_SAMPLE_HEADER_SUFFIXES]: E.g. "_pos".
            add_patterns (boolean): Whether to add the supplied patterns to the defaults or replace them.
        Exceptions:
            None
        Returns:
            guessed_sample_name (string)
        """
        pattern = cls.get_scan_pattern(
            scan_patterns=scan_patterns, add_patterns=add_patterns
        )
        return re.sub(pattern, "", mzxml_basename)

    def clean_up_created_mzxmls_in_archive(self):
        """Call this method when rollback did/will happen in order to delete mzXML files added to the archive on disk.

        Args:
            None
        Exceptions:
            Buffers:
                NotImplementedError
                OSError
            Raises:
                None
        Returns:
            delstats (Dict[str, list]): Lists of deleted, skipped, and errored file deletions.
        """
        delstats = {
            "deleted": [],
            "failures": [],
            "skipped": [],
        }

        if not self.aggregated_errors_object.should_raise():
            self.aggregated_errors_object.buffer_error(
                NotImplementedError(
                    "clean_up_created_mzxmls_in_archive is not intended for use when an exception has not been raised."
                )
            )
            return delstats

        for rec in self.created_mzxml_archive_file_recs:
            # If there was no associated file, or the file wasn't actually created, there's nothing to delete, so
            # continue
            if not rec.file_location or not os.path.isfile(rec.file_location.path):
                delstats["skipped"].append(rec.file_location.path)
                continue

            try:
                os.remove(rec.file_location.path)
                print(
                    f"DELETED (due to rollback): {rec.file_location.path}", flush=True
                )
                delstats["deleted"].append(rec.file_location.path)
            except Exception as e:
                self.aggregated_errors_object.buffer_error(
                    OSError(
                        f"Unable to delete created mzXML archive file: [{rec.file_location.path}] during "
                        f"rollback due to {type(e).__name__}: {e}"
                    ),
                    orig_exception=e,
                )
                delstats["failures"].append(rec.file_location.path)

        if len(self.created_mzxml_archive_file_recs) == 0:
            print(
                "No archived mzXML files to clean up from the disk archive.",
                flush=True,
            )
        else:
            print(
                (
                    f"mzXML file rollback disk archive clean up stats: {len(delstats['deleted'])} deleted, "
                    f"{len(delstats['failures'])} failed to be deleted, and {len(delstats['skipped'])} expected files "
                    "did not exist."
                ),
                flush=True,
            )

        return delstats

    def make_sample_header_from_mzxml_name(self, mzxml_name: str):
        """This turns an mzxml file- or base-name into a sample header.  Uses self.exact_mode to decide whether to
        replace dashes with underscores.

        Args:
            mzxml_filename (str): Name of an mzXML file, but can be a sample header, basename, etc.
        Exceptions:
            None
        Returns:
            sample_header (str)
        """
        sample_header: str
        # In case they passed in a file path
        mzxml_nopath = os.path.basename(mzxml_name)
        # In case they passed in a file name
        sample_header, _ = os.path.splitext(mzxml_nopath)
        if not self.exact_mode:
            sample_header = sample_header.replace("-", "_")
        return sample_header

    @classmethod
    def get_mzxml_files(cls, files=None, dir=None):
        """Return a list of mzXML files.

        If files is not None, it just returns that list.  Otherwise, if dir is None, return an empty list,
        otherwise, return all files ending with '.mzxml' (case insensitive).

        Args:
            files (Optional[List[str]]): A list of mzXML files, intended to be used by a command line option for
                files (mutually exclusive with a root directory option).
            dir (Optional[str]): A directory under which mzXML files reside (in subdirectories).
        Exceptions:
            None
        Returns:
            (List[str]): A list of file paths.
        """
        if files is not None:
            return files
        if dir is None:
            return []
        if dir == "":
            dir = os.getcwd()
        return [
            os.path.join(p, fl)
            for p, _, fs in os.walk(dir)
            for fl in fs
            if fl.lower().endswith(".mzxml")
        ]
