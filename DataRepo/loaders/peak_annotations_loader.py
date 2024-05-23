from abc import ABC, abstractmethod
from collections import namedtuple
from sqlite3 import ProgrammingError
from typing import Dict, List, Optional, Tuple

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction

from DataRepo.loaders.base.converted_table_loader import ConvertedTableLoader
from DataRepo.loaders.base.table_column import TableColumn
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models import (
    ArchiveFile,
    Compound,
    DataFormat,
    DataType,
    MSRunSample,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Sample,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredArgs,
    HeaderAsSampleDoesNotExist,
    MissingCompound,
    NoTracerLabeledElements,
    ObservedIsotopeParsingError,
    RecordDoesNotExist,
    RollbackException,
    generate_file_location_string,
)
from DataRepo.utils.file_utils import is_excel, string_to_datetime
from DataRepo.utils.infusate_name_parser import parse_isotope_label


class PeakAnnotationsLoader(ConvertedTableLoader, ABC):
    @property
    @abstractmethod
    def format_code(self) -> str:
        """The DataFormat.code for the peak annotation file"""
        pass

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    MEDMZ_KEY = "MEDMZ"
    MEDRT_KEY = "MEDRT"
    ISOTOPELABEL_KEY = "ISOTOPELABEL"
    FORMULA_KEY = "FORMULA"
    COMPOUND_KEY = "COMPOUND"
    SAMPLEHEADER_KEY = "SAMPLEHEADER"
    CORRECTED_KEY = "CORRECTED"
    RAW_KEY = "RAW"

    DataSheetName = "Peak Annotations"  # The official sole sheet name of the converted/merged peak annotation data

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "SAMPLEHEADER",
            "COMPOUND",
            "FORMULA",
            "ISOTOPELABEL",
            "MEDMZ",
            "MEDRT",
            "RAW",
            "CORRECTED",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        MEDMZ="MedMz",
        MEDRT="MedRt",
        ISOTOPELABEL="IsotopeLabel",
        FORMULA="Formula",
        COMPOUND="Compound",
        SAMPLEHEADER="mzXML Name",
        RAW="Raw Abundance",
        CORRECTED="Corrected Abundance",
    )

    # List of required header keys
    DataRequiredHeaders = [
        MEDMZ_KEY,
        MEDRT_KEY,
        ISOTOPELABEL_KEY,
        FORMULA_KEY,
        COMPOUND_KEY,
        SAMPLEHEADER_KEY,
        CORRECTED_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        MEDMZ_KEY: float,
        MEDRT_KEY: float,
        ISOTOPELABEL_KEY: str,
        FORMULA_KEY: str,
        COMPOUND_KEY: str,
        SAMPLEHEADER_KEY: str,
        RAW_KEY: float,
        CORRECTED_KEY: float,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [SAMPLEHEADER_KEY, COMPOUND_KEY, ISOTOPELABEL_KEY],
    ]

    # A mapping of database field to column.  Only set when 1 field maps to 1 column.  Omit others.
    # NOTE: The sample headers are always different, so we cannot map those here
    FieldToDataHeaderKey = {
        PeakGroup.__name__: {
            "name": COMPOUND_KEY,
            "formula": FORMULA_KEY,
        },
        PeakGroupLabel.__name__: {
            "element": ISOTOPELABEL_KEY,
        },
        PeakData.__name__: {
            "med_mz": MEDMZ_KEY,
            "med_rt": MEDRT_KEY,
            "raw_abundance": RAW_KEY,
            "corrected_abundance": CORRECTED_KEY,
        },
        PeakDataLabel.__name__: {
            "element": ISOTOPELABEL_KEY,
            "count": ISOTOPELABEL_KEY,
            "mass_number": ISOTOPELABEL_KEY,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        MEDMZ=TableColumn.init_flat(name=DataHeaders.MEDMZ, field=PeakData.med_mz),
        MEDRT=TableColumn.init_flat(name=DataHeaders.MEDRT, field=PeakData.med_rt),
        RAW=TableColumn.init_flat(name=DataHeaders.RAW, field=PeakData.raw_abundance),
        CORRECTED=TableColumn.init_flat(
            name=DataHeaders.RAW, field=PeakData.corrected_abundance
        ),
        FORMULA=TableColumn.init_flat(
            name=DataHeaders.FORMULA, field=PeakGroup.formula
        ),
        COMPOUND=TableColumn.init_flat(
            name=DataHeaders.COMPOUND,
            field=PeakGroup.name,
            guidance=(
                "One or more names of compounds with the same formula can be specified.  Must match a compound name or "
                "synonym in the Compounds sheet/file.  If a synonym is specified, the compound's primary name will be "
                "substituted for the peak group name."
            ),
            format=(
                "Forward-slash (/) delimited compound names, (e.g. 'citrate/isocitrate').  Order does not matter - "
                "they will be alphanumerically re-ordered upon insert into the database."
            ),
            # TODO: Might be able to add dynamic choices here if it can be based on joining compounds with the same
            # formula
        ),
        SAMPLEHEADER=TableColumn.init_flat(
            name=DataHeaders.SAMPLEHEADER,
            type=str,
            help_text=(
                "The name of the mzXML file (without the path or extension).  Also known as the 'sample header' (or "
                "'sample data header')."
            ),
            header_required=True,
            value_required=True,
        ),
        ISOTOPELABEL=TableColumn.init_flat(
            name=DataHeaders.ISOTOPELABEL,
            type=str,
            help_text=(
                "A formatted string describing the labeled elements identified in the compound in this specific peak."
            ),
            format=(
                "Either 'C12 PARENT' for unlabeled compounds or a formatted string describing the element(s), mass "
                "number(s), and label counts, e.g. 'C13N15-label-3-2' which contains 3 Carbons with a mass number of "
                "13 and 2 Nitrogens with a mass number of 15."
            ),
            header_required=True,
            value_required=True,
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [ArchiveFile, PeakData, PeakDataLabel, PeakGroup, PeakGroupLabel]

    CompoundNamesDelimiter = "/"

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
                data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                *file (Optional[str]) [None]: File name (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
                extra_headers (Optional[List[str]]): Use for dynamic headers (different in every file).  To allow any
                    unknown header, supply an empty list.
            Derived (this) class Args:
                peak_annotation_details_file (Optional[str]): The name of the file that the Peak Annotation Details came
                    from.
                peak_annotation_details_sheet (Optional[str]): The name of the sheet that the Peak Annotation Details
                    came from (if it was an excel file).
                peak_annotation_details_df (Optional[pandas DataFrame]): The DataFrame of the Peak Annotation Details
                    sheet/file that will be supplied to the MSRunsLoader class (that is an instance meber of this
                    instance)
                operator (Optional[str]): The researcher who ran the mass spec.  Mutually exclusive with defaults_df
                    (when it has a default for the operator column for the Sequences sheet).
                lc_protocol_name (Optional[str]): Name of the liquid chromatography method.  Mutually exclusive with
                    defaults_df (when it has a default for the lc_protocol_name column for the Sequences sheet).
                instrument (Optional[str]): Name of the mass spec instrument.  Mutually exclusive with defaults_df
                    (when it has a default for the instrument column for the Sequences sheet).
                date (Optional[str]): Date the Mass spec instrument was run.  Format: YYYY-MM-DD.  Mutually exclusive
                    with defaults_df (when it has a default for the date column for the Sequences sheet).
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        # These custom options are for the member instance of MSRunsLoader's file, sheet, and df arguments.
        # It will also take some arguments that overlap with this class's members (defaults_file, defaults_df, and
        # MSRunsLoader's custom options: operator, date, lc protocol name, and instrument).
        self.peak_annotation_details_file = kwargs.pop(
            "peak_annotation_details_file", None
        )
        self.peak_annotation_details_sheet = kwargs.pop(
            "peak_annotation_details_sheet", None
        )
        self.peak_annotation_details_df = kwargs.pop("peak_annotation_details_df", None)

        # Require the file argument if df is supplied
        if kwargs.get("df") is not None:
            if kwargs.get("file") is None:
                raise AggregatedErrors().buffer_error(
                    ConditionallyRequiredArgs(
                        "The [file] argument is required is the [df] argument is supplied."
                    )
                )

        # Error tracking
        # If the sample that a header maps to is missing, track it so we don't raise that error multiple times
        self.missing_headers_as_samples = []

        # We are going to use defaults as processed by the MSRunsLoader (which uses the SequencesLoader) in  order to be
        # able to obtain the correct MSRunSample record that each PeakGroup belongs to
        self.msrunsloader = MSRunsLoader(
            file=self.peak_annotation_details_file,
            data_sheet=self.peak_annotation_details_sheet,
            df=self.peak_annotation_details_df,
            defaults_df=kwargs.get("defaults_df"),
            defaults_file=kwargs.get("defaults_file"),
            operator=kwargs.pop("operator", None),
            date=kwargs.pop("date", None),
            lc_protocol_name=kwargs.pop("lc_protocol_name", None),
            instrument=kwargs.pop("instrument", None),
        )

        # Cannot call super().__init__() because ABC.__init__() takes a custom argument
        ConvertedTableLoader.__init__(self, *args, **kwargs)

        # Initialize the default sequence data (obtained from self.msrunsloader)
        self.operator_default = None
        self.date_default = None
        self.lc_protocol_name_default = None
        self.instrument_default = None
        self.msrun_sample_dict = {}
        self.initialize_sequence_defaults()

        # For referencing the compounds sheet in errors about missing compounds
        if self.peak_annotation_details_file is None:
            self.compounds_loc = generate_file_location_string(
                file="the study excel file",
                sheet=CompoundsLoader.DataSheetName,
            )
        elif is_excel(self.peak_annotation_details_file):
            self.compounds_loc = generate_file_location_string(
                file=self.peak_annotation_details_file,
                sheet=CompoundsLoader.DataSheetName,
            )
        else:
            self.compounds_loc = generate_file_location_string(
                file=self.peak_annotation_details_file,
            )

    def initialize_sequence_defaults(self):
        """Initializes the msrun_sample_dict (a dict of MSRunSample records keyed on sample header), if a PeakAnnotation
        Details dataframe was provided.  It also initializes the default values for the sequence, for use in filling in
        sequence data in the Peak Annotation Details sheet, or later, if a Peak Annotation Details sheet was not
        provided, to use in the search of MSRunSample to find the sample header (assuming it matches the name of the
        sample exactly).

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # Peak annotation details are optional if the operator, date, lc name, and instrument are provided AND the
        # sample headers match the database sample names (in which case, using the MSRunsLoader instance just serves
        # to process the default arguments).
        self.msrun_sample_dict = {}
        if self.peak_annotation_details_df is not None:
            self.msrun_sample_dict = self.msrunsloader.get_loaded_msrun_sample_dict(
                peak_annot_file=self.file
            )

        # TODO: Figure out a better way to handle buffered exceptions from another class that are only raised from a
        # specific method, so that methods raise them as a group instead of needing to incorporate instance loaders like
        # this for buffered errors
        self.aggregated_errors_object.merge_aggregated_errors_object(
            self.msrunsloader.aggregated_errors_object
        )

        # Set the MSRunSequence defaults as a fallback in case a peak annotation details file was not provided
        self.operator_default = self.msrunsloader.operator_default
        if self.msrunsloader.date_default is not None:
            self.date_default = string_to_datetime(self.msrunsloader.date_default)
        self.lc_protocol_name_default = self.msrunsloader.lc_protocol_name_default
        self.instrument_default = self.msrunsloader.instrument_default

    # TODO: Yet to be done:
    # DupeCompoundIsotopeCombos,
    #     - This catches instances of duplicate compound/isotopeLabel combos
    #       It is now handled via the unique constraint on sample/compound/isotope at the file level, but those unique
    #       constraint errors should be repackaged to remove the sample (because it will always be all samples affected)
    #     - In fact, I should see if I can make sure that all InfileErrors get converted to the cell locations in the
    #       original file?
    # ADD THESE:
    #     MissingCompounds,
    #     MissingSamplesError,
    #     NoSampleHeaders,
    #     NoSamplesError,
    #     PeakAnnotFileMismatches,
    #     ResearcherNotNew,
    #     SampleColumnInconsistency,
    #     TracerLabeledElementNotFound,
    #     UnexpectedIsotopes,
    #         When more labeled elements than in the tracers
    #     UnskippedBlanksError,
    #     IsotopeStringDupe,
    #         # If there are multiple isotope measurements that match the same parent tracer labeled element
    #         # E.g. C13N15C13-label-2-1-1 would match C13 twice
    #     EmptyColumnsError,
    #         Add this to TableLoader
    #         for k, _ in corr_iter.items():
    #             if k.startswith("Unnamed: "):
    #                 self.aggregated_errors_object.buffer_error(
    #                     EmptyColumnsError...
    # tests
    #     ConvertedTableLoader
    #         check_output_dataframe
    #         condense_columns
    #         revert_headers
    #         initialize_merge_dict
    #         get_required_sheets
    #     PeakAnnotationsLoader (AccucorLoader/IsocorrLoader)
    #         initialize_sequence_defaults
    #         load_data
    #         get_or_create_annot_file
    #         get_or_create_peak_group
    #         get_msrun_sample
    #         get_or_create_peak_data
    #         get_or_create_labels
    #         get_or_create_peak_group_label
    #         get_or_create_peak_data_label

    def load_data(self):
        """Loads the ArchiveFile, PeakGroup, PeakGroupLabel, PeakData, and PeakDataLabel tables from the dataframe.
        Args:
            None
        Raises:
            None
        Returns:
            None
        """
        try:
            annot_file_rec, _ = self.get_or_create_annot_file()
        except RollbackException:
            # We will continue the processing below to essentially generate the skip counts.
            # None of the following method calls will actually attempt to create records.  They will balk at the None-
            # valued arguments.
            annot_file_rec = None
            pass

        for _, row in self.df.iterrows():
            pgrec = None
            pdrec = None

            # Get matching compounds
            pgname, cmpd_recs = self.get_peak_group_name_and_compounds(row)

            # Get or create a PeakGroup record
            try:
                pgrec, _ = self.get_or_create_peak_group(row, annot_file_rec, pgname)
            except RollbackException:
                pass

            # Get or create a linking table record between pgrec and each cmpd_rec (compounds with the same formula)
            for cmpd_rec in cmpd_recs:
                try:
                    self.get_or_create_peak_group_compound_link(pgrec, cmpd_rec)
                except RollbackException:
                    pass

            # Get or create a PeakData record
            try:
                pdrec, _ = self.get_or_create_peak_data(row, pgrec)
            except RollbackException:
                pass

            # Check the elements of the PeakGroup's compound to ensure it shares elements with the ones labeled in the
            # tracers
            if pgrec is not None and len(pgrec.peak_labeled_elements) == 0:
                self.aggregated_errors_object.buffer_error(
                    NoTracerLabeledElements(pgrec.name, pgrec.tracer_labeled_elements)
                )
                # So this shouldn't happen.  If it does, it is user error.  Every peak group compound should have
                # elements that are labeled in at least 1 of the tracers.  We are technically skipping 0 record
                # retrievals/creations here, but we will count 1 skip for each record type, for good measure.
                self.skipped(PeakGroupLabel.__name__)
                self.skipped(PeakDataLabel.__name__)
                continue

            # Get or create a PeakGroupLabel and PeakDataLabel records
            self.get_or_create_labels(row, pdrec, pgrec)

    @transaction.atomic
    def get_or_create_annot_file(self):
        """Gets or creates an ArchiveFile record from self.file.

        Args:
            None
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
        # Get or create the ArchiveFile record for the mzXML
        try:
            rec_dict = {
                # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "is_binary": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "file_location": self.file,  # Intentionally a string and not a File object
                "data_type": DataType.objects.get(code="ms_peak_annotation"),
                "data_format": DataFormat.objects.get(code=self.format_code),
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

    def get_peak_group_name_and_compounds(self, row):
        """Retrieve the peak group name and compound records.

        Args:
            row (pandas.Series)
        Exceptions:
            None
        Returns:
            pgname (str)
            recs (Optional[List[Compound]])
        """
        names_str = self.get_row_val(row, self.headers.COMPOUND)
        names = names_str.split(self.CompoundNamesDelimiter)
        recs = None

        for name_str in names:
            name = name_str.strip()
            try:
                recs.append(Compound.compound_matching_name_or_synonym(name))
            except (ValidationError, ObjectDoesNotExist) as cmpderr:
                orig_col, orig_sheet = self.original_column_lookup(
                    self.headers.COMPOUND
                )
                self.aggregated_errors_object.buffer_error(
                    # TODO: Consolidate all MissingCompound exceptions into a MissingCompounds exception
                    MissingCompound(
                        name=name,
                        query_obj=Compound.get_name_query_expression(name),
                        compounds_loc=self.compounds_loc,
                        file=self.file,
                        sheet=orig_sheet,
                        column=orig_col,
                    ),
                    orig_exception=cmpderr,
                )

        pgname = None
        if recs is not None:
            # Set the peak group name to the sorted primary compound names, delimited by "/"
            pgname = self.CompoundNamesDelimiter.join(
                [r.name for r in sorted(recs, key=lambda r: r.name)]
            )

        return pgname, recs

    @transaction.atomic
    def get_or_create_peak_group(self, row, peak_annot_file, pgname):
        """Get or create a PeakGroup Record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_annot_file (Optional[ArchiveFile]): The ArchiveFile record for self.file
            pgname (Optional[str]): A slash-delimited string of sorted primary compound names.
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakGroup])
            created (boolean)
        """
        msrun_sample = self.get_msrun_sample(row)
        formula = self.get_row_val(row, self.headers.FORMULA)

        if (
            msrun_sample is None
            or pgname is None
            or peak_annot_file is None
            or self.is_skip_row()
        ):
            self.skipped(PeakGroup.__name__)
            return None, False

        rec_dict = {
            "msrun_sample": msrun_sample,
            "name": pgname,
            "formula": formula,
            "peak_annotation_file": peak_annot_file,
        }

        try:
            rec, created = PeakGroup.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakGroup.__name__)
            else:
                self.existed(PeakGroup.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakGroup, rec_dict)
            self.errored(PeakGroup.__name__)
            raise RollbackException()

        return rec, created

    def get_msrun_sample(self, row):
        """Retrieves the MSRunSample record, either as determined by the self.msrun_sample_dict that was returned by the
        member MSRunsLoader object (via the peak Annotation Details sheet) or by assuming that the sample header matches
        the sample name exactly and using the sequence defaults.

        In the latter case, it first tries to use just the sample name.  If it matches and exists in only a single
        MSRunSample record, it is enough.  If there are multiple, it tries to use the sequence defaults that were either
        provided in the defaults sheet/file or via the command line.  There still could theoretically be multiple
        matches if there are multiple mzXML files all with the same name, but in those cases, the headers must differ to
        be unique and the database sample name cannot match the header, in which case, you would need a Peak Annotation
        Details sheet/file.

        Args:
            row (pandas.Series)
        Exceptions:
            Buffers:
                HeaderAsSampleDoesNotExist
                RecordDoesNotExist
                ConditionallyRequiredArgs
                ProgrammingError
            Raises:
                None
        Returns:
            msrun_sample (Optional[MSRunSample])
        """
        sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)

        # If we already know (from a previous row, that) the sample the header maps to is missing in the database
        if sample_header in self.missing_headers_as_samples:
            return None

        # There are 2 ways we can use to obtain the MSRunSample Record
        # 1. The first way we will try to obtain the MSRunSample record is using the data provided in the Peak
        #    Annotation Details sheet, if one was provided.  That data was obtained in the initialize_sequence_defaults
        #    method (called by the constructor).
        if (
            sample_header in self.msrun_sample_dict.keys()
            and self.msrun_sample_dict[sample_header] is not None
        ):
            return self.msrun_sample_dict[sample_header]

        # 2. The second way (if a Peak Annotation Details sheet was not provided, or doesn't list a value for this
        #    sample header) is to start searching using the sample header to look for an exact matching sample name.  If
        #    there is more than 1 match, we can try to whittle it down using what we've been provided in the way of the
        #    default sequence data.

        # First, we will check the Sample table directly, so we can report the most relevant error if it's missing
        samples = Sample.objects.filter(name=sample_header)
        if samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                HeaderAsSampleDoesNotExist(
                    sample_header,
                    suggestion=(
                        f"Please add a row to {MSRunsLoader.DataSheetName} that matches the sample header to a "
                        "TraceBase sample name."
                    ),
                    file=self.file,
                )
            )
            self.missing_headers_as_samples.append(sample_header)
            return None
        sample = samples.get()

        # Now try to get the MSRunSample record using the sample
        msrun_samples = MSRunSample.objects.filter(sample__pk=sample.pk)

        # Check if there were too few or exactly 1 results.
        if msrun_samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(MSRunSample, {"sample__name": sample_header})
            )
            return None
        elif msrun_samples.count() == 1:
            return msrun_samples.get()

        # At this point, there are multiple results.  That means that defaults for the sequence are required in order to
        # proceed, so check them.
        if (
            self.operator_default is None
            and self.date_default is None
            and self.lc_protocol_name_default is None
            and self.instrument_default is None
        ):
            # Only buffer this error once
            if not self.aggregated_errors_object.exception_type_exists(
                ConditionallyRequiredArgs
            ):
                self.aggregated_errors_object.buffer_error(
                    ConditionallyRequiredArgs(
                        "The following arguments supplied to the constructor were insufficient.  Either "
                        "peak_annotation_details_df wasn't supplied or did not have enough information for every "
                        "sample column, in which case, enough of the following default arguments are required to match "
                        "each sample header with the already loaded MSRunSample records: [operator, lc_protocol_name, "
                        "instrument, and/or date."
                    )
                )
            return None

        # Let's see if the results can be narrowed to a single record using the sequence defaults we've been provided.

        # First, build a query dict with only the sequence defaults we have values for (NOTE: just the researcher or
        # date, for example, may be enough).
        query_dict = {}
        if self.operator_default is not None:
            query_dict["msrun_sequence__researcher"] = self.operator_default
        if self.lc_protocol_name_default is not None:
            query_dict["msrun_sequence__lc_method__name"] = (
                self.lc_protocol_name_default
            )
        if self.instrument_default is not None:
            query_dict["msrun_sequence__instrument"] = self.instrument_default
        if self.date_default is not None:
            query_dict["msrun_sequence__date"] = self.date_default

        msrun_samples = msrun_samples.filter(**query_dict)

        # Check if there were too few or too many results.
        if msrun_samples.count() == 0:
            query_dict["sample__name"] = sample_header
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(MSRunSample, query_dict)
            )
            return None
        elif msrun_samples.count() > 1:
            try:
                msrun_samples.get()
                self.aggregated_errors_object.buffer_error(
                    ProgrammingError("Well this is unexpected.")
                )
            except Exception as e:
                self.aggregated_errors_object.buffer_error(e)
            return None

        return msrun_samples.get()

    @transaction.atomic
    def get_or_create_peak_group_compound_link(self, pgrec, cmpd_rec):
        """Get or create a peakgroup_compound record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_group (Optional[PeakGroup])
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakData])
            created (boolean)
        """
        pgrec.compounds.add(cmpd_rec)

    @transaction.atomic
    def get_or_create_peak_data(self, row, peak_group: Optional[PeakGroup]):
        """Get or create a PeakData record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_group (Optional[PeakGroup])
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakData])
            created (boolean)
        """
        med_mz = self.get_row_val(row, self.headers.MEDMZ)
        med_rt = self.get_row_val(row, self.headers.MEDRT)
        raw_abundance = self.get_row_val(row, self.headers.RAW)
        corrected_abundance = self.get_row_val(row, self.headers.CORRECTED)

        if peak_group is None or self.is_skip_row():
            self.skipped(PeakData.__name__)
            return None, False

        rec_dict = {
            "peak_group": peak_group,
            "raw_abundance": raw_abundance,
            "corrected_abundance": corrected_abundance,
            "med_mz": med_mz,
            "med_rt": med_rt,
        }

        try:
            rec, created = PeakData.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakData.__name__)
            else:
                self.existed(PeakData.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakData, rec_dict)
            self.errored(PeakData.__name__)
            raise RollbackException()

        return rec, created

    def get_or_create_labels(
        self, row, pdrec: Optional[PeakData], pgrec: Optional[PeakGroup]
    ):
        """Get or create a PeakGrouplabel and PeakDataLabel records.  Handles exceptions, updates stats, and triggers a
        rollback.

        Args:
            row (pandas.Series)
            pdrec (Optional[PeakData])
            pgrec (Optional[PeakGroup])
        Exceptions:
            Buffers:
                ObservedIsotopeParsingError
            Raises:
                None
        Returns:
            pglrecs (List[Tuple[Optional[PeakGroupLabel, boolean]]])
            pdlrecs (List[Tuple[Optional[PeakDataLabel, boolean]]])
        """
        # Parse the isotope obsevations.
        isotope_label = self.get_row_val(row, self.headers.ISOTOPELABEL)
        pglrecs: List[Tuple[PeakGroup, bool]] = []
        pdlrecs: List[Tuple[PeakData, bool]] = []
        possible_isotope_observations = None
        num_possible_isotope_observations = 1
        if pgrec is not None:
            possible_isotope_observations = pgrec.possible_isotope_observations
            num_possible_isotope_observations = len(possible_isotope_observations)

        try:
            label_observations = parse_isotope_label(
                isotope_label, possible_isotope_observations
            )
        except ObservedIsotopeParsingError as iope:
            self.aggregated_errors_object.buffer_error(iope)
            self.skipped(PeakGroupLabel.__name__, num=num_possible_isotope_observations)
            self.skipped(PeakDataLabel.__name__, num=num_possible_isotope_observations)
            return pglrecs, pdlrecs

        # Get or create the PeakGroupLabel and PeakDataLabel records
        for label_obs in label_observations:
            try:
                pglrecs.append(
                    self.get_or_create_peak_group_label(pgrec, label_obs["element"])
                )
            except RollbackException:
                continue

            try:
                pdlrecs.append(
                    self.get_or_create_peak_data_label(
                        pdrec,
                        label_obs["element"],
                        label_obs["count"],
                        label_obs["mass_number"],
                    )
                )
            except RollbackException:
                continue

        return pglrecs, pdlrecs

    @transaction.atomic
    def get_or_create_peak_group_label(
        self, peak_group: Optional[PeakGroup], element: str
    ):
        """Get or create a PeakGroupLabel record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_group (Optional[PeakGroup])
            element (str)
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakGroupLabel])
            created (boolean)
        """
        if peak_group is None or self.is_skip_row():
            self.skipped(PeakGroupLabel.__name__)
            return None, False

        rec_dict = {
            "peak_group": peak_group,
            "element": element,
        }

        try:
            rec, created = PeakGroupLabel.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakGroupLabel.__name__)
            else:
                self.existed(PeakGroupLabel.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakGroupLabel, rec_dict)
            self.errored(PeakGroupLabel.__name__)
            raise RollbackException()

        return rec, created

    @transaction.atomic
    def get_or_create_peak_data_label(self, peak_data, element, count, mass_number):
        """Get or create a PeakDataLabel record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_data (Optional[PeakData])
            element (str)
            count (int)
            mass_number (int)
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakDataLabel])
            created (boolean)
        """
        if peak_data is None or self.is_skip_row():
            self.skipped(PeakDataLabel.__name__)
            return None, False

        rec_dict = {
            "peak_data": peak_data,
            "element": element,
            "count": count,
            "mass_number": mass_number,
        }

        try:
            rec, created = PeakDataLabel.objects.get_or_create(**rec_dict)
            if created:
                rec.full_clean()
                self.created(PeakDataLabel.__name__)
            else:
                self.existed(PeakDataLabel.__name__)
        except Exception as e:
            self.handle_load_db_errors(e, PeakDataLabel, rec_dict)
            self.errored(PeakDataLabel.__name__)
            raise RollbackException()

        return rec, created


class IsocorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an isocorr excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "isocorr"

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
        "compound": "Compound",
    }

    merged_drop_columns_list = [
        "compound",
        "label",
        "metaGroupId",
        "groupId",
        "goodPeakCount",
        "maxQuality",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
    ]

    condense_columns_dict = {
        "absolte": {
            "header_column": "Sample",
            "value_column": "Raw Abundance",
            "uncondensed_columns": [
                "compoundId",
                "formula",
                "label",
                "metaGroupId",
                "groupId",
                "goodPeakCount",
                "medMz",
                "medRt",
                "maxQuality",
                "isotopeLabel",
                "compound",
                "expectedRtDiff",
                "ppmDiff",
                "parent",
            ],
        },
    }

    # No columns to add
    add_columns_dict = None

    # No merge necessary, just use the absolte sheet
    merge_dict = {
        "first_sheet": "absolte",
        "next_merge_dict": None,
    }


class AccucorLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an accucor excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "accucor"

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
    }

    merged_drop_columns_list = [
        "compound",
        "adductName",
        "label",
        "metaGroupId",
        "groupId",
        "goodPeakCount",
        "maxQuality",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
        "C_Label",
    ]

    condense_columns_dict = {
        "Original": {
            "header_column": "Sample",
            "value_column": "Raw Abundance",
            "uncondensed_columns": [
                "label",
                "metaGroupId",
                "groupId",
                "goodPeakCount",
                "medMz",
                "medRt",
                "maxQuality",
                "adductName",
                "isotopeLabel",
                "compound",
                "compoundId",
                "formula",
                "expectedRtDiff",
                "ppmDiff",
                "parent",
                "Compound",  # From add_columns_dict
                "C_Label",  # From add_columns_dict
            ],
        },
        "Corrected": {
            "header_column": "Sample",
            "value_column": "Corrected Abundance",
            "uncondensed_columns": [
                "Compound",
                "C_Label",
                "adductName",
            ],
        },
    }

    add_columns_dict = {
        # Sheet: dict
        "Original": {
            # New column name: method that takes a dataframe to create the new column
            "C_Label": (
                lambda df: df["isotopeLabel"]
                .str.split("-")
                .str.get(-1)
                .replace({"C12 PARENT": "0"})
                .astype(int)
            ),
            # Rename happens after merge, but before merge, we want matching column names in each sheet, so...
            "Compound": lambda df: df["compound"],
        }
    }

    merge_dict = {
        "first_sheet": "Corrected",  # This key only occurs once in the outermost dict
        "next_merge_dict": {
            "on": ["Compound", "C_Label", "mzXML Name"],
            "left_columns": None,  # all
            "right_sheet": "Original",
            "right_columns": [
                "formula",
                "medMz",
                "medRt",
                "isotopeLabel",
                "Raw Abundance",
            ],
            "how": "left",
            "next_merge_dict": None,
        },
    }
