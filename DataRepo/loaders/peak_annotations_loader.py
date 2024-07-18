import os
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Dict, List, Optional

from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction
from django.db.utils import ProgrammingError

from DataRepo.loaders.base.converted_table_loader import ConvertedTableLoader
from DataRepo.loaders.base.table_column import TableColumn
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.models import (
    ArchiveFile,
    Compound,
    DataFormat,
    DataType,
    MaintainedModel,
    MSRunSample,
    PeakData,
    PeakDataLabel,
    PeakGroup,
    PeakGroupLabel,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConditionallyRequiredArgs,
    DuplicateCompoundIsotopes,
    DuplicateValues,
    InfileError,
    IsotopeStringDupe,
    MissingCompounds,
    MissingSamples,
    NoSamples,
    NoTracerLabeledElements,
    NoTracers,
    ObservedIsotopeParsingError,
    ObservedIsotopeUnbalancedError,
    RecordDoesNotExist,
    RollbackException,
    UnexpectedLabels,
    UnexpectedSamples,
    UnskippedBlanks,
    generate_file_location_string,
)
from DataRepo.utils.file_utils import is_excel, string_to_datetime
from DataRepo.utils.infusate_name_parser import (
    ObservedIsotopeData,
    parse_isotope_label,
)

PeakGroupCompound = PeakGroup.compounds.through


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
        SAMPLEHEADER="Sample Header",
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
    DataRequiredValues = [
        ISOTOPELABEL_KEY,
        FORMULA_KEY,
        COMPOUND_KEY,
        SAMPLEHEADER_KEY,
        CORRECTED_KEY,
    ]

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

    # No FieldToDataValueConverter needed

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
            # TODO: Add dynamic_choices here from the Compounds sheet by joining compounds with the same formula
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
    Models = [
        ArchiveFile,
        PeakData,
        PeakDataLabel,
        PeakGroup,
        PeakGroupLabel,
        PeakGroupCompound,
    ]

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
                filename (Optional[str]): In case the (superclass arg) "file" is a temp file with a nonsense name.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        # Custom options for the MSRunsLoader member instance.
        self.peak_annotation_details_file = kwargs.pop(
            "peak_annotation_details_file", None
        )
        self.peak_annotation_details_sheet = kwargs.pop(
            "peak_annotation_details_sheet", None
        )
        self.peak_annotation_details_df = kwargs.pop("peak_annotation_details_df", None)

        # Require the file argument if df is supplied
        if kwargs.get("df") is not None or self.peak_annotation_details_df is not None:
            if kwargs.get("file") is None:
                raise AggregatedErrors().buffer_error(
                    ConditionallyRequiredArgs(
                        "The [file] argument is required if either the [df] or [peak_annotation_details_df] argument "
                        "is supplied."
                    )
                )

        # The MSRunsLoader member instance is used for 2 purposes:
        # 1. Obtain/process the MSRunSequence defaults (it uses the SequencesLoader).
        # 2. Obtain the previously loaded MSRunSample records mapped to sample names (when different from the headers).
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

        # Convert the supplied df using the derived class.
        # Cannot call super().__init__() because ABC.__init__() takes a custom argument
        ConvertedTableLoader.__init__(self, *args, **kwargs)

        # Initialize the MSRun Sample and Sequence data (obtained from self.msrunsloader)
        self.operator_default = None
        self.date_default = None
        self.lc_protocol_name_default = None
        self.instrument_default = None
        self.msrun_sample_dict = {}
        self.initialize_msrun_data()

        # Error tracking/reporting - the remainder of this init (here, down) is all about error tracking/reporting.

        # If the sample that a header maps to is missing, track it so we don't raise that error multiple times.
        self.missing_headers_as_samples = []

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

        # Suggestions about failed header lookups depend on the supplied inputs.
        # TODO: Replace the sample sheet/column strings in the values below with a sheet/column reference from the
        # sample loader once the sample loader inherits from TableLoader
        if self.peak_annotation_details_df is None:
            self.missing_msrs_suggestion = (
                f"Please supply the peak_annotation_details_df argument to map "
                f"{self.msrunsloader.headers.SAMPLEHEADER}s to database {self.msrunsloader.headers.SAMPLENAME}s."
            )
        else:
            self.missing_msrs_suggestion = (
                f"Did you forget to include this {self.headers.SAMPLEHEADER} in the "
                f"{self.msrunsloader.headers.SAMPLEHEADER} column of the {self.msrunsloader.DataSheetName} sheet/file "
                "that matches a Sample in the Samples sheet?"
            )

    def initialize_msrun_data(self):
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
                peak_annot_file=self.friendly_file  # The name in the sheet will be the friendly one
            )
            # Keep track of what has been retrieved
            for sh in self.msrun_sample_dict.keys():
                self.msrun_sample_dict[sh]["seen"] = False

        # Remove exceptions about Sample table search failures.  Those are remored by a different loader.  This avoids
        # exceptions about MissingSamples exception building about sample names having come from different files.
        self.msrunsloader.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Sample
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

    # There are maintained fields in the models involved, so deferring autoupdates will make this faster
    @MaintainedModel.defer_autoupdates(
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def load_data(self):
        """Loads the ArchiveFile, PeakGroup, PeakGroupLabel, PeakData, and PeakDataLabel tables from the dataframe.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # There are cached fields in the models involved, so disabling cache updates will make this faster.
        disable_caching_updates()

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

            # Get compounds
            pgname, cmpd_recs = self.get_peak_group_name_and_compounds(row)

            # Get or create PeakGroups
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

            label_observations = self.get_label_observations(row, pgrec)

            # Get or create PeakData (need the label due to no unique constraint)
            try:
                # We can't know whether to get or create a PeakData record because there's no unique constraint.  You
                # need the labels to distinguish between 2 different records with the same mz, rt, raw, and corrected
                # count values.
                pdrec, _ = self.get_or_create_peak_data(row, pgrec, label_observations)
            except RollbackException:
                pass

            if label_observations is None or len(label_observations) == 0:
                self.skipped(PeakDataLabel.__name__)
                self.skipped(PeakGroup.__name__)
                continue

            for label_obs in label_observations:
                # Get or create PeakGroupLabel
                try:
                    self.get_or_create_peak_group_label(pgrec, label_obs["element"])
                except RollbackException:
                    continue

                try:
                    self.get_or_create_peak_data_label(
                        pdrec,
                        label_obs["element"],
                        label_obs["count"],
                        label_obs["mass_number"],
                    )
                except RollbackException:
                    continue

        # This currently only repackages DuplicateValues exceptions, but may do more WRT mapping to original file
        # locations of errors later.  It could be called at the top of this method (bec dupes are handled before this
        # method is called), but given the plan to have it handle more exceptions, having it here at the bottom is
        # better.
        self.handle_file_exceptions()

        enable_caching_updates()
        delete_all_caches()

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
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "is_binary": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "filename": os.path.basename(
                    self.friendly_file
                ),  # In case file is a temp file with a nonsense name
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
            recs (List[Compound])
        """
        names_str = self.get_row_val(row, self.headers.COMPOUND)
        names = names_str.split(self.CompoundNamesDelimiter)
        recs = []

        for name_str in names:
            name = name_str.strip()
            try:
                recs.append(Compound.compound_matching_name_or_synonym(name))
            except (ValidationError, ObjectDoesNotExist) as cmpderr:
                self.aggregated_errors_object.buffer_error(
                    RecordDoesNotExist(
                        Compound,
                        Compound.get_name_query_expression(name),
                        file=self.friendly_file,
                        sheet=self.sheet,
                        column=self.headers.COMPOUND,
                        rownum=self.rownum,
                    ),
                    orig_exception=cmpderr,
                )
                # Appending so that not only the skip count can be updated, but so the build-a-submission interface can
                # associate a delimited name with whether the record was found of not
                recs.append(None)

        pgname = None
        if len(recs) > 0:
            if None in recs:
                # Cannot set the name based on the records when they contain a None value.  There will be an error
                # anyway, so just set the name from the column.
                pgname = self.CompoundNamesDelimiter.join(sorted(names))
            else:
                recs = sorted(recs, key=lambda rec: rec.name)
                # Set the peak group name to the sorted primary compound names, delimited by "/"
                pgname = self.CompoundNamesDelimiter.join([r.name for r in recs])

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
        sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
        formula = self.get_row_val(row, self.headers.FORMULA)

        msrun_sample = self.get_msrun_sample(sample_header)

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

        if len(rec.tracer_labeled_elements) == 0:
            self.add_skip_row_index()
            if not self.aggregated_errors_object.exception_exists(
                NoTracers, "animal", msrun_sample.sample.animal
            ):
                self.aggregated_errors_object.buffer_error(
                    NoTracers(
                        msrun_sample.sample.animal,
                        file=self.friendly_file,
                        sheet=self.sheet,
                    )
                )

        return rec, created

    def get_msrun_sample(self, sample_header):
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
            sample_header (str)
        Exceptions:
            Buffers:
                RecordDoesNotExist
                ConditionallyRequiredArgs
                ProgrammingError
            Raises:
                None
        Returns:
            msrun_sample (Optional[MSRunSample])
        """
        # If we already know (from a previous row, that) the sample the header maps to is missing in the database
        if sample_header in self.missing_headers_as_samples:
            return None

        # There are 2 ways we can use to obtain the MSRunSample Record
        # 1. The first way we will try to obtain the MSRunSample record is using the data provided in the Peak
        #    Annotation Details sheet, if one was provided.  That data was obtained in the initialize_sequence_defaults
        #    method (called by the constructor).
        if sample_header in self.msrun_sample_dict.keys():

            self.msrun_sample_dict[sample_header]["seen"] = True

            # TODO: Consolidate the strategy.  I had made a quick change to the SKIP value coming from the file due to a
            # pandas quirk about dtype and empty excel cells, but the value returned by
            # self.msrunsloader.get_loaded_msrun_sample_dict is converted to a boolean.  This can lead to confusion, so
            # pick one strategy and go with it.
            if (
                self.msrunsloader.headers.SKIP
                in self.msrun_sample_dict[sample_header].keys()
                and self.msrun_sample_dict[sample_header][
                    self.msrunsloader.headers.SKIP
                ]
                is True
            ):
                return None

            if self.msrun_sample_dict[sample_header][MSRunSample.__name__] is not None:
                return self.msrun_sample_dict[sample_header][MSRunSample.__name__]

        # 2. The second way (if a Peak Annotation Details sheet was not provided, or doesn't list a value for this
        #    sample header) is to start searching using the sample header to look for an exact matching sample name.  If
        #    there is more than 1 match, we can try to whittle it down using what we've been provided in the way of the
        #    default sequence data.

        # Initialize the entry in the msrun_sample_dict so we can avoid this code block if we encounter the header again
        self.msrun_sample_dict[sample_header] = {}
        self.msrun_sample_dict[sample_header]["seen"] = True
        self.msrun_sample_dict[sample_header][MSRunSample.__name__] = None

        # First, we will check the Sample table directly, so we can report the most relevant error if it's missing
        query_dict = {"name": sample_header}
        samples = Sample.objects.filter(**query_dict)
        if samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    Sample,
                    query_dict,
                    suggestion=self.missing_msrs_suggestion,
                    file=self.friendly_file,
                    sheet=self.sheet,
                    column=self.headers.SAMPLEHEADER,
                    rownum=self.rownum,
                )
            )
            self.missing_headers_as_samples.append(sample_header)
            return None

        sample = samples.get()

        # Now try to get the MSRunSample record using the sample
        msrun_samples = MSRunSample.objects.filter(sample=sample)

        # Check if there were too few or exactly 1 results.
        if msrun_samples.count() == 0:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    MSRunSample,
                    # This is the effective query. Just wanted to distinguish between a missing sample record and a
                    # missing MSRunSample record
                    {"sample__name": sample_header},
                    suggestion=self.missing_msrs_suggestion,
                    file=self.friendly_file,
                    sheet=self.sheet,
                    column=self.headers.SAMPLEHEADER,
                    rownum=self.rownum,
                )
            )
            self.missing_headers_as_samples.append(sample_header)
            return None
        elif msrun_samples.count() == 1:
            self.msrun_sample_dict[sample_header][
                MSRunSample.__name__
            ] = msrun_samples.get()
            return self.msrun_sample_dict[sample_header][MSRunSample.__name__]

        # At this point, there are multiple results.  That means that defaults for the sequence are required in order to
        # proceed, so check them.

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

        # Create this exception object (without buffering) to use in 2 possible buffering locations
        not_enough_defaults_exc = ConditionallyRequiredArgs(
            "The arguments supplied to the constructor were insufficient to identify the sequence 1 or more of the "
            "samples belong to.  Either peak_annotation_details_df can be supplied with a sequence name for every "
            "MSRunSample record or the following defaults can be supplied via file_defaults of explicit arguments: "
            f"[operator, lc_protocol_name, instrument, and/or date].  {len(query_dict.keys())} defaults supplied were "
            f"used to create the following query: {query_dict}."
        )

        if len(query_dict.keys()) == 0:
            # Only buffer this error once
            if not self.aggregated_errors_object.exception_type_exists(
                ConditionallyRequiredArgs
            ):
                self.aggregated_errors_object.buffer_error(not_enough_defaults_exc)
            self.missing_headers_as_samples.append(sample_header)
            return None

        # Let's see if the results can be narrowed to a single record using the sequence defaults we've been provided.
        msrun_samples = msrun_samples.filter(**query_dict)

        # Check if there were too few or too many results.
        if msrun_samples.count() == 0:
            query_dict["sample__name"] = sample_header
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    MSRunSample,
                    query_dict,
                    suggestion=self.missing_msrs_suggestion,
                    file=self.friendly_file,
                    sheet=self.sheet,
                    column=self.headers.SAMPLEHEADER,
                    rownum=self.rownum,
                )
            )
            self.missing_headers_as_samples.append(sample_header)
            return None
        elif msrun_samples.count() > 1:
            if len(query_dict.keys()) < 4:
                if not self.aggregated_errors_object.exception_type_exists(
                    ConditionallyRequiredArgs
                ):
                    self.aggregated_errors_object.buffer_error(not_enough_defaults_exc)
            else:
                # TODO: After rebase on main, convert this into a MultipleRecordsReturned exception
                try:
                    msrun_samples.get()
                except Exception as e:
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            str(e),
                            file=self.friendly_file,
                            sheet=self.sheet,
                            rownum=self.rownum,
                        ),
                        orig_exception=e,
                    )

            return None

        self.msrun_sample_dict[sample_header][
            MSRunSample.__name__
        ] = msrun_samples.get()
        return self.msrun_sample_dict[sample_header][MSRunSample.__name__]

    @transaction.atomic
    def get_or_create_peak_group_compound_link(
        self, pgrec: Optional[PeakGroup], cmpd_rec: Compound
    ):
        """Get or create a peakgroup_compound record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            pgrec (Optional[PeakGroup])
            cmpd_rec (Optional[Compound])
        Exceptions:
            Buffers:
                None
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakData])
            created (boolean)
        """
        created = False
        rec = None

        if pgrec is None or cmpd_rec is None or self.is_skip_row():
            # Subsequent record creations from this row should be skipped.
            self.add_skip_row_index()
            self.skipped(PeakGroupCompound.__name__)
            return rec, created

        try:
            rec, created = pgrec.get_or_create_compound_link(cmpd_rec)
        except NoTracerLabeledElements as ntle:
            # Add infile context to the exception
            ntle.set_formatted_message(
                file=self.friendly_file,
                sheet=self.sheet,
                column=self.headers.COMPOUND,
                rownum=self.rownum,
            )
            self.aggregated_errors_object.buffer_error(ntle)
            # Subsequent record creations from this row should be skipped.
            self.add_skip_row_index()
            self.errored(PeakGroupCompound.__name__)
            raise RollbackException()
        except Exception as e:
            # This is the effective rec_dict
            rec_dict = {
                "peakgroup": pgrec,
                "compound": cmpd_rec,
            }
            self.handle_load_db_errors(e, PeakGroupCompound, rec_dict)
            self.errored(PeakGroupCompound.__name__)
            raise RollbackException()

        if created:
            self.created(PeakGroupCompound.__name__)
            # No need to call full clean.
        else:
            self.existed(PeakGroupCompound.__name__)

        return rec, created

    @transaction.atomic
    def get_or_create_peak_data(
        self,
        row,
        peak_group: Optional[PeakGroup],
        label_obs: Optional[List[ObservedIsotopeData]],
    ):
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

        if peak_group is None or label_obs is None or self.is_skip_row():
            self.skipped(PeakData.__name__)
            return None, False

        rec_dict = {
            "peak_group": peak_group,
            "raw_abundance": raw_abundance,
            "corrected_abundance": corrected_abundance,
            "med_mz": med_mz,
            "med_rt": med_rt,
        }

        # A prior processing of this file could have created this record, or a previous row of the file could have
        # created a PeakData record with identical values (e.g. med_mz=0, med_rt=0, raw_abundance=0, and
        # corrected_abundance=0).  The only way to tell them apart is by their associated labels (PeakDataLabel
        # records).

        try:
            rec, created = PeakData.get_or_create(label_obs, **rec_dict)
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

    def get_label_observations(self, row, pgrec: Optional[PeakGroup]):
        """Parse the isotopeLabel and add in labels from the tracers (whose elements are present in the observed
        compound) to record 0 counts.

        Args:
            row (pd.Series)
            pgrec (Optional[PeakGroup)
        Exceptions:
            Buffers:
                UnexpectedLabels
                IsotopeStringDupe
                ObservedIsotopeUnbalancedError
                ObservedIsotopeParsingError
            Raises:
                None
        Returns:
            label_observations (Optional[List[ObservedIsotopeData]])
        """
        # Parse the isotope obsevations.
        isotope_label = self.get_row_val(row, self.headers.ISOTOPELABEL)
        label_observations = None

        # Even if this is a skip row, we can still process the isotope label string to find issues...
        possible_isotope_observations = None
        num_possible_isotope_observations = 1
        if pgrec is not None:
            if pgrec.compounds.count() == 0:
                self.aggregated_errors_object.buffer_error(
                    ProgrammingError(
                        "PeakGroup record has no linked compounds.  A peak group must have associated compounds in "
                        "order to confirm label observations."
                    )
                )
            else:
                possible_isotope_observations = pgrec.possible_isotope_observations
                num_possible_isotope_observations = len(possible_isotope_observations)

        # For the exceptions below (for convenience)
        infile_err_args = {
            "file": self.friendly_file,
            "sheet": self.sheet,
            "rownum": self.rownum,
            "column": self.headers.ISOTOPELABEL,
        }
        try:
            label_observations = parse_isotope_label(
                isotope_label, possible_isotope_observations
            )
        except UnexpectedLabels as olnp:
            olnp.set_formatted_message(**infile_err_args)
            # There might be contamination.  Set is_fatal to true in validate mode to alert the researcher via a raised
            # warning (as opposed to just printing a warning for a curator running the load script on the command line -
            # who shouldn't have to worry about it)
            self.aggregated_errors_object.buffer_warning(olnp, is_fatal=self.validate)
            self.warned(PeakGroupLabel.__name__, num=num_possible_isotope_observations)
            self.warned(PeakDataLabel.__name__, num=num_possible_isotope_observations)
        except (
            IsotopeStringDupe,
            ObservedIsotopeUnbalancedError,
            ObservedIsotopeParsingError,
        ) as ie:
            # Add file context
            ie.set_formatted_message(**infile_err_args)
            self.aggregated_errors_object.buffer_error(ie)
            self.errored(PeakGroupLabel.__name__, num=num_possible_isotope_observations)
            self.errored(PeakDataLabel.__name__, num=num_possible_isotope_observations)

        return label_observations

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
        if peak_group is None or element is None or self.is_skip_row():
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
        if (
            peak_data is None
            or element is None
            or count is None
            or mass_number is None
            or self.is_skip_row()
        ):
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

    def report_discrepant_headers(self):
        """This removes RecordDoesNotExist exceptions (from the aggregated errors) about missing Sample records in the
        peak annotation details sheet and replaces them.  Among those not found in the peak annotation details, it
        breaks them up into a MissingSamples (or NoSamples) error and an UnskippedBlanks warning.  It also buffers a
        warning for sample headers in the peak annotation details sheet that were not found in the peak annotations
        file.

        Args:
            None
        Exceptions:
            Buffers:
                UnskippedBlanks
                MissingSamples
                NoSamples
                UnexpectedSamples
            Raises:
                None
        Returns:
            None
        """
        # Extract exceptions about missing Sample records
        sample_dnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Sample
        )

        # Separate the exceptions based on whether they appear to be blanks or not
        possible_blank_dnes = []
        likely_missing_dnes = []
        for sdne in sample_dnes:
            if Sample.is_a_blank(sdne.query_obj["name"]):
                possible_blank_dnes.append(sdne)
            else:
                likely_missing_dnes.append(sdne)

        # Buffer an error about missing samples (that are not blanks)
        if len(likely_missing_dnes) > 0:
            # See if *any* samples were found (i.e. the MSRunSample record existed)
            num_found_samples = len(
                [
                    sh
                    for sh in self.msrun_sample_dict.keys()
                    # A Sample can be inferred to be found if the MSRunSample rec is not None in the msrun_sample_dict
                    # Sample searches do not occur for these cases
                    if (
                        self.msrun_sample_dict[sh]["seen"] is True
                        and self.msrun_sample_dict[sh][MSRunSample.__name__] is not None
                    )
                ]
            )

            if num_found_samples == 0:
                self.aggregated_errors_object.buffer_error(
                    NoSamples(
                        likely_missing_dnes,
                        suggestion=self.missing_msrs_suggestion,
                    )
                )
            else:
                self.aggregated_errors_object.buffer_error(
                    MissingSamples(
                        likely_missing_dnes,
                        suggestion=self.missing_msrs_suggestion,
                    )
                )

        if len(possible_blank_dnes) > 0:
            self.aggregated_errors_object.buffer_warning(
                UnskippedBlanks(
                    possible_blank_dnes,
                    suggestion=(
                        f"Use the {self.msrunsloader.DataSheetName} sheet/file to add these "
                        f"{self.headers.SAMPLEHEADER}s to the {self.msrunsloader.headers.SAMPLEHEADER} column and set "
                        f"its {self.msrunsloader.headers.SKIP} column to 'true'."
                    ),
                )
            )

        # See if *any* samples were found (including Sample searches that weren't performed bec. the MSRunSample
        # record existed)
        unexpected_samples = [
            uss
            for uss in self.msrun_sample_dict.keys()
            if self.msrun_sample_dict[uss]["seen"] is False
        ]
        if len(unexpected_samples) > 0:
            self.aggregated_errors_object.buffer_warning(
                UnexpectedSamples(
                    unexpected_samples,
                    suggestion=(
                        f"Make sure that the {self.headers.SAMPLEHEADER}s whose peak annotation file is "
                        f"'{self.friendly_file}' in the {self.msrunsloader.DataSheetName} sheet/file is correct."
                    ),
                )
            )

    def handle_file_exceptions(self):
        """Repackage and summarize repeated exceptions.

        Because of the file conversion, the file metadata in the exceptions doesn't relate directly to the structure
        of the original file.  Errors also have extra associated metadata (e.g. instances of DuplicateValues
        unnecessarily include multiples by sample name, i.e. every duplicate isotope always affects every sample, thus
        only 1 error indicating the row and 2 columns: compound and isotopeLabel).

        This method must be called before the end of the load_data method, because the wrapper around load_data
        summarizes the DuplicateValues exceptions.

        Args:
            None
        Exceptions:
            Buffers:
                DuplicateCompoundIsotope
            Raises:
                None
        Returns:
            None
        """
        # This code relies on the following DataUniqueColumnConstraints, so we need to raise if it changes
        unique_constraint = [
            self.SAMPLEHEADER_KEY,
            self.COMPOUND_KEY,
            self.ISOTOPELABEL_KEY,
        ]
        if len(self.DataUniqueColumnConstraints) != 1 or set(
            self.DataUniqueColumnConstraints[0]
        ) != set(unique_constraint):
            # If you find yourself here, it means that the exception "DuplicateCompoundIsotope" must only be raised when
            # the unique constraint for [self.SAMPLEHEADER_KEY, self.COMPOUND_KEY, self.ISOTOPELABEL_KEY] has been
            # violated (i.e. only remove and repackage DuplicateValues exceptions that relate to the columns in the
            # conditional above).
            raise ProgrammingError(
                "DataUniqueColumnConstraints has changed, which means that handle_file_exceptions must be updated, as "
                "it assumes 1 specific file-level unique constraint."
            )

        # Repackage/Summarize DuplicateValues
        dves = self.aggregated_errors_object.remove_matching_exceptions(
            DuplicateValues,
            "colnames",
            # This lambda is to determine if the DuplicateValues exception is for the target unique constraint, by
            # checking that the population of column names saved in the instance is the same
            lambda colnames: set(colnames) == set(unique_constraint),
        )
        if len(dves) > 0:
            self.aggregated_errors_object.buffer_error(
                DuplicateCompoundIsotopes(
                    dves, [self.COMPOUND_KEY, self.ISOTOPELABEL_KEY]
                )
            )

        # Catch sample header discrepancies between the peak annotation file and the peak annotation details file and
        # summarize RecordDoesNotExist exceptions related to those sample headers.
        self.report_discrepant_headers()

        # Summarize missing compounds
        compound_dnes = self.aggregated_errors_object.remove_matching_exceptions(
            RecordDoesNotExist, "model", Compound
        )
        if len(compound_dnes) > 0:
            self.aggregated_errors_object.buffer_error(
                MissingCompounds(
                    compound_dnes,
                    suggestion=(
                        "Compounds referenced in the peak annotation files must be loaded into the database before "
                        "loading.  Please take note of the compounds, select a primary name, any synonyms, and find an "
                        f"HMDB ID associated with the compound, and add it to {self.compounds_loc} in your submission."
                    ),
                ),
            )

        # TODO: Add handling/consolidation of MultipleRecordsReturned (either here or in TableLoader)

    @classmethod
    def determine_matching_formats(cls, df) -> List[str]:
        """Given a dataframe or (ideally) a dict of dataframes, return a list of the format_codes of the matching
        formats.

        Args:
            df (dict|pd.DataFrame)
        Exceptions:
            None
        Returns:
            matching_format_codes (List[str]): Format codes of the matching DataFormats.
        """
        matching_format_codes: List[str] = []
        for subcls in PeakAnnotationsLoader.__subclasses__():
            if subcls == UnicorrLoader:
                # This is the identity type, which is handled below
                continue
            if isinstance(df, dict):
                expected_sheets = set(subcls.get_required_sheets())
                supplied_sheets = set(list(df.keys()))
                if expected_sheets <= supplied_sheets:
                    match = True
                    for sheet in expected_sheets:
                        expected_headers = set(subcls.get_required_headers(sheet))
                        supplied_headers = set(list(df[sheet].columns))
                        if expected_headers > supplied_headers:
                            match = False
                            break
                    if match:
                        matching_format_codes.append(str(subcls.format_code))
            else:  # pd.DataFrame
                # All we can do here (currently) is check that the headers in the dataframe are a subset of the
                # flattened original headers (from all the sheets).  It would be possible to do the determination by
                # specific sheet header contents if the class attributes were populated differently, but that can be
                # done via a refactor.
                supplied_headers = set(list(df.columns))
                expected_headers = set(subcls.get_required_headers(None))
                if expected_headers <= supplied_headers:
                    matching_format_codes.append(str(subcls.format_code))

        # Handle the converted format too:
        expected_headers = set(PeakAnnotationsLoader.DataHeaders._asdict().values())
        if isinstance(df, dict):
            if PeakAnnotationsLoader.DataSheetName in df.keys():
                supplied_headers = set(
                    list(df[PeakAnnotationsLoader.DataSheetName].columns)
                )
                if supplied_headers <= expected_headers:
                    matching_format_codes.append(str(UnicorrLoader.format_code))
        else:
            supplied_headers = set(list(df.columns))
            if supplied_headers <= expected_headers:
                matching_format_codes.append(str(UnicorrLoader.format_code))

        return matching_format_codes

    @classmethod
    def get_supported_formats(cls) -> List[str]:
        """Get a list of format codes for all supported formats."""
        return [
            str(subcls.format_code) for subcls in PeakAnnotationsLoader.__subclasses__()
        ]


class IsocorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an isocorr excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "isocorr"

    OrigDataTableHeaders = namedtuple(
        "OrigDataTableHeaders",
        [
            "COMPOUNDID",
            "FORMULA",
            "LABEL",
            "METAGROUPID",
            "GROUPID",
            "GOODPEAKCOUNT",
            "MEDMZ",
            "MEDRT",
            "MAXQUALITY",
            "ADDUCTNAME",
            "ISOTOPELABEL",
            "COMPOUND",
            "EXPECTEDRTDIFF",
            "PPMDIFF",
            "PARENT",
        ],
    )

    OrigDataHeaders = OrigDataTableHeaders(
        COMPOUNDID="compoundId",
        FORMULA="formula",
        LABEL="label",
        METAGROUPID="metaGroupId",
        GROUPID="groupId",
        GOODPEAKCOUNT="goodPeakCount",
        MEDMZ="medMz",
        MEDRT="medRt",
        MAXQUALITY="maxQuality",
        ADDUCTNAME="adductName",
        ISOTOPELABEL="isotopeLabel",
        COMPOUND="compound",
        EXPECTEDRTDIFF="expectedRtDiff",
        PPMDIFF="ppmDiff",
        PARENT="parent",
    )

    OrigDataRequiredHeaders = {
        "absolte": [
            "FORMULA",
            "MEDMZ",
            "MEDRT",
            "ISOTOPELABEL",
            "COMPOUND",
        ],
    }

    OrigDataColumnTypes: Dict[str, type] = {
        "FORMULA": str,
        "MEDMZ": float,
        "MEDRT": float,
        "ISOTOPELABEL": str,
        "COMPOUND": str,
    }

    # These attributes are defined in the order in which they are applied

    # No columns to add
    add_columns_dict = None

    condense_columns_dict = {
        "absolte": {
            "header_column": "Sample Header",
            "value_column": "Corrected Abundance",
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
                "adductName",
                "isotopeLabel",
                "compound",
                "expectedRtDiff",
                "ppmDiff",
                "parent",
            ],
        },
    }

    # No merge necessary, just use the absolte sheet
    merge_dict = {
        "first_sheet": "absolte",
        "next_merge_dict": None,
    }

    nan_defaults_dict = {
        "Corrected Abundance": 0,
        "medMz": 0,
        "medRt": 0,
    }

    sort_columns = None
    nan_filldown_columns = None

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
        "adductName",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
    ]


class AccucorLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an accucor excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "accucor"

    OrigDataTableHeaders = namedtuple(
        "OrigDataTableHeaders",
        [
            "COMPOUNDID",
            "FORMULA",
            "LABEL",
            "METAGROUPID",
            "GROUPID",
            "GOODPEAKCOUNT",
            "MEDMZ",
            "MEDRT",
            "MAXQUALITY",
            "ISOTOPELABEL",
            "ORIGCOMPOUND",
            "CORRCOMPOUND",
            "EXPECTEDRTDIFF",
            "PPMDIFF",
            "PARENT",
            "CLABEL",
        ],
    )

    OrigDataHeaders = OrigDataTableHeaders(
        COMPOUNDID="compoundId",
        FORMULA="formula",
        LABEL="label",
        METAGROUPID="metaGroupId",
        GROUPID="groupId",
        GOODPEAKCOUNT="goodPeakCount",
        MEDMZ="medMz",
        MEDRT="medRt",
        MAXQUALITY="maxQuality",
        ISOTOPELABEL="isotopeLabel",
        ORIGCOMPOUND="compound",
        CORRCOMPOUND="Compound",
        EXPECTEDRTDIFF="expectedRtDiff",
        PPMDIFF="ppmDiff",
        PARENT="parent",
        CLABEL="C_Label",
    )

    OrigDataRequiredHeaders = {
        "Original": [
            "FORMULA",
            "MEDMZ",
            "MEDRT",
            "ISOTOPELABEL",
            "ORIGCOMPOUND",
        ],
        "Corrected": [
            "CORRCOMPOUND",
            "CLABEL",
        ],
    }

    # This is the union of all sheets' column types
    OrigDataColumnTypes: Dict[str, type] = {
        "FORMULA": str,
        "MEDMZ": float,
        "MEDRT": float,
        "ISOTOPELABEL": str,
        "ORIGCOMPOUND": str,
        "CORRCOMPOUND": str,
        "CLABEL": int,
    }

    # These attributes are defined in the order in which they are applied

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

    condense_columns_dict = {
        "Original": {
            "header_column": "Sample Header",
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
            "header_column": "Sample Header",
            "value_column": "Corrected Abundance",
            "uncondensed_columns": [
                "Compound",
                "C_Label",
                # The adductName column only ends up in this sheet due to earlier versions of Accucor not being aware
                # that El Maven added it in later versions.
                "adductName",
            ],
        },
    }

    # Merge happens after column add and condense, but before column rename, so it refers to added and condensed (final)
    # column names and original (un-renamed) column names.
    merge_dict = {
        "first_sheet": "Corrected",  # This key only occurs once in the outermost dict
        "next_merge_dict": {
            # Note, if adductName is erroneously in both sheets, merging will duplicate it with _x and _y suffixes
            "on": ["Compound", "C_Label", "Sample Header"],
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

    nan_defaults_dict = {
        "Raw Abundance": 0,
        "Corrected Abundance": 0,
        "medMz": 0,
        "medRt": 0,
        "isotopeLabel": lambda df: "C13-label-" + df["C_Label"].astype(str),
    }

    sort_columns = ["Sample Header", "Compound", "C_Label"]

    nan_filldown_columns = ["formula"]

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
    }

    merged_drop_columns_list = [
        "compound",
        "adductName",  # In case only (correctly) present in the Original sheet (depending on El Maven version)
        "adductName_x",  # In case present in both Original and Corrected sheets (depending on El Maven/Accucor version)
        "adductName_y",  # In case present in both Original and Corrected sheets (depending on El Maven/Accucor version)
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


class IsoautocorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an accucor excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "isoautocorr"

    OrigDataTableHeaders = namedtuple(
        "OrigDataTableHeaders",
        [
            "COMPOUNDID",
            "FORMULA",
            "LABEL",
            "METAGROUPID",
            "GROUPID",
            "GOODPEAKCOUNT",
            "MEDMZ",
            "MEDRT",
            "MAXQUALITY",
            "ISOTOPELABEL",
            "COMPOUND",
            "EXPECTEDRTDIFF",
            "PPMDIFF",
            "PARENT",
        ],
    )

    OrigDataHeaders = OrigDataTableHeaders(
        COMPOUNDID="compoundId",
        FORMULA="formula",
        LABEL="label",
        METAGROUPID="metaGroupId",
        GROUPID="groupId",
        GOODPEAKCOUNT="goodPeakCount",
        MEDMZ="medMz",
        MEDRT="medRt",
        MAXQUALITY="maxQuality",
        ISOTOPELABEL="isotopeLabel",
        COMPOUND="compound",
        EXPECTEDRTDIFF="expectedRtDiff",
        PPMDIFF="ppmDiff",
        PARENT="parent",
    )

    OrigDataRequiredHeaders = {
        # Raw abundances are optional, so no original
        "cor_pct": [
            "FORMULA",
            "MEDMZ",
            "MEDRT",
            "ISOTOPELABEL",
            "COMPOUND",
        ],
    }

    # This is the union of all sheets' column types
    OrigDataColumnTypes: Dict[str, type] = {
        "FORMULA": str,
        "MEDMZ": float,
        "MEDRT": float,
        "ISOTOPELABEL": str,
        "COMPOUND": str,
    }

    uncondensed_columns = [
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
    ]

    # These attributes are defined in the order in which they are applied

    add_columns_dict = None

    condense_columns_dict = {
        "original": {
            "header_column": "Sample Header",
            "value_column": "Raw Abundance",
            "uncondensed_columns": uncondensed_columns,
        },
        "cor_pct": {
            "header_column": "Sample Header",
            "value_column": "Corrected Abundance",
            "uncondensed_columns": uncondensed_columns,
        },
    }

    # Merge happens after column add and condense, but before column rename, so it refers to added and condensed (final)
    # column names and original (un-renamed) column names.
    merge_dict = {
        "first_sheet": "cor_pct",  # This key only occurs once in the outermost dict
        "next_merge_dict": {
            "on": [
                *uncondensed_columns,  # All these are common between sheets.  Including them here prevents duplication.
                "Sample Header",  # From condense
            ],
            "left_columns": None,  # all
            "right_sheet": "original",
            "right_columns": [],  # There are no additional columns we want - only the condensed raw abundances
            "how": "left",
            "next_merge_dict": None,
        },
    }

    nan_defaults_dict = {
        "Raw Abundance": 0,
        "Corrected Abundance": 0,
        "medMz": 0,
        "medRt": 0,
    }

    sort_columns = None
    nan_filldown_columns = None

    merged_column_rename_dict = {
        "formula": "Formula",
        "medMz": "MedMz",
        "medRt": "MedRt",
        "isotopeLabel": "IsotopeLabel",
        "compound": "Compound",
    }

    merged_drop_columns_list = [
        "label",
        "metaGroupId",
        "groupId",
        "goodPeakCount",
        "maxQuality",
        "adductName",
        "compoundId",
        "expectedRtDiff",
        "ppmDiff",
        "parent",
    ]


class UnicorrLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that defines the universal format.

    This concrete class does no conversion, so all of the attributes are None.
    """

    format_code = "unicorr"

    OrigDataTableHeaders = PeakAnnotationsLoader.DataTableHeaders
    OrigDataHeaders = PeakAnnotationsLoader.DataHeaders
    OrigDataColumnTypes = PeakAnnotationsLoader.DataColumnTypes
    OrigDataRequiredHeaders = {
        # This works because PeakAnnotationsLoader.DataRequiredHeaders happens to be a 1 dimensional list
        PeakAnnotationsLoader.DataSheetName: PeakAnnotationsLoader.DataRequiredHeaders,
    }

    add_columns_dict = None
    condense_columns_dict = None
    # This is the only one we need to define, in case multiple sheets are provided.  E.g. if the user adds a defaults
    # sheet.
    merge_dict = {
        "first_sheet": PeakAnnotationsLoader.DataSheetName,
        "next_merge_dict": None,
    }
    nan_defaults_dict = None
    sort_columns = None
    nan_filldown_columns = None
    merged_column_rename_dict = None
    merged_drop_columns_list = None
