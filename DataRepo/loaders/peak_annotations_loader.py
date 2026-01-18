import os
import re
from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Dict, List, Optional

import pandas as pd
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction
from django.db.utils import ProgrammingError

from DataRepo.loaders.base.converted_table_loader import ConvertedTableLoader
from DataRepo.loaders.base.table_column import TableColumn
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.samples_loader import SamplesLoader
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
    ComplexPeakGroupDuplicate,
    ConditionallyRequiredArgs,
    DuplicateCompoundIsotopes,
    DuplicatePeakGroup,
    DuplicatePeakGroupResolutions,
    DuplicateValues,
    InfileError,
    IsotopeStringDupe,
    MissingC12ParentPeak,
    MissingCompounds,
    MissingSamples,
    MultiplePeakGroupRepresentation,
    MultipleRecordsReturned,
    NoPeakAnnotationDetails,
    NoSamples,
    NoTracerLabeledElements,
    ObservedIsotopeParsingError,
    ObservedIsotopeUnbalancedError,
    ProhibitedCompoundName,
    ProhibitedStringValue,
    RecordDoesNotExist,
    ReplacingPeakGroupRepresentation,
    RollbackException,
    TechnicalPeakGroupDuplicate,
    UnexpectedLabel,
    UnexpectedSamples,
    UnskippedBlanks,
    generate_file_location_string,
)
from DataRepo.utils.file_utils import is_excel, string_to_date
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
        ISOTOPELABEL_KEY,
        COMPOUND_KEY,
        SAMPLEHEADER_KEY,
        CORRECTED_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = [
        ISOTOPELABEL_KEY,
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
            name=DataHeaders.CORRECTED, field=PeakData.corrected_abundance
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

    name_fix_suggestion = (
        f"You may choose to manually edit the automatically fixed compound name in the peak annotation file, but be "
        f"sure to also fix any occurrences in the '{CompoundsLoader.DataHeaders.NAME}' and/or "
        f"'{CompoundsLoader.DataHeaders.SYNONYMS}' columns of the '{CompoundsLoader.DataSheetName}' sheet as well."
    )

    def __init__(self, *args, multrep_suggestion=None, **kwargs):
        """Constructor.

        *NOTE: This constructor requires the file argument (which is an optional argument to the superclass) if the df
        argument is supplied.

        Limitations:
            1. Custom headers for the peak annotation details file are not (yet) supported.  Only the class defaults of
                the MSRunsLoader are allowed.
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
                filename (Optional[str]): In case the (superclass arg) "file" is a temp file with a nonsense name.
                Sample, MSRunSequence, and mzXML data:
                    peak_annotation_details_file (Optional[str]): The filepeth that the Peak Annotation Details came
                        from.
                    peak_annotation_details_filename (Optional[str]): The name of the file that the Peak Annotation
                        Details came from.
                    peak_annotation_details_sheet (Optional[str]): The name of the sheet that the Peak Annotation
                        Details came from (if it was an excel file).
                    peak_annotation_details_df (Optional[pandas DataFrame]): The DataFrame of the Peak Annotation
                        Details sheet/file that will be supplied to the MSRunsLoader class (that is an instance member
                        of this instance).
                MSRunSequence defaults:
                    operator (Optional[str]): The researcher who ran the mass spec.  Mutually exclusive with defaults_df
                        (when it has a default for the operator column for the Sequences sheet).
                    lc_protocol_name (Optional[str]): Name of the liquid chromatography method.  Mutually exclusive with
                        defaults_df (when it has a default for the lc_protocol_name column for the Sequences sheet).
                    instrument (Optional[str]): Name of the mass spec instrument.  Mutually exclusive with defaults_df
                        (when it has a default for the instrument column for the Sequences sheet).
                    date (Optional[str]): Date the Mass spec instrument was run.  Format: YYYY-MM-DD.  Mutually
                        exclusive with defaults_df (when it has a default for the date column for the Sequences sheet).
                PeakGroup conflicts (a.k.a. "multiple representations"):
                    peak_group_conflicts_file (Optional[str]): The name of the file that the Peak Group conflict
                        resolutions came from.
                    peak_group_conflicts_sheet (Optional[str]): The name of the sheet that the Peak Group conflict
                        resolutions came from (if it was an excel file).
                    peak_group_conflicts_df (Optional[pandas DataFrame]): The DataFrame of the Peak Group conflict
                        resolutions sheet/file that will be supplied to the PeakGroupConflicts class (that is an
                        instance member of this instance) and is used to skip peak groups based on user selections.
                multrep_suggestion (Optional[str]): A description of what to do if you encounter a
                    MultiplePeakGroupRepresentation exception, which will be appended to the text of those exceptions.
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ConditionallyRequiredArgs
        Returns:
            None
        """
        # This avoids circular import:
        from DataRepo.loaders.msruns_loader import MSRunsLoader
        from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts

        # Custom options for the MSRunsLoader member instance.
        self.peak_annotation_details_file = kwargs.pop(
            "peak_annotation_details_file", None
        )
        padfn = (
            None
            if self.peak_annotation_details_file is None
            else os.path.basename(self.peak_annotation_details_file)
        )
        self.peak_annotation_details_filename = kwargs.pop(
            "peak_annotation_details_filename", padfn
        )
        self.peak_annotation_details_sheet = kwargs.pop(
            "peak_annotation_details_sheet", None
        )
        self.peak_annotation_details_df = kwargs.pop("peak_annotation_details_df", None)

        # Peak Group Conflict resolutions selected by the user.
        self.peak_group_conflicts_file = kwargs.pop("peak_group_conflicts_file", None)
        self.peak_group_conflicts_sheet = kwargs.pop("peak_group_conflicts_sheet", None)
        self.peak_group_conflicts_df = kwargs.pop("peak_group_conflicts_df", None)

        # A suggestion of how to resolve MultiplePeakGroupRepresentation exceptions
        self.multrep_suggestion = multrep_suggestion

        # Require the file argument if df is supplied
        if kwargs.get("file") is None and (
            kwargs.get("df") is not None
            or self.peak_annotation_details_df is not None
            or self.peak_group_conflicts_df is not None
        ):
            debug = kwargs.get("debug") or False
            raise AggregatedErrors(debug=debug).buffer_error(
                ConditionallyRequiredArgs(
                    "The [file] argument is required if either the [df], [peak_annotation_details_df], or "
                    "[peak_group_conflicts_df] argument is supplied."
                )
            )

        # The MSRunsLoader member instance is used for 2 purposes:
        # 1. Obtain/process the MSRunSequence defaults (it uses the SequencesLoader).
        # 2. Obtain the previously loaded MSRunSample records mapped to sample names (when different from the headers).
        self.msrunsloader = MSRunsLoader(
            file=self.peak_annotation_details_file,
            filename=self.peak_annotation_details_filename,
            data_sheet=self.peak_annotation_details_sheet,
            df=self.peak_annotation_details_df,
            defaults_df=kwargs.get("defaults_df"),
            defaults_file=kwargs.get("defaults_file"),
            operator=kwargs.pop("operator", None),
            date=kwargs.pop("date", None),
            lc_protocol_name=kwargs.pop("lc_protocol_name", None),
            instrument=kwargs.pop("instrument", None),
            skip_mzxmls=True,
        )

        # Example: self.peak_group_selections[sample][pgname.lower()]["filename"] = selected_peak_annotation_filename
        self.peakgroupconflicts = PeakGroupConflicts(
            file=self.peak_group_conflicts_file,
            data_sheet=self.peak_group_conflicts_sheet,
            df=self.peak_group_conflicts_df,
            defaults_df=kwargs.get("defaults_df"),
            defaults_file=kwargs.get("defaults_file"),
        )
        self.peak_group_selections = (
            self.peakgroupconflicts.get_selected_representations()
        )

        try:
            # Convert the supplied df using the derived class.
            # Cannot call super().__init__() because ABC.__init__() takes a custom argument
            ConvertedTableLoader.__init__(self, *args, **kwargs)
        except AggregatedErrors:
            # Whenever ConvertedTableLoader raises an AggregatedErrors object, it is raising
            # self.aggregated_errors_object, so there is no need to merge
            # The conversion failed, so we cannot proceed
            raise self.aggregated_errors_object
        except Exception as e:
            # The conversion failed, so we cannot proceed
            raise self.aggregated_errors_object.buffer_error(e)

        if self.df is not None:
            # Sort the dataframe by sample header, compound, and isotope label - then reset the index.
            # This is important for the fill-down.
            self.df.sort_values(
                by=[
                    self.headers.SAMPLEHEADER,
                    self.headers.COMPOUND,
                    self.headers.ISOTOPELABEL,
                ],
                inplace=True,
            )
            self.df.reset_index(inplace=True, drop=True)

        # Check that every compound has a valid C12 PARENT row.  Must do this after conversion in order to have an
        # aggregated_errors_object attribute to buffer warnings in
        self.check_c12_parents(self.orig_df)

        # Initialize the MSRun Sample and Sequence data (obtained from self.msrunsloader)
        self.operator_default = None
        self.date_default = None
        self.lc_protocol_name_default = None
        self.instrument_default = None
        self.msrun_sample_dict = {}
        self.missing_annot_file_details = {}
        self.initialize_msrun_data()

        # Error tracking/reporting - the remainder of this init (here, down) is all about error tracking/reporting.

        # If the sample that a header maps to is missing, track it so we don't raise that error multiple times.
        self.missing_headers_as_samples = []

        # For referencing the compounds sheet in errors about missing compounds
        if self.peak_annotation_details_filename is None:
            self.compounds_loc = generate_file_location_string(
                file="the study excel file",
                sheet=CompoundsLoader.DataSheetName,
            )
        elif is_excel(self.peak_annotation_details_filename):
            self.compounds_loc = generate_file_location_string(
                file=self.peak_annotation_details_filename,
                sheet=CompoundsLoader.DataSheetName,
            )
        else:
            self.compounds_loc = generate_file_location_string(
                file=self.peak_annotation_details_filename,
            )

        # Compound lookup is slow and compounds are repeatedly looked up, so this will buffer the results
        self.compound_lookup = {}

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

        # Consume any errors buffered by PeakGroupConflicts
        self.aggregated_errors_object.merge_aggregated_errors_object(
            self.peakgroupconflicts.aggregated_errors_object
        )

    @abstractmethod
    def check_c12_parents(self, orig_df):
        """This is an abstract method that must be implemented in the derived class.  It should look for compounds that
        are missing a C12 PARENT row.

        Args:
            orig_df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
        Exceptions:
            Raises:
                None
            Buffers:
                MissingC12ParentPeak: Example:
                    self.aggregated_errors_object.buffer_error(
                        MissingC12ParentPeak(
                            compound,
                            file=self.friendly_file,
                            # The sheet, column, and row numbers are irrelevant in the converted format
                        )
                    )
        Returns:
            None
        """
        pass

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
        self.missing_annot_file_details = {}
        if self.peak_annotation_details_df is not None:
            self.msrun_sample_dict = self.msrunsloader.get_loaded_msrun_sample_dict(
                peak_annot_file=self.friendly_file  # The name in the sheet will be the friendly one
            )
            # Keep track of what has been retrieved
            for sh in self.msrun_sample_dict.keys():
                self.msrun_sample_dict[sh]["seen"] = False

            # If the peak annotation details sheet was provided (the enclosing conditional), but nothing was retrieved
            # for this peak annotations file, buffer a NoPeakAnnotationDetails error and track the missing file.
            if len(self.msrun_sample_dict.keys()) == 0:
                self.missing_annot_file_details[self.get_friendly_filename()] = True
                self.aggregated_errors_object.buffer_warning(
                    NoPeakAnnotationDetails(
                        self.get_friendly_filename(),  # Peak annot file
                        file=self.msrunsloader.friendly_file,  # Study doc
                        sheet=self.msrunsloader.DataSheetName,
                        column=self.msrunsloader.DataHeaders.ANNOTNAME,
                    ),
                    is_fatal=self.validate,
                )

        # Remove exceptions about Sample table search failures generated by the MSRunsLoader.  Those are redundant
        # because they are already generated when that loader loads its sheet, so we don't need to repeat them in this
        # loader.
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
            self.date_default = string_to_date(self.msrunsloader.date_default)
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
        # TODO: Remove this after implementing issue #1387
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
            cmpdrecs_dict = self.get_peak_group_compounds_dict(row=row)

            # Get or create PeakGroups
            try:
                pgrec, _ = self.get_or_create_peak_group(
                    row, annot_file_rec, cmpdrecs_dict
                )
            except RollbackException:
                pass

            # Get or create a linking table record between pgrec and each cmpd_rec (compounds with the same formula)
            for cmpdrec in cmpdrecs_dict.values():
                try:
                    self.get_or_create_peak_group_compound_link(pgrec, cmpdrec)
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

            # If label_observations is None, it is because there was an error parsing the isotope string, but the counts
            # are only updated when label_observations is populated, so increment a skipped count for each label model.
            # Note that if label_observations is empty, it can be inferred to be the parent (with no labels).
            if label_observations is None:
                self.skipped(PeakDataLabel.__name__)
                self.skipped(PeakGroupLabel.__name__)
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

        # This assumes that if rollback is deferred, that the caller has disabled caching updates and that they should
        # remain disabled so that the caller can enable them when it is done.
        # TODO: Remove this after implementing issue #1387
        if not self.defer_rollback:
            enable_caching_updates()
            if not self.dry_run and not self.validate:
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

    def get_peak_group_compounds_dict(
        self, row=None, names_str=None, buffer_errors=True, fix_elmaven_compounds=True
    ) -> Dict[str, Optional[Compound]]:
        """Retrieve the peak group name and compound records.

        Args:
            row (Optional[pandas.Series])
            names_str (Optional[str]): Delimited compound names and/or synonyms.  Use this if you are calling this
                method from outside of a load process and do not have a row.
            buffer_errors (bool): Set this to False if you do not need errors to attach to cells of the excel
                spreadsheet, because skipping error buffering is faster.
            fix_elmaven_compounds (bool) [True]: Call fix_elmaven_compound on each row/names_str compound to remove
                version strings that ELMaven appends, e.g. " (1)".
        Exceptions:
            None
        Returns:
            recs (Dict[str, Optional[Compound]]): A dict of compound records keyed on the compound synonyms parsed from
                the peakgroup name
        """
        if (row is None) == (names_str is None):
            raise ProgrammingError(
                "row and names_str are mutually exclusive and 1 is required."
            )

        recs: Dict[str, Optional[Compound]] = {}

        if names_str is None:
            names_str = self.get_row_val(row, self.headers.COMPOUND)

        if fix_elmaven_compounds:
            names_str = self.fix_elmaven_compound(names_str)

        # If the names_str is still None
        if names_str is None:
            # An error about required missing values would have already been buffered, so just return an empty dict to
            # avoid an exception from the code below, which assumes non-None
            return recs

        pgnames = []
        for ns in names_str.split(PeakGroup.NAME_DELIM):
            name = ns.strip()
            try:
                Compound.validate_compound_name(name)
            except ProhibitedStringValue as pc:
                name = Compound.validate_compound_name(name, fix=True)
                self.aggregated_errors_object.buffer_warning(
                    ProhibitedCompoundName(
                        pc.found,
                        value=pc.value,
                        disallowed=pc.disallowed,
                        fixed=name,
                        file=self.friendly_file,
                        sheet=self.sheet,
                        column=self.headers.COMPOUND,
                        rownum=self.rownum,
                        suggestion=self.name_fix_suggestion,
                    ),
                    is_fatal=self.validate,
                    orig_exception=pc,
                )
            pgnames.append(name)

        for compound_synonym in pgnames:
            recs[compound_synonym] = self.get_compound(
                compound_synonym, buffer_errors=buffer_errors
            )

        return recs

    def get_compound(self, name, buffer_errors=True):
        """Cached compound lookups.  Loading of a study was profiled and Compound.compound_matching_name_or_synonym was
        found to be a bottleneck (particularly when there are repeated lookups of compunds that don't exist in the
        database), so this method utilizes a "cache" in the form of the self.compound_lookup dict, which saves the
        result of every lookup to return the cached results, if they exist.

        Args:
            name (str): Compound name or synonym.
            buffer_errors (bool): Set this to False if you do not need errors to attach to cells of the excel
                spreadsheet, because skipping error buffering is faster.
        Exceptions:
            Raises:
                None
            Buffers:
                RecordDoesNotExist
        Returns:
            rec (Compound)
        """
        rec = None
        exc = None
        query = None
        if name in self.compound_lookup.keys():
            rec, exc, query = self.compound_lookup[name]
        else:
            try:
                rec = Compound.compound_matching_name_or_synonym(name)
            except (ValidationError, ObjectDoesNotExist) as cmpderr:
                exc = cmpderr
                query = Compound.get_name_query_expression(name)

        if rec is None and buffer_errors:
            self.aggregated_errors_object.buffer_error(
                RecordDoesNotExist(
                    Compound,
                    query,
                    file=self.friendly_file,
                    sheet=self.sheet,
                    column=self.headers.COMPOUND,
                    rownum=self.rownum,
                ),
                orig_exception=exc,
            )

        self.compound_lookup[name] = rec, exc, query

        return rec

    @transaction.atomic
    def get_or_create_peak_group(
        self, row, peak_annot_file, compound_recs_dict: Dict[str, Compound]
    ):
        """Get or create a PeakGroup Record.  Handles exceptions, updates stats, and triggers a rollback.

        Args:
            row (pandas.Series)
            peak_annot_file (Optional[ArchiveFile]): The ArchiveFile record for self.file
            compound_recs_dict (Dict[str, Compound]): Dict of Compound records keyed on the compound synonyms that were
                parsed from the supplied peak group name.
        Exceptions:
            Buffers:
                InfileError
            Raises:
                RollbackException
        Returns:
            rec (Optional[PeakGroup])
            created (boolean)
        """
        sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
        formula = self.get_row_val(row, self.headers.FORMULA)
        compound_synonyms = list(compound_recs_dict.keys())

        msrun_sample = self.get_msrun_sample(sample_header)

        if (
            msrun_sample is None
            or len(compound_synonyms) == 0
            or peak_annot_file is None
            or self.is_skip_row()
        ):
            self.skipped(PeakGroup.__name__)
            return None, False

        # We use the synonym provided (via the keys in the dict) because each synonym may represent a significant
        # difference from the primary compound name, e.g. it could be a specific stereoisomer.  However, we order the
        # names for consistency and searchability.
        pgname = PeakGroup.compound_synonyms_to_peak_group_name(compound_synonyms)

        # The formula can be None if loading just the Accucor Corrected sheet.  In this instance, we can retrieve the
        # formula from the Compound record.
        if formula is None:
            # Arbitrarily grab the first compound formula (assuming all have the same formula)
            first_compound = list(compound_recs_dict.values())[0]
            if first_compound is not None:
                formula = first_compound.formula
            # If the formula is still None
            if formula is None:
                self.buffer_infile_exception(
                    f"No compound formula available for peak group {pgname} in %s."
                )
                self.add_skip_row_index()
                self.errored(PeakGroup.__name__)
                return None, False

        # If the peak annotation file for this PeakGroup is a part of a multiple representation and is not the selected
        # one, skip its load.  (Note, this updates counts and deletes an unselected PeakGroup if it was previously
        # loaded.)
        # BUG: I think there may be a bug here.  The below conditional only ever allows a PeakGroup to be loaded when it
        # BUG: has been *selected*.  If there is no selected peak group (i.e. a row is missing from the conflicts
        # BUG: sheet), I think it's likely that this conditional will never allow any representation of a peak group to
        # BUG: load, because there is no line for it in the peak group conflicts sheet.
        if not self.is_selected_peak_group(pgname, peak_annot_file, msrun_sample):
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
        except MultiplePeakGroupRepresentation as mpgr:
            self.aggregated_errors_object.buffer_error(
                mpgr.set_formatted_message(suggestion=self.multrep_suggestion)
            )
            self.errored(PeakGroup.__name__)
            raise RollbackException()
        except DuplicatePeakGroup as dpg:
            self.aggregated_errors_object.buffer_warning(
                dpg.set_formatted_message(
                    file=self.friendly_file,
                    sheet=self.DataSheetName,
                    rownum=self.rownum,
                ),
                is_fatal=False,
            )
            self.warned(PeakGroup.__name__)
            # We are going to rollback and simply ignore the creation of this record.
            raise RollbackException()
        except (ComplexPeakGroupDuplicate, TechnicalPeakGroupDuplicate) as tpgd:
            self.aggregated_errors_object.buffer_error(
                tpgd.set_formatted_message(
                    file=self.friendly_file,
                    sheet=self.DataSheetName,
                    rownum=self.rownum,
                ),
            )
            self.errored(PeakGroup.__name__)
            raise RollbackException()
        except NoTracerLabeledElements as ntle:
            self.buffer_infile_exception(
                ntle,
                is_error=False,
                is_fatal=self.validate,
                column=self.headers.COMPOUND,
                suggestion=f"The load of peak group '{pgname}' for sample {msrun_sample.sample.name} will be skipped.",
            )
            self.errored(PeakGroup.__name__)
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, PeakGroup, rec_dict)
            self.errored(PeakGroup.__name__)
            raise RollbackException()

        return rec, created

    def is_selected_peak_group(
        self, pgname: str, peak_annot_file: ArchiveFile, msrun_sample: MSRunSample
    ):
        """This handles peak group conflicts and returns False if the peak group should be skipped.  This updates the
        stats and deletes previously loaded PeakGroups that will be replaced.

        If a peakgroup is not a part of a conflict, it returns True (as if it was selected to be loaded).

        It's notable that we could end up here multiple times for each compound, if there are more than 2 peak
        annotation files containing the compound.  This means that there can be multiple checks of the DB for
        "unselected" and previously loaded versions of that compound, but it will only be attempted to be deleted once.
        If this turns out to be a speed issue, the speed might be able to be addressed by doing 1 bulk query in the
        __init__ method.

        Args:
            pgname (str): The name of the peak group to be loaded.
            peak_annot_file (ArchiveFile): The peak annotation file that the peak group to be loaded came from.
            msrun_sample (MSRunSample): The MSRunSample of the peak group to be loaded.
        Exceptions:
            Buffers:
                ProgrammingError
                ReplacingPeakGroupRepresentation
            Raises:
                None
        Returns:
            is_selected (bool): True if the PeakGroup load should be skipped.
        """
        is_selected = True
        # If this PeakGroup is a part of a conflict (because peak_group_selections has this sample and peak group name)
        if (
            # The sample has selected peak groups
            msrun_sample.sample.name in self.peak_group_selections.keys()
            # The peak group name is a selected peak group
            and pgname.lower()
            in self.peak_group_selections[msrun_sample.sample.name].keys()
        ):
            # If the filename is None, it means there was an error in PeakGroupConflicts
            if (
                self.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
                    "filename"
                ]
                is None
            ):
                if not self.peakgroupconflicts.aggregated_errors_object.exception_exists(
                    DuplicatePeakGroupResolutions,
                    attr_name="conflicting",
                    attr_val=True,
                ):
                    self.aggregated_errors_object.buffer_error(
                        ProgrammingError(
                            "Expected DuplicatePeakGroupResolutions exception missing."
                        )
                    )
                self.errored(PeakGroup.__name__)
                # Since there is an error, there's nothing to do, but we will return True so that the PeakGroup is not
                # loaded.
                is_selected = False
                return is_selected

            # If the peak annotation file for this Peak Group is not the selected one, skip its load (and delete it if
            # it was previously loaded).
            if (
                # The file with the selected peak group doesn't match this file's name
                peak_annot_file.filename
                != self.peak_group_selections[msrun_sample.sample.name][pgname.lower()][
                    "filename"
                ]
            ):
                # Note that this skipped PeakGroup COULD already exist in the DB if it was loaded from a previous study
                # submission.  If this is the case, the attempt to load the selected one would raise a
                # MultiplePeakGroupRepresentation exception below, but we don't want an exception to be raised during a
                # save when we want to solve it by deletion, so we handle it here, preemptively by checking for
                # conflicts before the creation attempt.

                # Query for peak groups for the same sample and compound that come from this unselected file.
                conflicting_pgrecs = PeakGroup.objects.filter(
                    msrun_sample__sample__name=msrun_sample.sample.name,
                    name__iexact=pgname,
                    peak_annotation_file=peak_annot_file,
                )

                # Each sample is only allowed a single version of every peak group.
                # We are skipping this peak group for this sample because the user selected a different file to load it
                # from.
                if conflicting_pgrecs.count() == 0:
                    self.skipped(PeakGroup.__name__)

                is_selected = False
            else:
                # Note that other PeakGroups COULD already exist in the DB if it was loaded from a previous study
                # submission, but those would not be in this load.  If this is the case, the attempt to load the
                # selected one would raise a MultiplePeakGroupRepresentation exception below, but we don't want an
                # exception to be raised during a save when we want to solve it by deletion, so we handle it here,
                # preemptively by checking for conflicts before the creation attempt.

                # Query for peak groups for the same sample and compound that come from this unselected file.
                conflicting_pgrecs = PeakGroup.objects.filter(
                    msrun_sample__sample__name=msrun_sample.sample.name,
                    name__iexact=pgname,
                ).exclude(peak_annotation_file=peak_annot_file)

                is_selected = True

            # There's probably only 1 record, if any, but that's only because of code (the clean method).  The DB allows
            # multiple, so we will loop just to guarantee this works even if the DB was manipulated manually.
            for conflicting_pgrec in conflicting_pgrecs:
                self.aggregated_errors_object.buffer_exception(
                    ReplacingPeakGroupRepresentation(
                        conflicting_pgrec,
                        self.peak_group_selections[msrun_sample.sample.name][
                            pgname.lower()
                        ]["filename"],
                        file=self.peakgroupconflicts.friendly_file,
                        sheet=self.peakgroupconflicts.sheet,
                        rownum=self.peak_group_selections[msrun_sample.sample.name][
                            pgname.lower()
                        ]["rownum"],
                    ),
                    is_error=False,
                    is_fatal=self.validate,
                )
                delete_counts_dict = tuple(conflicting_pgrec.delete())[1]
                for qual_mdl_name, cnt in delete_counts_dict.items():
                    mdl_name = list(qual_mdl_name.split("."))[-1]
                    self.deleted(mdl_name, cnt)

        return is_selected

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

        Assumptions:
            1. If a placeholder record exists, then there are multiple concrete records.  This is in reference to the
               business logic in the MSRunsLoader that says that if there is only 1 concrete record, PeakGroup records
               will link directly to it.  Otherwise, a placeholder record is created and PeakGroup records link to it
               for any particular sample.
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

            print(f"TTT self.msrun_sample_dict[{sample_header}]: {self.msrun_sample_dict[sample_header]}")

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
        query_dict = {"sample__name": sample.name}
        msrun_samples = MSRunSample.objects.filter(**query_dict)

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

        # At this point, there are multiple results.  That means 1 of 3 things.  Either:
        # 1. There exists a mix of concrete and placeholder MSRunSample records
        # 2. This sample was included in multiple sequnces, which would mean we could solve it by including the
        #    sequence in the query and that defaults for the sequence are required in order to proceed, so check them.
        # 3. This sample was included in multiple sequences AND there exist both placeholder and concrete records.

        # Keep track of whether we have all sequence metadata defaults
        num_sequence_defaults = 0
        has_all_sequence_defaults = False

        # First, let's account for placeholder versus concrete by checking for a placeholder.  Note, code in the
        # MSRunsLoader conditionally links to concrete records if there is only 1 concrete record, but we are going to
        # assume that if a placeholder exists, then there are multiple concrete records.
        placeholder_query_dict = query_dict.copy()
        placeholder_query_dict["ms_data_file__isnull"] = True
        placeholder_msrun_samples = msrun_samples.filter(**placeholder_query_dict)
        if placeholder_msrun_samples.count() == 1:
            msrun_samples = placeholder_msrun_samples
        else:
            # Now, add the sequence defaults we have to the query dict (NOTE: just the researcher or date, for example,
            # may be enough).
            if self.operator_default is not None:
                query_dict["msrun_sequence__researcher"] = self.operator_default
                num_sequence_defaults += 1
            if self.lc_protocol_name_default is not None:
                query_dict["msrun_sequence__lc_method__name"] = (
                    self.lc_protocol_name_default
                )
                num_sequence_defaults += 1
            if self.instrument_default is not None:
                query_dict["msrun_sequence__instrument"] = self.instrument_default
                num_sequence_defaults += 1
            if self.date_default is not None:
                query_dict["msrun_sequence__date"] = self.date_default
                num_sequence_defaults += 1

            if num_sequence_defaults == 4:
                has_all_sequence_defaults = True

            # Create this exception object (without buffering) to use in 2 possible buffering locations
            not_enough_defaults_exc = ConditionallyRequiredArgs(
                "The arguments supplied to the constructor were insufficient to identify the MS Run Sequence that 1 or "
                "more of the samples belong to.  Either peak_annotation_details_df can be supplied with a sequence "
                "name for every MSRunSample record or the following defaults can be supplied via file_defaults of "
                f"explicit arguments: [operator, lc_protocol_name, instrument, and/or date].  {len(query_dict.keys())} "
                f"defaults supplied were used to create the following query: {query_dict}."
            )

            if num_sequence_defaults == 0:
                # Only buffer this error once
                if not self.aggregated_errors_object.exception_type_exists(
                    ConditionallyRequiredArgs
                ):
                    self.aggregated_errors_object.buffer_error(not_enough_defaults_exc)
                self.missing_headers_as_samples.append(sample_header)
                return None

            # Let's see if the results can be narrowed to a single record using the sequence defaults.
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
            # Now check if this case is both that this sample was in multiple sequences and has both placeholder and
            # concrete records
            placeholder_msrun_samples = msrun_samples.filter(**placeholder_query_dict)
            if placeholder_msrun_samples.count() == 1:
                msrun_samples = placeholder_msrun_samples
            else:
                if not has_all_sequence_defaults:
                    if not self.aggregated_errors_object.exception_type_exists(
                        ConditionallyRequiredArgs
                    ):
                        self.aggregated_errors_object.buffer_error(
                            not_enough_defaults_exc
                        )
                else:
                    try:
                        msrun_samples.get()
                    except MSRunSample.MultipleObjectsReturned as mor:
                        self.aggregated_errors_object.buffer_error(
                            MultipleRecordsReturned(
                                MSRunSample,
                                query_dict,
                                file=self.friendly_file,
                                sheet=self.sheet,
                                rownum=self.rownum,
                            ),
                            orig_exception=mor,
                        )
                    except MSRunSample.DoesNotExist as dne:
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
        self, pgrec: Optional[PeakGroup], cmpd_rec: Optional[Compound]
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
                suggestion="This compound / peak group link will be skipped.",
            )
            self.aggregated_errors_object.buffer_exception(
                ntle,
                is_error=False,
                is_fatal=self.validate,
            )
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
                UnexpectedLabel
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
                # There had to have been errors when trying to retrieve compounds to link, which means that this error
                # is unnecessary (i.e. fixing the previous error would make this error go away), but just to be safe,
                # error if there have been no errors buffered.
                if self.aggregated_errors_object.num_errors == 0:
                    self.aggregated_errors_object.buffer_error(
                        ProgrammingError(
                            "PeakGroup record has no linked compounds.  A peak group must have associated compounds in "
                            "order to confirm label observations."
                        )
                    )
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
        except UnexpectedLabel as olnp:
            suggestion = None
            if pgrec is not None:
                suggestion = (
                    f"Check to make sure animal '{pgrec.msrun_sample.sample.animal.name}' has the correct "
                    "tracer(s)."
                )
            olnp.set_formatted_message(
                **infile_err_args,
                suggestion=suggestion,
            )
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
            RecordDoesNotExist, "model", Sample, is_error=True
        )

        # Separate the exceptions based on whether they appear to be blanks or not
        possible_blank_dnes = []
        likely_missing_dnes = []
        for sdne in sample_dnes:
            if Sample.is_a_blank(sdne.query_obj["name"]):
                # Make the missing blanks exceptions, contained by UnskippedBlanks, into warnings
                sdne.is_error = False
                sdne.is_fatal = self.validate
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
                self.aggregated_errors_object.buffer_exception(
                    NoSamples(
                        likely_missing_dnes,
                        suggestion=self.missing_msrs_suggestion,
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
                        suggestion=self.missing_msrs_suggestion,
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
            if self.get_friendly_filename() in self.missing_annot_file_details.keys():
                suggestion = (
                    "Either a peak annotation details file(/sheet in a study doc) was not supplied of this file "
                    f"'{self.msrunsloader.friendly_file}' is not in the peak annotation details sheet.  This sheet "
                    "must be populated with the sample headers from this file in order to be able to silently skip "
                    "blank samples."
                )
            else:
                suggestion = (
                    f"Either add 'skip' to the '{self.msrunsloader.headers.SKIP}' column in the "
                    f"'{self.msrunsloader.DataSheetName}' sheet for these {self.headers.SAMPLEHEADER}s or add the "
                    f"missing sample(s) to the '{SamplesLoader.DataSheetName}' sheet."
                )
            self.aggregated_errors_object.buffer_warning(
                UnskippedBlanks(
                    possible_blank_dnes,
                    # TODO: Have StudyLoader pass along the peak annotations filename
                    file="the study doc",
                    suggestion=suggestion,
                ),
                is_fatal=self.validate,
            )

        # See if *any* sample headers in the Peak Annotation Details sheet were not found in the peak annotation file.
        sample_headers_in_details_sheet_not_in_peak_annot_file = dict(
            (uss, self.msrun_sample_dict[uss][self.msrunsloader.headers.SKIP])
            for uss in self.msrun_sample_dict.keys()
            if self.msrun_sample_dict[uss]["seen"] is False
            and not Sample.is_a_blank(uss)
        )
        # Set to an error if not all samples are skipped samples
        is_err = not all(
            sample_headers_in_details_sheet_not_in_peak_annot_file.values()
        )
        if len(sample_headers_in_details_sheet_not_in_peak_annot_file.keys()) > 0:
            self.aggregated_errors_object.buffer_exception(
                UnexpectedSamples(
                    list(sample_headers_in_details_sheet_not_in_peak_annot_file.keys()),
                    file=self.friendly_file,
                    rel_file=self.msrunsloader.friendly_file,
                    rel_sheet=self.msrunsloader.DataSheetName,
                    rel_column=(
                        f"{self.msrunsloader.DataHeaders.SAMPLEHEADER} "
                        f"and {self.msrunsloader.DataHeaders.ANNOTNAME}"
                    ),
                ),
                is_error=is_err,
                is_fatal=is_err or self.validate,
            )

        possible_blanks_in_details_sheet_not_in_peak_annot_file = [
            uss
            for uss in self.msrun_sample_dict.keys()
            if self.msrun_sample_dict[uss]["seen"] is False and Sample.is_a_blank(uss)
        ]
        if len(possible_blanks_in_details_sheet_not_in_peak_annot_file) > 0:
            self.aggregated_errors_object.buffer_warning(
                UnexpectedSamples(
                    possible_blanks_in_details_sheet_not_in_peak_annot_file,
                    file=self.friendly_file,
                    rel_file=self.msrunsloader.friendly_file,
                    rel_sheet=self.msrunsloader.DataSheetName,
                    rel_column=(
                        f"{self.msrunsloader.DataHeaders.SAMPLEHEADER} "
                        f"and {self.msrunsloader.DataHeaders.ANNOTNAME}"
                    ),
                    possible_blanks=True,
                ),
                is_fatal=self.validate,
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
        # TODO: Gather information for the user sufficient for them to fix the file format problem, like the way this
        # was done in the analogous method in StudyLoader
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

    @classmethod
    def fix_elmaven_compound(
        cls, compound: Optional[str], pattern=r" \([1-9]+[0-9]*\)$"
    ):
        """Takes a compound name originating from EL-Maven and by default (based on the pattern argument), removes 1
        occurrence of strings like " (1)".

        Args:
            compound (Optional[str])
            pattern (str): A regex.
        Exceptions:
            None
        Returns:
            (Optional[str]): A string with pattern removed, if present.
        """
        return re.sub(pattern, "", compound, count=1) if compound is not None else None


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

    def check_c12_parents(self, _):
        """Not implemented yet (because it doesn't cause a problem, e.g. it doesn't fill the formula down)"""
        # TODO: Look for missing C12 PARENT rows in the Isocorr format
        return


class AccucorLoader(PeakAnnotationsLoader):
    """Derived class of PeakAnnotationsLoader that just defines how to convert an accucor excel file to the format
    accepted by the parent class's load_data method.

    PeakAnnotationsLoader is an abstract base class.  It has a method called convert_df() that uses the data described
    here to automatically convert self.df to the format it accepts.
    """

    format_code = "accucor"

    # Isotopes supported by accucor.  See https://cran.r-project.org/web/packages/accucor/readme/README.html
    AccucorIsotopes = {
        "C": "C13",
        "N": "N15",
        "D": "H2",  # TraceBase needs the mass_number
        "H": "H2",
    }

    @classmethod
    def get_accucor_label_column_name(cls, df: pd.DataFrame):
        """Given a pandas dataframe, it returns the column header of the label column, e.g. 'C_Label'.

        Args:
            df (pd.DataFrame)
        Exceptions:
            None
        Returns:
            matches[0] (str)
        """
        matches = [cn for cn in df.columns if cn.endswith("_Label")]
        return matches[0] if len(matches) == 1 else None

    @classmethod
    def get_accucor_isotope_string(cls, df: pd.DataFrame):
        """Given a pandas dataframe, it returns the isotope portion of the isotopeLabel column, e.g. 'N15'.

        Args:
            df (pd.DataFrame)
        Exceptions:
            ValueError
            KeyError
        Returns:
            matches[0] (str)
        """
        header_matches = [cn for cn in df.columns if cn.endswith("_Label")]
        if len(header_matches) != 1:
            raise ValueError(
                "Unable to identify label column among the accucor corrected column names: "
                f"{list(df.columns)}"
            )
        label_column = header_matches[0]
        element, _ = label_column.split("_")
        if element not in cls.AccucorIsotopes.keys():
            raise KeyError(
                f"Unsupported accucor isotope element '{element}'.  Supported elements are: "
                f"{list(cls.AccucorIsotopes.keys())}."
            )
        return cls.AccucorIsotopes[element]

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
            "NLABEL",
            "HLABEL",
            "DLABEL",
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
        CLABEL="C_Label",  # Only 1 of the X_Label column possibilities will be present
        NLABEL="N_Label",
        HLABEL="H_Label",
        DLABEL="D_Label",
    )

    OrigDataRequiredHeaders = {
        "Corrected": [
            "CORRCOMPOUND",
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
        "NLABEL": int,
        "HLABEL": int,
        "DLABEL": int,
    }

    # These attributes are defined in the order in which they are applied

    add_columns_dict = {
        # Sheet: dict
        "Original": {
            # Rename happens after merge, but before merge, we want matching column names in each sheet, so...
            "Compound": lambda df: df["compound"],
        },
        "Corrected": {
            "isotopeLabel": lambda df: df.apply(
                lambda row: (
                    ""
                    if AccucorLoader.get_accucor_label_column_name(df) is None
                    else (
                        (
                            f"{AccucorLoader.get_accucor_isotope_string(df)}"
                            "-label-"
                            f"{row[AccucorLoader.get_accucor_label_column_name(df)]}"
                        )
                        if row[AccucorLoader.get_accucor_label_column_name(df)] > 0
                        else "C12 PARENT"
                    )
                ),
                axis=1,
            ),
        },
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
            ],
        },
        "Corrected": {
            "header_column": "Sample Header",
            "value_column": "Corrected Abundance",
            "uncondensed_columns": [
                "Compound",
                "isotopeLabel",  # From add_columns_dict
                "C_Label",
                "N_Label",
                "D_Label",
                "H_Label",
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
            "on": ["Compound", "isotopeLabel", "Sample Header"],
            "left_columns": None,  # all
            "right_sheet": "Original",
            "right_columns": [
                "formula",
                "medMz",
                "medRt",
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
        "isotopeLabel": lambda df: (
            ""
            if AccucorLoader.get_accucor_label_column_name(df) is None
            else df.apply(
                lambda row: (
                    (
                        f"{AccucorLoader.get_accucor_isotope_string(df)}"
                        "-label-"
                        f"{row[AccucorLoader.get_accucor_label_column_name(df)]}"
                    )
                    if row[AccucorLoader.get_accucor_label_column_name(df)] > 0
                    else "C12 PARENT"
                ),
                axis=1,
            )
        ),
        # This sets formula (when formula is a NaN) to the ConvertedTableLoader.nan_filldown_stop_str (which is
        # "BACKFILL") so that when nan_filldown_columns is processed, the formula from the compound above will not fill
        # down through the parent is the C12 PARENT row is missing.  It then (as a builtin feature) fills *up* and
        # replaces the BACKFILL value in the process.  See ConvertedTableLoader.fill_down_nan_columns.
        "formula": lambda df: (
            df.apply(
                lambda row: (
                    AccucorLoader.nan_filldown_stop_str
                    if row[AccucorLoader.get_accucor_label_column_name(df)] == 0
                    else ""
                ),
                axis=1,
            )
        ),
    }

    sort_columns = ["Sample Header", "Compound", "isotopeLabel"]

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
        "N_Label",
        "H_Label",
        "D_Label",
    ]

    def check_c12_parents(self, df):
        """This method ensures that every compound in the original peak annotations file had a row for the C12 PARENT.

        Args:
            df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
        Exceptions:
            Raised:
                None
            Buffered:
                MissingC12ParentPeak
        Returns:
            None
        """
        if df is None:
            return

        if isinstance(df, dict) and "Original" in df.keys():
            self.check_c12_parents_original(df["Original"])

        return

    def check_c12_parents_original(self, df: pd.DataFrame):
        """This method ensures that every compound in the original peak annotations file's Original sheet had a row for
        the C12 PARENT.

        Args:
            df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
        Exceptions:
            Raised:
                None
            Buffered:
                MissingC12ParentPeak
        Returns:
            None
        """
        # The accucor files in un-merged csv format do not have a formula and don't need a C12 PARENT check because it
        # will be automatically created from the X_Label column.  (The formula column will also be added from existing
        # records in the DB)
        if self.OrigDataHeaders.FORMULA not in df.columns:
            return

        # Create a dataframe with just the relevant data needed to identify missing C12 PARENT rows in the original data
        # We infer this from the fact that a formula from an above compound filled down into the next compound (because
        # its parent was missing)
        parent_compounds: pd.DataFrame = df[
            df[self.OrigDataHeaders.ISOTOPELABEL] == "C12 PARENT"
        ]
        parent_compounds = parent_compounds[self.OrigDataHeaders.COMPOUNDID]
        parent_compounds = parent_compounds.drop_duplicates()
        uniq_parent_compounds: list = parent_compounds.to_list()

        labeled_compounds: pd.DataFrame = df[
            df[self.OrigDataHeaders.ISOTOPELABEL] != "C12 PARENT"
        ]

        # This creates a column named "index" (and resets the actual index)
        labeled_compounds = labeled_compounds.reset_index()

        # Get unique compound list with their original row index (in an added "index" column)
        # See https://saturncloud.io/blog/how-to-get-the-first-row-of-each-group-in-a-pandas-dataframe/
        labeled_compounds = labeled_compounds.groupby(
            self.OrigDataHeaders.COMPOUNDID, as_index=False
        ).first()

        # Now filter those compounds to only those that are not in the C12 PARENT compounds
        missing_parent_compounds = labeled_compounds[
            ~labeled_compounds[self.OrigDataHeaders.COMPOUNDID].isin(
                uniq_parent_compounds
            )
        ]

        # Create a method that we will apply to the missing_parent_compounds
        def buffer_missing_parents(compound, row_index):
            self.aggregated_errors_object.buffer_warning(
                MissingC12ParentPeak(
                    compound,
                    file=self.friendly_file,
                    sheet="Original",
                    column=self.OrigDataHeaders.ISOTOPELABEL,
                    rownum=row_index + 2,
                ),
                is_fatal=self.validate,
            )

        if not missing_parent_compounds.empty:
            missing_parent_compounds.apply(
                lambda row: buffer_missing_parents(
                    row[self.OrigDataHeaders.COMPOUNDID], row["index"]
                ),
                axis=1,
            )


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
        "cor_abs": [
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
        "cor_abs": {
            "header_column": "Sample Header",
            "value_column": "Corrected Abundance",
            "uncondensed_columns": uncondensed_columns,
        },
    }

    # Merge happens after column add and condense, but before column rename, so it refers to added and condensed (final)
    # column names and original (un-renamed) column names.
    merge_dict = {
        "first_sheet": "cor_abs",  # This key only occurs once in the outermost dict
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

    def check_c12_parents(self, _):
        """Not implemented yet (because it doesn't cause a problem, e.g. it doesn't fill the formula down)"""
        # TODO: Look for missing C12 PARENT rows in the Isoautocorr format
        return


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

    def check_c12_parents(self, _):
        """Not implemented yet (because it doesn't cause a problem, e.g. it doesn't fill the formula down)"""
        # TODO: Look for missing C12 PARENT rows in the Unicorr format
        return
