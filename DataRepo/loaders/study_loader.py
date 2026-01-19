# mypy: disable-error-code="index"

# The above ignore has to do with ConvertedTableLoader's @property/@abstractmethod functions that are overridden using
# class attributes.  Normally, mypy doesn't complain about this (it doesn't complain about all references to that
# attribute, treating it as a dict, or about any of the other attributes, similarly treated), but it was having a
# problem detecting that OrigDataRequiredHeaders in the derived classes were dicts.  Probably it's due to the fact that
# StudyLoader is abtract as well, and it references OrigDataRequiredHeaders via __subclasses__(), and mypy only knows
# about the abstractmethod at that point (not the derived classes that define the type).  (Also note the `type: ignore
# [attr-defined]` below.)

# TODO: Figure out a better way to simulate abstract class attributes (see above comment))

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from copy import deepcopy
from typing import Dict, List, Optional, Type

import pandas as pd
from django.db import ProgrammingError
from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.converted_table_loader import ConvertedTableLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.loaders.lcprotocols_loader import LCProtocolsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.peak_annotations_loader import (
    AccucorLoader,
    IsoautocorrLoader,
    IsocorrLoader,
    PeakAnnotationsLoader,
    UnicorrLoader,
)
from DataRepo.loaders.peak_group_conflicts import PeakGroupConflicts
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.loaders.studies_loader import StudiesLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models.animal import Animal
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.models.infusate import Infusate
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.protocol import Protocol
from DataRepo.models.sample import Sample
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AggregatedErrorsSet,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingStudies,
    AllMissingTissues,
    AllMissingTreatments,
    AllMultiplePeakGroupRepresentations,
    AllUnexpectedLabels,
    AllUnskippedBlanks,
    AnimalsWithoutSamples,
    AnimalsWithoutSerumSamples,
    AnimalWithoutSamples,
    AnimalWithoutSerumSamples,
    BlankRemoved,
    BlanksRemoved,
    ConditionallyRequiredArgs,
    InfileError,
    InvalidStudyDocVersion,
    MissingCompounds,
    MissingModelRecordsByFile,
    MissingRecords,
    MissingSamples,
    MissingStudies,
    MissingTissues,
    MissingTreatments,
    MultiLoadStatus,
    MultiplePeakGroupRepresentations,
    MultipleStudyDocVersions,
    NoSamples,
    PlaceholderAdded,
    PlaceholdersAdded,
    RecordDoesNotExist,
    UnexpectedLabels,
    UnknownStudyDocVersion,
    UnskippedBlanks,
)
from DataRepo.utils.file_utils import (
    datetime_to_string,
    is_excel,
    read_from_file,
    string_to_date,
)
from DataRepo.utils.infusate_name_parser import (
    parse_infusate_name,
    parse_tracer_concentrations,
)

# See: https://stackoverflow.com/q/9134795/2057516 and https://stackoverflow.com/q/53965596/2057516
# This is just warning us that it doesn't read in the data validation formulas, but we don't need them anyway.
warnings.filterwarnings(
    "ignore", message="Data Validation extension is not supported and will be removed"
)


class StudyLoader(ConvertedTableLoader, ABC):
    """Loads an entire study doc (i.e. all of its sheets - not just the Study model)."""

    @property
    @abstractmethod
    def version_number(self) -> str:
        """The version of the study doc"""
        pass

    ConversionHeading = "Study Doc Version Check"

    LatestLoaderName = "StudyV3Loader"

    STUDY_SHEET = "STUDY"
    ANIMALS_SHEET = "ANIMALS"
    SAMPLES_SHEET = "SAMPLES"
    TREATMENTS_SHEET = "TREATMENTS"
    TISSUES_SHEET = "TISSUES"
    INFUSATES_SHEET = "INFUSATES"
    TRACERS_SHEET = "TRACERS"
    COMPOUNDS_SHEET = "COMPOUNDS"
    LCPROTOCOLS_SHEET = "LCPROTOCOLS"
    SEQUENCES_SHEET = "SEQUENCES"
    HEADERS_SHEET = "HEADERS"
    FILES_SHEET = "FILES"
    PGCONFLICTS_SHEET = "PGCONFLICTS"
    DEFAULTS_SHEET = "DEFAULTS"
    ERRORS_SHEET = "ERRORS"

    # Overloading this for sheet keys (not header keys), in load order
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "STUDY",
            "COMPOUNDS",
            "TRACERS",
            "INFUSATES",
            "TREATMENTS",
            "ANIMALS",
            "TISSUES",
            "SAMPLES",
            "LCPROTOCOLS",
            "SEQUENCES",
            "HEADERS",
            "FILES",
            "PGCONFLICTS",
            "DEFAULTS",
            "ERRORS",
        ],
    )

    # Overloading this for sheet names (not header names)
    DataHeaders = DataTableHeaders(
        STUDY=StudiesLoader.DataSheetName,
        ANIMALS=AnimalsLoader.DataSheetName,
        SAMPLES=SamplesLoader.DataSheetName,
        TREATMENTS=ProtocolsLoader.DataSheetName,
        TISSUES=TissuesLoader.DataSheetName,
        INFUSATES=InfusatesLoader.DataSheetName,
        TRACERS=TracersLoader.DataSheetName,
        COMPOUNDS=CompoundsLoader.DataSheetName,
        LCPROTOCOLS=LCProtocolsLoader.DataSheetName,
        SEQUENCES=SequencesLoader.DataSheetName,
        HEADERS=MSRunsLoader.DataSheetName,
        FILES=PeakAnnotationFilesLoader.DataSheetName,
        PGCONFLICTS=PeakGroupConflicts.DataSheetName,
        DEFAULTS="Defaults",
        ERRORS="Errors",
    )

    # Overloading for required sheets
    # Note, in the current version, no 1 sheet is required, so this is empty.
    DataRequiredHeaders: List[str] = []

    # These are unused...
    DataRequiredValues = DataRequiredHeaders
    DataUniqueColumnConstraints: List[list] = []
    FieldToDataHeaderKey: Dict[str, dict] = {}
    DataColumnMetadata = DataTableHeaders(
        STUDY=None,
        ANIMALS=None,
        SAMPLES=None,
        TREATMENTS=None,
        TISSUES=None,
        INFUSATES=None,
        TRACERS=None,
        COMPOUNDS=None,
        LCPROTOCOLS=None,
        SEQUENCES=None,
        HEADERS=None,
        FILES=None,
        PGCONFLICTS=None,
        DEFAULTS=None,
        ERRORS=None,
    )
    Models: List[Model] = []
    # No FieldToDataValueConverter needed

    # TODO: Support for a dict of dataframes should be introduced here by setting the DataSheetName to None
    # Unused (currently)
    DataSheetName = "Study Doc Tabs"

    Loaders = DataTableHeaders(
        STUDY=StudiesLoader,
        ANIMALS=AnimalsLoader,
        SAMPLES=SamplesLoader,
        TREATMENTS=ProtocolsLoader,
        TISSUES=TissuesLoader,
        INFUSATES=InfusatesLoader,
        TRACERS=TracersLoader,
        COMPOUNDS=CompoundsLoader,
        LCPROTOCOLS=LCProtocolsLoader,
        SEQUENCES=SequencesLoader,
        HEADERS=MSRunsLoader,
        FILES=PeakAnnotationFilesLoader,
        PGCONFLICTS=PeakGroupConflicts,
        DEFAULTS=None,
        ERRORS=None,
    )

    # NOTE: The constructor copies this and adds to it
    CustomLoaderKwargs = DataTableHeaders(
        STUDY={},
        ANIMALS={},
        SAMPLES={},
        TREATMENTS={"headers": ProtocolsLoader.DataHeadersExcel},
        TISSUES={},
        INFUSATES={},
        TRACERS={},
        COMPOUNDS={},
        LCPROTOCOLS={},
        SEQUENCES={},
        HEADERS={},
        FILES={},
        PGCONFLICTS={},
        DEFAULTS=None,
        ERRORS=None,
    )

    DataSheetDisplayOrder = [
        STUDY_SHEET,
        TRACERS_SHEET,
        INFUSATES_SHEET,
        ANIMALS_SHEET,
        SAMPLES_SHEET,
        SEQUENCES_SHEET,
        FILES_SHEET,
        HEADERS_SHEET,
        PGCONFLICTS_SHEET,
        TREATMENTS_SHEET,
        TISSUES_SHEET,
        COMPOUNDS_SHEET,
        LCPROTOCOLS_SHEET,
        DEFAULTS_SHEET,
        ERRORS_SHEET,
    ]

    representations_suggestion = (
        "Please select the best peak annotation file to use for each compound in the "
        f"'{PeakGroupConflicts.DataSheetName}' sheet.  If that sheet is hidden or empty, supply all files listed in "
        "the multiple representations errors to the submission start page and copy the sheet to your study doc to "
        "fill out."
    )

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[Dict[str, pandas dataframe]]): Data, e.g. as parsed from an excel file.  *See
                    ConvertedTableLoader.*
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
                annot_files_dict (Optional[Dict[str, str]]): This is a dict of peak annotation file paths keyed on peak
                    annotation file basename.  This is not necessary on the command line.  It is only provided for the
                    purpose of web forms, where the name of the actual file is a randomized hash string at the end of a
                    temporary path.  This dict associates the user's readable filename parsed from the infile (the key)
                    with the actual file (the value).
                skip_mzxmls (bool) [False]: Skips the loading of mzXML file records into the ArchiveFile table.  Note,
                    this also skips the creation of raw file record (but also note that raw files are never actually
                    loaded - what is skipped is the creation of the record representing the raw file).
                exclude_sheets (Optional[List[str]]): A list of default DataSheetNames (i.e. the values in the list must
                    match the value of the in each of the cls.Loaders' DataSheetName class attribute - not any custom
                    sheet name, so that it can be scripted on the data repo).
        Exceptions:
            Raises:
                ProgrammingError
            Buffers:
                None
        Returns:
            None
        """
        # NOTE: self.load_data() requires the file argument to have been provided to this constructor.
        if kwargs.get("defer_rollback") is True:
            raise ProgrammingError(
                "Modifying the following superclass constructor arguments is prohibited by StudyLoader: "
                "[defer_rollback]."
            )

        # Custom dataframe storage for this class, since we want to handle df as a dict of dataframes, but still take
        # advantage of the TableLoader checks on the sheets as if they were columns.
        self.df_dict = None

        # Custom options specific to individual loaders
        self.annot_files_dict = kwargs.pop("annot_files_dict", {})
        self.skip_mzxmls = kwargs.pop("skip_mzxmls", False)
        self.exclude_sheets = kwargs.pop("exclude_sheets", []) or []

        clkwa = self.CustomLoaderKwargs._asdict()
        clkwa["FILES"]["annot_files_dict"] = self.annot_files_dict
        clkwa["HEADERS"]["skip_mzxmls"] = self.skip_mzxmls
        # This occludes the CustomLoaderKwargs class attribute (which we copied and are leaving unchanged)
        # Just note that only the instance has annot_files_dict
        self.CustomLoaderKwargs = self.DataTableHeaders(**clkwa)

        self.missing_study_record_exceptions = []
        self.missing_sample_record_exceptions = []
        self.no_sample_record_exceptions = []
        self.unskipped_blank_record_exceptions = []
        self.missing_tissue_record_exceptions = []
        self.missing_treatment_record_exceptions = []
        self.unexpected_labels_exceptions = []
        self.missing_compound_record_exceptions = []
        self.multiple_pg_reps_exceptions = []
        self.load_statuses = MultiLoadStatus(debug=kwargs.get("debug", False))

        self.derived_loaders = {}
        for derived_class in StudyLoader.__subclasses__():
            self.derived_loaders[derived_class.__name__] = derived_class

        # Convert the supplied df using the derived class.
        # Cannot call super().__init__() because ABC.__init__() takes a custom argument
        ConvertedTableLoader.__init__(self, *args, **kwargs)

        self.load_statuses.init_load(
            # Tell the MultiLoadStatus object we're going to try to load these files
            load_key=[
                self.get_friendly_filename(),
                *list(self.annot_files_dict.keys()),
            ]
        )

        self.check_exclude_sheets()
        self.check_study_class_attributes()

    @classmethod
    def check_study_class_attributes(cls):
        """Basically just error-checks that the sheet display keys are equivalent to the load order keys.

        Args:
            None
        Exceptions:
            Raises:
                ProgrammingError
            Buffers:
                None
        Returns:
            None
        """
        if set(cls.DataTableHeaders._fields) != set(cls.DataSheetDisplayOrder):
            raise ProgrammingError(
                "DataTableHeaders and DataSheetDisplayOrder must have the same sheet keys"
            )

    def check_exclude_sheets(self):
        """This buffers an error if any supplied sheet names do not match any of the loader classes' DataSheetName class
        attributes.

        Args:
            None
        Exceptions:
            Raises:
                None
            Buffers:
                ValueError
        Returns:
            None
        """
        # TODO: Add support for custom sheet names (in addition to default).  This *could* use the overloaded
        # get_headers, but ATM, this only applies to those with a loader class and not the Defaults or Errors sheets,
        # which should also eventually be supported.
        if not hasattr(self, "exclude_sheets") or self.exclude_sheets is None:
            return
        ldr_cls_sheet_names = [
            ldrcls.DataSheetName for ldrcls in self.get_loader_classes()
        ]
        bad_sheets = [
            exclude_sheet
            for exclude_sheet in self.exclude_sheets
            if exclude_sheet not in ldr_cls_sheet_names
        ]
        if len(bad_sheets) > 0:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"Excluded sheet names [{', '.join(bad_sheets)}] must match one of "
                    f"[{', '.join(ldr_cls_sheet_names)}].  (Note: sheets with custom names can be excluded, but only "
                    "by supplying their default sheet name.)"
                )
            )

    @MaintainedModel.defer_autoupdates(
        disable_opt_names=["validate", "dry_run"],
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def load_data(self):
        """Loads the study file and.

        Args:
            None
        Exceptions:
            Raises:
                AggregatedErrorsSet
                ConditionallyRequiredArgs
                MultiLoadStatus
            Buffers:
                ProgrammingError
                ValueError
        Returns:
            None
        """
        # TODO: Remove the requirement for the file path/name.  The file is used for the load_keys in the
        # MultiLoadStatus object.  Other files' paths and names come from inside the study doc.  So I think we can just
        # use something like "study doc" in place of the actual name.  This facilitates testing where the name doesn't
        # matter.
        if self.file is None:
            raise self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"The [file] argument to {type(self).__name__}() is required to load data."
                )
            )
        elif not is_excel(self.file):
            raise self.aggregated_errors_object.buffer_error(
                ValueError(
                    f"'{self.file}' is not an excel file.  {type(self).__name__}'s file argument requires excel."
                )
            )

        if self.df_dict is not None and not isinstance(self.df_dict, dict):
            raise self.aggregated_errors_object.buffer_error(
                ProgrammingError(
                    f"The [df] argument to {type(self).__name__}() must be a dict of DataFrames, not "
                    f"'{type(self.df_dict).__name__}'.  (This is necessary in order to convert to the latest study doc "
                    "version via convert_df called from the constructor.)  Hint: if you are using read_from_file() to "
                    "create the value for the df argument, you must set sheet=None."
                ),
            )

        if self.file is not None and self.df is None:
            raise ConditionallyRequiredArgs(
                "'df' is required to have been supplied to the constructor if 'file' was supplied/defined."
            )

        if self.df_dict is None:
            raise self.aggregated_errors_object.buffer_error(
                ProgrammingError(
                    "The supplied file has to have been converted to the latest version (which should be a dict of "
                    "pandas dataframes) via convert_df called from the constructor.  However, self.df is of type "
                    f"'{type(self.df_dict).__name__}'."
                )
            )

        # This class is a ConvertedTableLoader class.  As such, the sheets in the file will be the pre-conversion
        # sheets.  The conversion (e.g. v2 to v3) will add sheets, so we must get the sheets from the df_dict object.

        file_sheets = list(self.df_dict.keys())
        sheets_to_make = []
        for file_sheet in file_sheets:
            # Skip loads that the user has excluded, based on a match of the class attribute DataSheetName
            if file_sheet not in self.exclude_sheets:
                sheets_to_make.append(file_sheet)
        loaders = self.get_loader_instances(sheets_to_make=sheets_to_make)

        if (
            len(self.annot_files_dict.keys()) > 0
            and PeakAnnotationFilesLoader.DataSheetName not in file_sheets
        ):
            self.buffer_infile_exception(
                (
                    f"Peak annotation files [{list(self.annot_files_dict.keys())}] were provided without a "
                    f"{PeakAnnotationFilesLoader.DataSheetName} sheet in {self.get_friendly_filename()}.  A "
                    f"{PeakAnnotationFilesLoader.DataSheetName} sheet is required to load peak annotation files."
                )
            )
            raise self.aggregated_errors_object

        # TODO: Add support for custom args for every loader for each of these (given multiple inputs, e.g. a file for
        # each input: animals.csv, samples.tsv, etc)
        # file, user_headers, headers, extra_headers, defaults

        # TODO: Add support for custom study name delimiter to the loader
        # TODO: Add support for custom tracer name delimiter to the loader
        # TODO: Add support for custom isotope positions delimiter to the loader
        # TODO: Add support for custom synonyms delimiter to the loader

        disable_caching_updates()

        # This cycles through the loaders in the order in which they were defined in the namedtuple
        all_aggregated_errors = []
        for loader_key in self.Loaders._fields:
            if loader_key not in loaders.keys():
                continue

            loader: TableLoader = loaders[loader_key]

            try:
                loader.load_data()
            except Exception as e:
                all_aggregated_errors.append(e)

        # Perform cross-loader checks
        self.perform_checks(loaders)

        print(f"ALL EXC BEFORE PACKAGING: QQQ {all_aggregated_errors} PPP")

        # Package up all of the exceptions.  This changes the error states of the various loaders, to emphasis the
        # summaries and deemphasize (and/or remove) potentially repeated errors.
        for aes in all_aggregated_errors:
            self.package_group_exceptions(aes)

        # Now update the load statuses
        for loader_key in self.Loaders._fields:
            if loader_key not in loaders.keys():
                continue
            loader: TableLoader = loaders[loader_key]
            self.update_load_stats(loader.get_load_stats())

        enable_caching_updates()

        print(f"ALL EXC BEFORE GROUPING: QQQ {all_aggregated_errors} PPP")

        self.create_grouped_exceptions()

        # If we're in validate mode, raise the MultiLoadStatus Exception whether there were errors or not, so
        # that we can roll back all changes and pass all the status data to the validation interface via this
        # exception.
        if self.validate:
            # If we are in validate mode, we raise the entire load_statuses object whether the load failed or
            # not, so that we can report the load status of all load files, including successful loads.  It's
            # like Dry Run mode, but exclusively for the validation interface.
            raise self.load_statuses

        print(f"ALL EXC BEFORE RAISING: QQQ {all_aggregated_errors} PPP")

        # If there were actual errors, raise an AggregatedErrorsSet exception inside the atomic block to cause
        # a rollback of everything
        if not self.load_statuses.is_valid:
            raise self.load_statuses.get_final_exception()

        if not self.dry_run:
            delete_all_caches()

        # dry_run and defer_rollback are handled by the load_data wrapper

    @classmethod
    def get_loader_classes(cls):
        """Return a list of loader classes."""
        return [
            ldrcls
            for ldrcls in list(cls.Loaders)
            if ldrcls is not None and issubclass(ldrcls, TableLoader)
        ]

    def get_loader_instances(self, sheets_to_make=None):
        """Instantiates the loaders and returns a dict of loader instances keyed on loader keys.

        Args:
            sheets_to_make (Optional[List[str]]): List of sheet keys, e.g. "ANIMALS"
        Exceptions:
            None
        Returns:
            loaders (Dict[str, TableLoader]): Dict of loader objects keyed on sheet key
        """

        loaders = {}
        loader_classes = self.Loaders
        sheet_names = self.get_sheet_names_tuple()
        sheets_present = [] if self.file is None else list(self.df_dict.keys())

        common_args = {
            # Do not pass dry-run.  That raises FOR rollback.  It should only be raised from THIS loader, not the
            # loaders it calls.  If you want to run in dry-run mode, that's fine.  Just don't pass that argument to the
            # child loaders.
            # "dry_run": self.dry_run,
            "debug": self.debug,
            "defer_rollback": True,
            "defaults_sheet": self.defaults_sheet,
            "file": self.file,
            "filename": self.get_friendly_filename(),
            "defaults_file": self.defaults_file,
            "_validate": self.validate,
        }

        # This cycles through the loaders in the order in which they were defined in the namedtuple
        loader_class: Type[TableLoader]
        for loader_key, loader_class in loader_classes._asdict().items():
            if getattr(loader_classes, loader_key) is None:
                continue
            sheet = getattr(sheet_names, loader_key)
            custom_args = getattr(self.CustomLoaderKwargs, loader_key)

            if sheets_to_make is None or sheet in sheets_to_make:

                # Build the keyword arguments to read_from_file
                rffkwargs = {"sheet": sheet}
                dtypes = self.get_loader_class_dtypes(loader_class)
                if dtypes is not None and len(dtypes.keys()) > 0:
                    rffkwargs["dtype"] = dtypes

                # Create a loader instance (e.g. CompoundsLoader())
                loaders[loader_key] = loader_class(
                    df=(
                        None
                        if self.file is None or sheet not in sheets_present
                        else self.df_dict[sheet]
                    ),
                    data_sheet=sheet,
                    **common_args,
                    **custom_args,
                )

        return loaders

    @classmethod
    def _get_loader_instances(cls, sheets_to_make=None):
        """A class version of get_loader_instances.

        Args:
            sheets_to_make (Optional[List[str]]): List of sheet keys, e.g. "ANIMALS"
        Exceptions:
            None
        Returns:
            loaders (Dict[str, TableLoader]): Dict of loader objects keyed on sheet key
        """

        loaders = {}
        loader_classes = cls.Loaders
        sheet_names = cls.DataHeaders

        # This cycles through the loaders in the order in which they were defined in the namedtuple
        loader_class: Type[TableLoader]
        for loader_key, loader_class in loader_classes._asdict().items():
            if getattr(loader_classes, loader_key) is None:
                continue
            sheet = getattr(sheet_names, loader_key)
            custom_args = getattr(cls.CustomLoaderKwargs, loader_key)

            if sheets_to_make is None or sheet in sheets_to_make:
                # Create a loader instance (e.g. CompoundsLoader())
                loaders[loader_key] = loader_class(**custom_args)

        return loaders

    def get_loader_class_dtypes(self, loader_class: Type[TableLoader], headers=None):
        """Retrieve a dtypes dict from the loader_class.

        Args:
            loader_class (Type[TableLoader]): Any class that inherits from TableLoader.
            headers (namedtuple): Custom headers, e.g. from a yaml file.
        Exceptions:
            None:
        Returns:
            dtypes (dict): Types keyed on header names (not keys)
        """
        if headers is not None:
            dtypes, aes = loader_class._get_column_types(headers, optional_mode=True)
            self.aggregated_errors_object.merge_aggregated_errors_object(aes)
            return dtypes

        # TODO Get rid of (/refactor) the ProtocolsLoader to not use this "DataHeadersExcel" class attribute
        if hasattr(loader_class, "DataHeadersExcel"):
            dtypes, aes = loader_class._get_column_types(
                loader_class.DataHeadersExcel, optional_mode=True
            )
            self.aggregated_errors_object.merge_aggregated_errors_object(aes)
            return dtypes

        dtypes, aes = loader_class._get_column_types(optional_mode=True)
        self.aggregated_errors_object.merge_aggregated_errors_object(aes)
        return dtypes

    def get_sheet_names_tuple(self):
        """Retrieve a tuple containing all of the loaders' sheet names.

        This will use any user-supplied sheet names.

        Args:
            None
        Exceptions:
            None:
        Returns:
            dtypes (dict): Types keyed on header names (not keys)
        """
        # Sheet names are stored as TableLoader "headers".  This is an overload of the TableLoader header functionality.
        # This class does not take or use a dataframe with headers.  It only takes a file and calls other loaders.
        return self.get_headers()

    @classmethod
    def get_study_sheet_column_display_order(cls, required=False):
        """Returns a list of lists to specify the sheet and column order of a created study excel file.

        The structure of the returned list is:

            [
                [sheet_name, [column_names]],
                ...
            ]

        Args:
            required (bool): Only return the required sheets
        Exceptions:
            None
        Returns:
            list of lists of a string (sheet name) and list of strings (column names)
        """
        ordered_sheet_keys = cls.DataSheetDisplayOrder
        display_order_spec = []
        loader: Type[TableLoader]
        for sheet_key in ordered_sheet_keys:
            if required and sheet_key not in cls.DataRequiredHeaders:
                continue

            sheet_name = getattr(cls.DataHeaders, sheet_key)

            if sheet_key == cls.DEFAULTS_SHEET:
                # Special case: The default sheet - We can use THIS class's superclass to get the defaults details, as
                # they are defined in the superclass and all the loaders derive from that
                display_order_spec.append(
                    [
                        sheet_name,
                        [
                            getattr(cls.DefaultsHeaders, dhk)
                            for dhk in cls.DefaultsTableHeaders._fields
                        ],
                    ]
                )
            elif sheet_key == cls.ERRORS_SHEET:
                # This is the only place currently where the headers in the errors sheet is defined.  This will likely
                # change
                display_order_spec.append([sheet_name, ["Errors"]])
            else:
                loader_cls = getattr(cls.Loaders, sheet_key)
                kwargs = getattr(cls.CustomLoaderKwargs, sheet_key)
                loader = loader_cls(**kwargs)

                display_order_spec.append(
                    [sheet_name, loader.get_ordered_display_headers()]
                )

        return display_order_spec

    def perform_checks(self, loaders: dict):
        self.check_animals(
            loaders.get(self.ANIMALS_SHEET), loaders.get(self.SAMPLES_SHEET)
        )

    def check_animals(
        self,
        animals_loader: Optional[AnimalsLoader],
        samples_loader: Optional[SamplesLoader],
    ):
        # Grab all the animals from the animals sheet (dataframe), if the animals loader is not None
        animals_from_animalssheet = (
            []
            if animals_loader is None
            else animals_loader.df[animals_loader.headers.NAME].to_list()
        )

        # If there were animals in the animals sheet
        if (
            isinstance(animals_loader, AnimalsLoader)
            and len(animals_from_animalssheet) > 0
        ):
            # Grab all the animals from the *samples* sheet (dataframe), if the samples loader is not None
            animals_from_samplessheet = (
                []
                if samples_loader is None
                else samples_loader.df[samples_loader.headers.ANIMAL].to_list()
            )

            # Now get the difference (animals in the animals sheet that were not in the samples sheet)
            # (We don't need to check animals that have samples in the samples sheet.  Their load may have failed, and
            #  we don't need to report them being missing because the reason they are missing will be in an error from
            #  the samples loader.)
            animals_to_check = list(
                set(animals_from_animalssheet) - set(animals_from_samplessheet)
            )

            # First, see if there are any animals without samples (accounting for previously loaded animals with no
            # samples).
            animals_without_samples = Animal.get_animals_without_samples(
                animals_to_check
            )
            # Only report animals without samples if the samples sheet has been loaded.  If the user is only loading
            # animals, we don't expect them to have any samples.
            if (
                len(animals_without_samples) > 0
                and isinstance(animals_loader, AnimalsLoader)
                and isinstance(samples_loader, SamplesLoader)
            ):
                # We are not going to buffer these individually.  Can't think of a reason to do so.
                animals_without_samples_exceptions: List[AnimalWithoutSamples] = []
                for animal in animals_without_samples:
                    rownum = animals_from_animalssheet.index(animal) + 2
                    aws = AnimalWithoutSamples(
                        animal,
                        file=animals_loader.friendly_file,
                        sheet=animals_loader.sheet,
                        rownum=rownum,
                        column=animals_loader.DataHeaders.NAME,
                    )

                    # TODO: Figure out a more uniform and consistent way to handle the summarization exceptions that
                    # takes advantage of self._loader instead of using setattr to set the error type and manually
                    # creating them calling set_load_exception.  I had played around with buffering the exception in
                    # StudyLoader, with the thinking that SummarizableErrors would be automatically taken care of, but
                    # that happens after load_data ends and we will be raising a MultiLoadStatus exception from here
                    # that has to already have had the summarizations done.

                    # Buffer the warning in the StudyLoader object so that the individual exceptions can be seen when we
                    # are in debug mode.  NOTE: We do not want to buffer it in the AnimalsLoader object because that
                    # load has completed and its exceptions post-processed.
                    if self.debug:
                        self.aggregated_errors_object.buffer_warning(
                            aws, is_fatal=self.validate
                        )

                    # Superficially set it as a warning, as if it came from an AggregatedErrors object buffer_warning
                    # method
                    setattr(aws, "is_error", False)
                    setattr(aws, "is_fatal", self.validate)
                    animals_without_samples_exceptions.append(aws)

                self.load_statuses.set_load_exception(
                    AnimalsWithoutSamples(animals_without_samples_exceptions),
                    "Animals Check",
                    default_is_error=False,
                    default_is_fatal=self.validate,
                )

            # Only check for additional missing serum samples if samples are being loaded or if samples already have
            # been loaded
            if (
                samples_loader is not None
                or Animal.objects.filter(
                    name__in=animals_to_check, samples__isnull=False
                ).exists()
            ):
                # It is possible that a previously loaded animal in the current animals sheet, with no samples in the
                # current samples sheet, had previously loaded without serum samples.  We take this opportunity to
                # double-check that the user has recitfied this situation.  If they have not, we remind them that an
                # animal in the animals sheet still has no serum samples.  But we filter out any that were already
                # reported by the sample loader.

                animals_without_serum_samples = (
                    Animal.get_animals_without_serum_samples(animals_from_animalssheet)
                )

                # We are not going to buffer these individually.  Can't think of a reason to do so.
                more_animals_without_serum_exceptions: List[
                    AnimalWithoutSerumSamples
                ] = []
                for animal in animals_without_serum_samples:
                    # Create an individual exception, so we can add it to the summary exception
                    rownum = animals_from_animalssheet.index(animal) + 2
                    awss = AnimalWithoutSerumSamples(
                        animal,
                        file=animals_loader.friendly_file,
                        sheet=animals_loader.sheet,
                        rownum=rownum,
                        column=animals_loader.DataHeaders.NAME,
                    )
                    # Superficially set it as a warning, as if it came from an AggregatedErrors object buffer_warning
                    # method
                    setattr(awss, "is_error", False)
                    setattr(awss, "is_fatal", self.validate)

                    # If there was a samples sheet (inferred by the samples loader being in the loaders dict), this
                    # animal wasn't already reported as not having serum samples, and there are no missing sample errors
                    if isinstance(samples_loader, SamplesLoader):
                        # Get the animals without serum samples exception(s) from the samples loader
                        sl_no_serum_exceptions: List[AnimalsWithoutSerumSamples] = (
                            samples_loader.aggregated_errors_object.get_exception_type(
                                AnimalsWithoutSerumSamples,
                                remove=False,
                                modify=False,
                                is_error=False,
                            )
                        )

                        if (
                            # This isn't an animal that has NO samples
                            animal not in animals_without_samples
                            and (
                                # Either the samples sheet had no missing serum sample exceptions
                                len(sl_no_serum_exceptions) == 0
                                # Or it did and this animal was not one of them
                                or (
                                    len(sl_no_serum_exceptions) > 0
                                    and animal not in sl_no_serum_exceptions[0].animals
                                )
                            )
                            # Only add the animal if the sample doesn't exist simply potentially because the samples
                            # load encountered an error.
                            and not samples_loader.aggregated_errors_object.is_error
                        ):
                            # Filter out animals that were already warned about by the sample loader
                            more_animals_without_serum_exceptions.append(awss)
                    else:
                        more_animals_without_serum_exceptions.append(awss)

                for awss in more_animals_without_serum_exceptions:
                    # Buffer the warning in the StudyLoader object.  We do not want to buffer it in the SamplesLoader
                    # object because that load has completed and its exceptions post-processed.  If we were to buffer
                    # exceptions in it after-the-fact, summary exceptions would not be taken care of.  Buffering it in
                    # this class means that it will get post-processed by this method's _loader wrapper.  The mean
                    # reason for this is to summarize SummarizableError exceptions.
                    if self.debug:
                        self.aggregated_errors_object.buffer_warning(
                            awss, is_fatal=self.validate
                        )

                # If there are any previously loaded animals, that are still in the animals sheet that still have no
                # serum samples, buffer a warning that they still have no serum samples.
                if len(more_animals_without_serum_exceptions) > 0:
                    self.load_statuses.set_load_exception(
                        AnimalsWithoutSerumSamples(
                            more_animals_without_serum_exceptions
                        ),
                        "Animals Check",
                        default_is_error=False,
                        default_is_fatal=self.validate,
                    )

    def package_group_exceptions(self, exception):
        """Repackages an exception for consolidated reporting.

        Compile group-level errors/warnings relevant specifically to a study load that should be reported in one
        consolidated error based on exceptions contained in AggregatedErrors.  Note, this could potentially change
        individual load file statuses from fatal errors to non-fatal warnings.  This is because the consolidated
        representation will be the "fatal error" and the errors in the files will be kept to cross-reference with the
        group level error.  See handle_exceptions.

        Args:
            exception (Exception)
        Exceptions:
            None
        Returns:
            None
        """
        # load_data (from which this is called) requires that self.file is not None, so we're assuming that here.  This
        # will be used as the load_key in the MultiLoadStatus object - for errors that arise from the file being
        # directly processed by this loader.  The PeakAnnotationsLoader calls will get their own file load keys.
        parent_file = self.get_friendly_filename()

        # Compile group-level errors/warnings relevant specifically to a study load that should be reported in one
        # consolidated error based on exceptions contained in AggregatedErrors.  Note, this could potentially change
        # individual load file statuses from fatal errors to non-fatal warnings.  This is because the consolidated
        # representation will be the "fatal error" and the errors in the files will be kept to cross-reference with the
        # group level error.  See handle_exceptions.
        if isinstance(exception, AggregatedErrors):

            self.extract_repeated_exceptions(exception)
            self.load_statuses.set_load_exception(exception, parent_file)

        elif isinstance(exception, AggregatedErrorsSet):

            for load_key, aes in exception.aggregated_errors_dict.items():
                self.extract_repeated_exceptions(aes)
                self.load_statuses.set_load_exception(aes, load_key)

        else:
            self.load_statuses.set_load_exception(exception, parent_file)

    def extract_repeated_exceptions(self, aes: AggregatedErrors):
        """This method takes selected exceptions (currently just missing records of Samples, Tissues,
        Protocols(/Treatments), and Compounds) and extracts their contained RecordDoesNotExist exceptions and puts them
        in lists of exceptions to be repackaged later.  The purpose is to consolidate errors about the same missing
        records that come from multiple different files in order to later collate them.

        Args:
            aes (AggregatedErrors)
        Exceptions:
            None
        Returns:
            None
        """
        # Missing record exceptions
        for exc_cls, buffer in [
            (MissingStudies, self.missing_study_record_exceptions),
            (MissingSamples, self.missing_sample_record_exceptions),
            (NoSamples, self.no_sample_record_exceptions),
            (UnskippedBlanks, self.unskipped_blank_record_exceptions),
            (MissingTissues, self.missing_tissue_record_exceptions),
            (MissingTreatments, self.missing_treatment_record_exceptions),
            (MissingCompounds, self.missing_compound_record_exceptions),
        ]:
            self.extract_missing_records_exception(aes, exc_cls, buffer)

        # Multiple Peak Group Representation exceptions
        mpgr_excs = aes.modify_exception_type(
            MultiplePeakGroupRepresentations, is_fatal=False, is_error=False
        )
        mpgr_exc: MultiplePeakGroupRepresentations
        for mpgr_exc in mpgr_excs:
            mpgr_exc.set_formatted_message(suggestion=self.representations_suggestion)
            self.multiple_pg_reps_exceptions.extend(mpgr_exc.exceptions)

        # Unexpected labels exceptions
        uel_excs = aes.get_exception_type(
            UnexpectedLabels, attr_name="is_error", attr_val=False
        )
        uel_exc: UnexpectedLabels
        for uel_exc in uel_excs:
            self.unexpected_labels_exceptions.extend(uel_exc.exceptions)
        print(
            f"NNN Num UnexpectedLabels exceptions found: {len(self.unexpected_labels_exceptions)} EXISTS IN AES: {aes.exception_type_exists(UnexpectedLabels)}"
        )

    def extract_missing_records_exception(
        self,
        aes: AggregatedErrors,
        missing_class: Type[MissingRecords],
        buffer: List[RecordDoesNotExist],
    ):
        """This extracts RecordDoesNotExist exceptions from exceptions derived from MissingRecords exceptions.

        Args:
            aes (AggregatedErrors)
            missing_class (MissingRecords)
            buffer (List[RecordDoesNotExist])
        Exceptions:
            None
        Returns:
            None
        """
        aes.modify_exception_type(missing_class, is_fatal=False, is_error=False)
        mrecs_excs = aes.get_exception_type(missing_class)
        for mrecs_exc in mrecs_excs:
            buffer.extend(mrecs_exc.exceptions)

    def create_grouped_exceptions(self):
        """
        This method compiles group-level exceptions, raises an AggregatedErrorsSet exception if fatal errors have been
        aggregated for any load file.
        """

        exc_cls: MissingModelRecordsByFile
        exc_lst: List[RecordDoesNotExist]
        for exc_cls, exc_lst, load_key, succinct, suggestion in [
            (
                AllMissingStudies,
                self.missing_study_record_exceptions,
                "Studies Check",
                False,
                None,
            ),
            (
                AllMissingSamples,
                self.missing_sample_record_exceptions,
                "Samples Check",
                False,
                None,
            ),
            (
                AllMissingSamples,
                self.no_sample_record_exceptions,
                "Peak Annotation Samples Check",
                True,
                None,
            ),
            (
                AllUnskippedBlanks,
                self.unskipped_blank_record_exceptions,
                "Peak Annotation Blanks Check",
                True,
                None,
            ),
            (
                AllMissingTissues,
                self.missing_tissue_record_exceptions,
                "Tissues Check",
                False,
                None,
            ),
            (
                AllMissingTreatments,
                self.missing_treatment_record_exceptions,
                "Treatments Check",
                False,
                None,
            ),
            (
                AllMissingCompounds,
                self.missing_compound_record_exceptions,
                "Compounds Check",
                True,
                None,
            ),
            (
                AllMultiplePeakGroupRepresentations,
                self.multiple_pg_reps_exceptions,
                "Peak Groups Check",
                True,
                self.representations_suggestion,
            ),
            (
                AllUnexpectedLabels,
                self.unexpected_labels_exceptions,
                "Contamination Check",
                True,
                None,
            ),
        ]:
            # Add this load key (if not already present anong the load keys).
            # This is what causes passes green status checks on the validation status report.
            self.load_statuses.update_load(load_key)

            # Collect all the missing samples in 1 error to add to the animal sample table file
            if len(exc_lst) > 0:
                print(
                    f"MMM CALLING {exc_cls}({exc_lst}, succinct={succinct}, suggestion={suggestion})"
                )
                self.load_statuses.set_load_exception(
                    exc_cls(exc_lst, succinct=succinct, suggestion=suggestion),
                    load_key,
                    top=True,
                    default_is_error=any(
                        e.is_error for e in exc_lst if hasattr(e, "is_error")
                    ),
                    default_is_fatal=any(
                        e.is_fatal for e in exc_lst if hasattr(e, "is_fatal")
                    ),
                )

    @classmethod
    def determine_matching_versions(cls, df_dict):
        """Given a dict of dataframes, return a list of the version numbers of the matching versions.

        Args:
            df_dict (Dict[str,pd.DataFrame]|pd.DataFrame)
        Exceptions:
            Raises:
                ProgrammingError
            Buffers:
                None
        Returns:
            matching_version_numbers (List[str]): Version numbers of the matching study doc versions.
            version_match_data (dict): Details of the sheets and headers that do and don't match every study doc version
                Example: {
                    "supplied": {sheet name: [supplied column names]},
                    "versions": {
                        version number: {
                            "match": bool,
                            "expected": {sheet: [required headers]},
                            "missing_sheets": []
                            "unknown_sheets": []
                            "matching": {sheet: {"matching": [headers], "missing": [headers], "unknown": [headers]}},
                        },
                    },
                }
        """
        matching_version_numbers: List[str] = []
        CurrentStudyLoader: Optional[Type[StudyLoader]] = None

        # In order to provide useful feeback on why there was no (or multiple) matches...
        # version_match_data Example: {
        #     "supplied": {sheet name: [supplied column names]},
        #     "versions": {
        #         version number: {
        #             "match": bool,
        #             "expected": {sheet: [required headers]},
        #             "missing_sheets": []
        #             "unknown_sheets": []
        #             "matching": {sheet: {"matching": [headers], "missing": [headers], "unknown": [headers]}},
        #         },
        #     },
        # }
        version_match_data = {
            "supplied": defaultdict(list),
            "versions": defaultdict(  # {version number: ...}
                lambda: {
                    "match": False,
                    "expected": defaultdict(list),  # {sheet: [required headers]}
                    "missing_sheets": [],
                    "unknown_sheets": [],
                    "matching": defaultdict(  # {sheet: {"matching": [], "missing": [headers], "unknown": [headers]}}
                        lambda: {
                            "matching": [],
                            "missing": [],
                            "unknown": [],
                        }
                    ),
                }
            ),
        }

        version_match_data["supplied"] = dict(
            (sheet, list(df.columns)) for sheet, df in df_dict.items()
        )
        supplied_sheets = set(list(df_dict.keys()))

        for study_loader_subcls in cls.__subclasses__():
            subclass_name = study_loader_subcls.__name__
            version_number = study_loader_subcls.version_number
            version_match_data["versions"][version_number][
                "expected"
            ] = study_loader_subcls.OrigDataRequiredHeaders.copy()  # type: ignore [attr-defined]
            expected_sheets = set(
                version_match_data["versions"][version_number]["expected"].keys()
            )

            # Sanity check to ensure that the CurrentStudyLoader is valid
            if subclass_name == cls.LatestLoaderName:
                # This is the latest version, which is handled below
                CurrentStudyLoader = study_loader_subcls

            if isinstance(df_dict, dict):
                # "headers" (from TableLoader) are "sheets" in this loader, because it is overloaded.  This loader has
                # no column headers of its own.  It just defines all the sheets and their individual loaders.
                common_sheets = list(expected_sheets.intersection(supplied_sheets))
                version_match_data["versions"][version_number]["missing_sheets"] = list(
                    expected_sheets.difference(supplied_sheets)
                )
                version_match_data["versions"][version_number]["unknown_sheets"] = list(
                    supplied_sheets.difference(expected_sheets)
                )
                # TODO: This debug data should be tracked and be used/presented when there is no version match.  For
                # now, keep it as a debug print.
                print(
                    f"V{study_loader_subcls.version_number} EXPECTED SHEETS: {expected_sheets} SUPPLIED SHEETS: "
                    f"{supplied_sheets}"
                )
                match = False
                if len(common_sheets) > 0:
                    # So far so good.  Let's assume it's a match unless the headers in each sheet say otherwise...
                    match = True
                    for sheet in common_sheets:
                        required_headers = set(
                            study_loader_subcls.get_required_headers(sheet)
                        )
                        supplied_headers = set(list(df_dict[sheet].columns))
                        version_match_data["versions"][version_number]["matching"][
                            sheet
                        ]["matching"] = list(
                            required_headers.intersection(supplied_headers)
                        )
                        version_match_data["versions"][version_number]["matching"][
                            sheet
                        ]["missing"] = list(
                            required_headers.difference(supplied_headers)
                        )
                        version_match_data["versions"][version_number]["matching"][
                            sheet
                        ]["unknown"] = list(
                            supplied_headers.difference(required_headers)
                        )
                        # TODO: This debug data should be tracked and be used/presented when there is no version match.
                        # For now, keep it as a debug print.
                        print(
                            f"V{study_loader_subcls.version_number} SHEET {sheet} REQUIRED HEADERS: {required_headers} "
                            f"SUPPLIED HEADERS: {supplied_headers}"
                        )
                        if not required_headers <= supplied_headers:
                            # TODO: This debug data should be tracked and be used/presented when there is no version
                            # match. For now, keep it as a debug print.
                            print(f"NOT {study_loader_subcls.version_number}")
                            match = False

                if match:
                    matching_version_numbers.append(
                        str(study_loader_subcls.version_number)
                    )
                    version_match_data["versions"][version_number]["match"] = True

            else:  # pd.DataFrame
                # Check to see if the supplied sheet matches any of the sheets in this version.  If so, it's a match
                match = False
                supplied_headers = set(list(df_dict.columns))
                version_match_data["versions"][version_number]["missing_sheets"] = []
                for expected_sheet in expected_sheets:
                    required_headers = set(
                        study_loader_subcls.get_required_headers(sheet=expected_sheet)
                    )
                    version_match_data["versions"][version_number]["matching"][
                        expected_sheet
                    ]["matching"] = list(
                        required_headers.intersection(supplied_headers)
                    )

                    # If there is any overlap
                    if (
                        len(
                            version_match_data["versions"][version_number]["matching"][
                                expected_sheet
                            ]["matching"]
                        )
                        > 0
                    ):
                        version_match_data["versions"][version_number]["matching"][
                            expected_sheet
                        ]["matching"] = list(
                            required_headers.intersection(supplied_headers)
                        )
                        version_match_data["versions"][version_number]["matching"][
                            expected_sheet
                        ]["missing"] = list(
                            required_headers.difference(supplied_headers)
                        )
                        version_match_data["versions"][version_number]["matching"][
                            expected_sheet
                        ]["unknown"] = list(
                            supplied_headers.difference(required_headers)
                        )
                        # If the required headers are a subset of the supplied headers
                        if required_headers <= supplied_headers:
                            matching_version_numbers.append(
                                str(study_loader_subcls.version_number)
                            )
                            match = True
                            version_match_data["versions"][version_number][
                                "match"
                            ] = True
                    else:
                        version_match_data["versions"][version_number][
                            "missing_sheets"
                        ].append(expected_sheet)

        # This just checks to make sure that a valid derived class was set as the cls.LatestLoaderName
        if CurrentStudyLoader is None:
            dclss = [c.__name__ for c in cls.__subclasses__()]
            if cls.LatestLoaderName is None:
                raise ProgrammingError(
                    "The latest/current loader/file version is not set.  "
                    "See StudyLoader.LatestLoaderName."
                )
            raise ProgrammingError(
                "The latest/current loader/file version was not found among the derived classes: "
                f"{dclss}"
            )

        return matching_version_numbers, version_match_data

    @classmethod
    def get_supported_versions(cls) -> List[str]:
        """Get a list of all supported version numbers (of the infile [aka, the 'study doc'])."""
        return [str(subcls.version_number) for subcls in StudyLoader.__subclasses__()]

    @classmethod
    def get_derived_class(cls, df_dict, version=None):
        """Retrieves the derived class of StudyLoader representing the detected/supplied infile study doc version.

        Args:
            df_dict (Dict[str, pandas.DataFrame])
            version (Optional[str])
        Exceptions:
            Raises:
                InvalidStudyDocVersion
                UnknownStudyDocVersion
                MultipleStudyDocVersions
            Buffers:
                None
        Returns:
            loader_class (StudyLoader): A derived class of StudyLoader matching the study doc version
        """
        loader_class: TableLoader
        match_data = {}

        if version is not None:
            matching_version_numbers = [version]
        else:
            matching_version_numbers, match_data = (
                StudyLoader.determine_matching_versions(df_dict)
            )

        if len(matching_version_numbers) == 1:
            if matching_version_numbers[0] == StudyV2Loader.version_number:
                loader_class = StudyV2Loader
            elif matching_version_numbers[0] == StudyV3Loader.version_number:
                loader_class = StudyV3Loader
            else:
                raise InvalidStudyDocVersion(
                    f"Unrecognized version number: {matching_version_numbers}."
                )
        elif len(matching_version_numbers) == 0:
            raise UnknownStudyDocVersion(
                StudyLoader.get_supported_versions(), match_data
            )
        else:
            raise MultipleStudyDocVersions(matching_version_numbers, match_data)

        return loader_class

    # TODO: ConvertedTableLoader was originally written to convert a dict of pandas dataframes to 1 pandas dataframe,
    # however, this class does a conversion to a dict of pandas dataframes (i.e. it doesn't do a merge).  This
    # functionality was superimposed on top of ConvertedTableLoader by this override, an override of convert_df in the
    # derived classes, and some class attribute changes. This strategy should be consolidated and the merge_dict class
    # attribute should be made to be optional.
    @classmethod
    def get_required_headers(cls, sheet=None):
        """Returns a list of required original headers in the supplied required sheet.  Returns headers from all
        required sheets if the supplied sheet is None.

        NOTE: This is an override of ConvertedTableLoader's method, because the assortment of headers differs, so we
        can't use the same namedtuple for each version, like we did for the PeakAnnotationsLoader.

        Args:
            None
        Exceptions:
            None
        Returns:
            sheets (List[str])
        """
        if sheet is None:
            all_hdrs = []
            for lst in cls.OrigDataRequiredHeaders.values():
                for hdr in lst:
                    if hdr not in all_hdrs:
                        all_hdrs.append(hdr)
        else:
            all_hdrs = cls.OrigDataRequiredHeaders[sheet]

        return all_hdrs


class StudyV3Loader(StudyLoader):
    version_number = "3.0"

    # These are actually sheet names, not headers
    OrigDataTableHeaders = StudyLoader.DataTableHeaders
    OrigDataHeaders = StudyLoader.DataHeaders

    # TODO: ConvertedTableLoader was originally written to convert a dict of pandas dataframes to 1 pandas dataframe.
    # This datastructure was designed around that.  It should be changed to have 2 levels of keys: sheets and columns.
    # Since StudyLoader overloads the columns to use them as sheets, putting column types here will not work, so we are
    # leaving it empty.
    OrigDataColumnTypes = {}

    # TODO: ConvertedTableLoader was originally written to convert a dict of pandas dataframes to 1 pandas dataframe,
    # however, this class does a conversion to a dict of pandas dataframes (i.e. it doesn't do a merge).  This
    # functionality was superimposed on top of ConvertedTableLoader by an override of get_required_headers, convert_df,
    # and changes to the following class attribute. This strategy should be consolidated and the merge_dict class
    # attribute should be made to be optional.

    # Explanation: OrigDataRequiredHeaders is different here from how ConvertedTableLoader expects it, so classmethod
    # get_required_headers was overridden to accommodate it.  Since "OrigDataHeaders" defined the sheet names (not the
    # headers), we couldn't use the keys in there as the actual column headers in each individual sheet, so this just
    # puts those headers directly in this class attribute.  And we're not going to use it as "required" sheets, only
    # headers.
    OrigDataRequiredHeaders = {
        **dict(
            (
                loader.DataSheetName,
                [
                    (
                        getattr(loader.DataHeaders, hk)
                        # TODO: Figure out how to eliminate ProtocolsLoader.DataHeaderExcel
                        if not hasattr(loader, "DataHeadersExcel")
                        else getattr(loader.DataHeadersExcel, hk)
                    )
                    for hk in loader.flatten_ndim_strings(loader.DataRequiredHeaders)
                ],
            )
            for loader in StudyLoader.Loaders._asdict().values()
            if not isinstance(loader, list) and loader is not None
        ),
        TableLoader.DefaultsSheetName: list(
            TableLoader.DefaultsHeaders._asdict().values()
        ),
        "Errors": [],
    }

    # This is the only one we need to define, in case multiple sheets are provided.  E.g. if the user adds a custom
    # sheet.
    # TODO: Make this optional by allowing the end result of the conversion be a dict of dataframes
    # merge_dict is unused, because convert_df will be overridden.
    merge_dict = {
        "first_sheet": StudyLoader.DataSheetName,
        "next_merge_dict": None,
    }
    add_columns_dict = None
    condense_columns_dict = None
    nan_defaults_dict = None
    sort_columns = None
    nan_filldown_columns = None
    merged_column_rename_dict = None
    merged_drop_columns_list = None

    def convert_df(self):
        # Explicitly NOT adding the ConversionHeading as a load key.

        # self.orig_df is created by the ConvertedTableLoader constructor
        self.df_dict = self.orig_df
        # The ConvertedTableLoader constructor also creates an empty df for the sheets (to treat them as headers).  This
        # class will ignore that instance attribute because we're modifying the behavior to keep the dict of dataframes.
        return self.df


class StudyV2Loader(StudyLoader):
    version_number = "2.0"

    # These are actually for sheet names, not headers
    OrigDataTableHeaders = namedtuple(
        "OrigDataTableHeaders",
        [
            "ANIMALS",
            "SAMPLES",
            "TISSUES",
            "TREATMENTS",
        ],
    )
    OrigDataHeaders = OrigDataTableHeaders(
        ANIMALS="Animals",
        SAMPLES="Samples",
        TISSUES="Tissues",
        TREATMENTS="Treatments",
    )

    # TODO: ConvertedTableLoader was originally written to convert a dict of pandas dataframes to 1 pandas dataframe.
    # This datastructure was designed around that.  It should be changed to have 2 levels of keys: sheets and columns.
    # Since StudyLoader overloads the columns to use them as sheets, putting column types here will not work, so we are
    # leaving it empty.
    OrigDataColumnTypes = {}

    # TODO: ConvertedTableLoader was originally written to convert a dict of pandas dataframes to 1 pandas dataframe,
    # however, this class does a conversion to a dict of pandas dataframes (i.e. it doesn't do a merge).  This
    # functionality was superimposed on top of ConvertedTableLoader by an override of get_required_headers, convert_df,
    # and changes to the following class attribute. This strategy should be consolidated and the merge_dict class
    # attribute should be made to be optional.

    # Explanation: OrigDataRequiredHeaders is different here from how ConvertedTableLoader expects it, so classmethod
    # get_required_headers was overridden to accommodate it.  Since "OrigDataHeaders" defined the sheet names (not the
    # headers), we couldn't use the keys in there as the actual column headers in each individual sheet, so this just
    # puts those headers directly in this class attribute.  And we're not going to use it for "required" sheets.  We're
    # only going to use it as possible sheets for matching all headers (not just the required ones, since many of the
    # differentiating column names are optional headers, but since people used a template to create them, they should
    # pretty much always be present).
    OrigDataRequiredHeaders = {
        "Animals": [
            "Animal ID",
            "Infusate",
            "Infusion Rate",
            "Study Name",
            "Study Description",
            # "Animal Body Weight",  # Not in 122221_highglycine_13Cgly_sucrosewater_MM
            # "Age",  # Not in serine-glycine-free-glucose-infusion
            "Sex",
            "Animal Genotype",
            "Feeding Status",
            "Diet",
            "Animal Treatment",
            "Tracer Concentrations",
        ],
        "Samples": [
            "Animal ID",
            "Sample Name",
            "Researcher Name",
            "Tissue",
            "Collection Time",
            "Date Collected",
        ],
        "Tissues": [
            "TraceBase Tissue Name",
            "Description",
        ],
        "Treatments": [
            "Animal Treatment",
            "Treatment Description",
        ],
    }

    # This is the only one we need to define, in case multiple sheets are provided.  E.g. if the user adds a custom
    # sheet.
    # TODO: Make this optional by allowing the end result of the conversion be a dict of dataframes
    # merge_dict is unused, because convert_df will be overridden.
    merge_dict = {
        "first_sheet": StudyLoader.DataSheetName,
        "next_merge_dict": None,
    }
    add_columns_dict = None
    condense_columns_dict = None
    nan_defaults_dict = None
    sort_columns = None
    nan_filldown_columns = None
    merged_column_rename_dict = None
    merged_drop_columns_list = None

    def convert_df(self):
        if not isinstance(self.orig_df, dict):
            raise NotImplementedError(
                "A single pandas DataFrame (i.e. a merge of the Animals and Samples sheets) is not yet supported.  "
                "Must be a dict of pandas dataframes."
            )

        # Add the conversion check heading to the load statuses (even though version 2 will always be added as a
        # warning).  This defines where the placeholder added and blanks removed warnings will be presented.
        self.load_statuses.update_load(self.ConversionHeading)

        indf_dict = deepcopy(self.orig_df)

        # 1. Prepare all sheets

        # We're not ready yet for actual dataframes.  It will be easier to move forward with dicts to be able to add
        # data.
        dfs_dict = {}
        # Get a dict of all the loader instances (e.g. StudiesLoader, AnimalsLoader, etc)
        # Calling the class version to do it without the input dataframes.  We will overwrite any that were supplied.
        loaders: Dict[str, TableLoader] = self._get_loader_instances()
        populate_sheets = [
            ProtocolsLoader.DataSheetName,
            LCProtocolsLoader.DataSheetName,
            TissuesLoader.DataSheetName,
        ]
        filters = {
            ProtocolsLoader.DataSheetName: {"category": Protocol.ANIMAL_TREATMENT}
        }

        # Add missing sheets (Note, there are none that need renamed)
        sheets_supplied = []
        for hk in StudyLoader.DataHeaders._fields:
            if getattr(StudyLoader.Loaders, hk) is None:
                continue
            sheet = getattr(StudyLoader.DataHeaders, hk)
            if sheet in indf_dict.keys():
                dfs_dict[sheet] = indf_dict[sheet].to_dict()
                sheets_supplied.append(sheet)
            else:
                dfs_dict[sheet] = loaders[hk].get_dataframe_template(
                    populate=sheet in populate_sheets,
                    filter=None if sheet not in filters.keys() else filters[sheet],
                )

        # 2. Animals sheet

        # Animals sheet mods
        sheet = AnimalsLoader.DataSheetName

        if sheet in sheets_supplied:
            # Rename Animal Genotype -> Genotype
            old_header = "Animal Genotype"
            new_header = loaders["ANIMALS"].DataHeaders.GENOTYPE
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # Rename Animal Body Weight -> Weight
            old_header = "Animal Body Weight"
            new_header = loaders["ANIMALS"].DataHeaders.WEIGHT
            weight_col = dfs_dict[sheet].pop(old_header, None)
            if weight_col is not None:
                dfs_dict[sheet][new_header] = weight_col

            # Rename Animal Treatment -> Treatment
            old_header = "Animal Treatment"
            new_header = loaders["ANIMALS"].DataHeaders.TREATMENT
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # Rename Study Name -> Study
            old_header = "Study Name"
            new_header = loaders["ANIMALS"].DataHeaders.STUDY
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # Rename Animal ID -> Animal Name
            old_header = "Animal ID"
            new_header = loaders["ANIMALS"].DataHeaders.NAME
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # 3. Study sheet

            # Copy Study name and description to study sheet
            animals_study_name_header = loaders["ANIMALS"].DataHeaders.STUDY
            animals_study_desc_header = "Study Description"

            study_study_name_header = loaders["STUDY"].DataHeaders.NAME
            study_study_desc_header = loaders["STUDY"].DataHeaders.DESCRIPTION

            study_dict = {
                study_study_name_header: {},
                study_study_desc_header: {},
            }
            new_i = 0
            seen = {}

            for i in dfs_dict[sheet][animals_study_name_header].keys():
                name = dfs_dict[sheet][animals_study_name_header][i]
                desc = dfs_dict[sheet][animals_study_desc_header][i]
                key = f"{name},{str(desc)}"
                if key not in seen.keys():
                    study_dict[study_study_name_header][new_i] = name
                    study_dict[study_study_desc_header][new_i] = desc
                    seen[key] = 0
                    new_i += 1

            dfs_dict[StudiesLoader.DataSheetName] = study_dict

            # Delete Study description column from the Animals sheet
            dfs_dict[sheet].pop(animals_study_desc_header)

            # 4. Infusates and Tracers sheets

            # Modify the Infusate column to include the tracer concentrations in the name
            animals_infusate_header = loaders["ANIMALS"].DataHeaders.INFUSATE
            animals_concentrations_header = "Tracer Concentrations"

            infusates_groupnum_header = loaders["INFUSATES"].DataHeaders.ID
            infusates_groupname_header = loaders["INFUSATES"].DataHeaders.TRACERGROUP
            infusates_trcr_header = loaders["INFUSATES"].DataHeaders.TRACERNAME
            infusates_conc_header = loaders["INFUSATES"].DataHeaders.TRACERCONC
            infusates_name_header = loaders["INFUSATES"].DataHeaders.NAME

            tracers_groupnum_header = loaders["TRACERS"].DataHeaders.ID
            tracers_cmpd_header = loaders["TRACERS"].DataHeaders.COMPOUND
            tracers_mass_header = loaders["TRACERS"].DataHeaders.MASSNUMBER
            tracers_elem_header = loaders["TRACERS"].DataHeaders.ELEMENT
            tracers_count_header = loaders["TRACERS"].DataHeaders.LABELCOUNT
            tracers_poss_header = loaders["TRACERS"].DataHeaders.LABELPOSITIONS
            tracers_name_header = loaders["TRACERS"].DataHeaders.NAME

            infusates = {}
            tracers = {}

            # Build the infusates and tracers dicts
            for i, infname in dfs_dict[sheet][animals_infusate_header].items():
                concs_str = str(dfs_dict[sheet][animals_concentrations_header][i])

                # Skip empties
                if str(infname) in self.none_vals and str(concs_str) in self.none_vals:
                    continue

                try:
                    concentrations = parse_tracer_concentrations(concs_str.strip())
                    infusate_data = parse_infusate_name(infname.strip(), concentrations)
                    infusate_name_with_concs = Infusate.name_from_data(infusate_data)
                except Exception as e:
                    sug = (
                        f"Unable to parse infusate name '{infname}' and concentrations '{concs_str}' while building a "
                        "dict of infusates and tracers for populating the Infusates and Tracers sheets from version 2 "
                        "to version 3.  Ignoring."
                    )

                    ie = InfileError(
                        f"{type(e).__name__}: {str(e)}",
                        file=self.friendly_file,
                        sheet=sheet,
                        column=f"{animals_infusate_header} and {animals_concentrations_header}",
                        rownum=i + 2,
                        suggestion=sug,
                    )

                    # TODO: Right now, this error doesn't get in front of the user on the validation page.  I'm using
                    # load_statuses.set_load_exception below to do that and I'm buffering here to be able to see the
                    # trace in the console.  I should fix this so that buffered errors from the conversion get their
                    # own load_key and get in front of the user.
                    self.aggregated_errors_object.buffer_error(ie, orig_exception=e)

                    self.load_statuses.set_load_exception(
                        ie,
                        self.ConversionHeading,
                        top=True,
                    )
                    continue

                dfs_dict[sheet][animals_infusate_header][i] = infusate_name_with_concs

                if infusate_name_with_concs not in infusates.keys():
                    infusates[infusate_name_with_concs] = infusate_data

                    for tracer_link in infusate_data["tracers"]:
                        tracer_name = tracer_link["tracer"]["unparsed_string"]

                        if tracer_name not in tracers.keys():
                            tracers[tracer_name] = tracer_link["tracer"]

            # Add the Infusates and Tracers to their respective sheets (name only)
            new_i = 0
            inf_n = 1
            for infname, infdata in infusates.items():
                for tracer_link in infdata["tracers"]:
                    dfs_dict[InfusatesLoader.DataSheetName][infusates_groupnum_header][
                        new_i
                    ] = inf_n
                    dfs_dict[InfusatesLoader.DataSheetName][infusates_groupname_header][
                        new_i
                    ] = infdata["infusate_name"]
                    dfs_dict[InfusatesLoader.DataSheetName][infusates_name_header][
                        new_i
                    ] = infname

                    dfs_dict[InfusatesLoader.DataSheetName][infusates_trcr_header][
                        new_i
                    ] = tracer_link["tracer"]["unparsed_string"]
                    dfs_dict[InfusatesLoader.DataSheetName][infusates_conc_header][
                        new_i
                    ] = tracer_link["concentration"]
                    new_i += 1
                inf_n += 1

            new_i = 0
            trc_n = 1
            pdelim = TracersLoader.POSITIONS_DELIMITER
            for tracer_name, tracer_data in tracers.items():
                for isotope in tracer_data["isotopes"]:
                    dfs_dict[TracersLoader.DataSheetName][tracers_groupnum_header][
                        new_i
                    ] = trc_n
                    dfs_dict[TracersLoader.DataSheetName][tracers_cmpd_header][
                        new_i
                    ] = tracer_data["compound_name"]
                    dfs_dict[TracersLoader.DataSheetName][tracers_mass_header][
                        new_i
                    ] = isotope["mass_number"]
                    dfs_dict[TracersLoader.DataSheetName][tracers_elem_header][
                        new_i
                    ] = isotope["element"]
                    dfs_dict[TracersLoader.DataSheetName][tracers_count_header][
                        new_i
                    ] = isotope["count"]
                    if isotope["positions"] is None:
                        dfs_dict[TracersLoader.DataSheetName][tracers_poss_header][
                            new_i
                        ] = None
                    else:
                        dfs_dict[TracersLoader.DataSheetName][tracers_poss_header][
                            new_i
                        ] = pdelim.join([str(p) for p in sorted(isotope["positions"])])
                    dfs_dict[TracersLoader.DataSheetName][tracers_name_header][
                        new_i
                    ] = tracer_name
                    new_i += 1
                trc_n += 1

            # Delete tracer concentrations
            dfs_dict[sheet].pop(animals_concentrations_header)

        # 5. Samples sheet

        # Samples sheet mods
        sheet = SamplesLoader.DataSheetName

        if sheet in sheets_supplied:
            # Rename Sample Name -> Sample
            old_header = "Sample Name"
            new_header = loaders["SAMPLES"].DataHeaders.SAMPLE
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # V2 included blank samples in the Samples sheet.  V3 does not.  It puts them in the Peak Annotation Details
            # sheet (which is handled later and is based on the names of the sample headers in the peak annotation
            # files). We need to remove rows with blank samples.  V2 identified blanks to skip by excluding rows that do
            # not have a tissue, so...
            sample_header = loaders["SAMPLES"].DataHeaders.SAMPLE
            tissue_header = loaders["SAMPLES"].DataHeaders.TISSUE
            # Get rows that are missing tissue values (which is what determines "blanks", but also check the sample name
            blank_row_idxs = [
                idx
                for idx, tiss in dfs_dict[sheet][tissue_header].items()
                if (
                    # In v2, the tissue column being empty was to indicate a blank
                    str(tiss) in self.none_vals
                    # Sometimes the researcher literally puts "blank" in the tissue column
                    or Sample.is_a_blank(str(tiss))
                    # But let's also look for "blank" in the sample name too
                    or Sample.is_a_blank(dfs_dict[sheet][sample_header][idx])
                )
            ]
            samples_removed = []

            # Assume it's an empty row if the sample name is undefined
            blanks_exceptions = []
            for blank_row_idx in blank_row_idxs:
                if dfs_dict[sheet][sample_header][blank_row_idx] in self.none_vals:
                    continue
                samples_removed.append(dfs_dict[sheet][sample_header][blank_row_idx])
                blanks_exceptions.append(
                    BlankRemoved(
                        dfs_dict[sheet][sample_header][blank_row_idx],
                        MSRunsLoader.DataSheetName,
                        self.version_number,
                        self.derived_loaders[self.LatestLoaderName].version_number,
                        file=self.friendly_file,
                        sheet=sheet,
                        rownum=blank_row_idx + 2,
                    )
                )
                # Keep the row present, but blank it out.  We can later add comments to cells on this row explaining why
                # it's blank
                for header in dfs_dict[sheet].keys():
                    dfs_dict[sheet][header][blank_row_idx] = None

            if len(samples_removed) > 0:
                self.load_statuses.set_load_exception(
                    BlanksRemoved(
                        blanks_exceptions,
                        MSRunsLoader.DataSheetName,
                        self.version_number,
                        self.derived_loaders[self.LatestLoaderName].version_number,
                        file=self.friendly_file,
                        sheet=sheet,
                    ),
                    self.ConversionHeading,
                    top=True,
                    default_is_error=not self.validate,  # Warning for researchers, error for curators
                    default_is_fatal=self.validate,  # Fatal in validate mode in order to pass the exc to the template
                )

            # Date Collected (dates are detected by pandas, but written out in a long format, so this makes them nice)
            date_header = loaders["SAMPLES"].DataHeaders.DATE
            for idx, date_str in dfs_dict[sheet][date_header].items():
                if idx in blank_row_idxs or date_str in self.none_vals:
                    continue

                try:
                    # Convert the provided value (as a string) to a date (not a datetime)
                    dfs_dict[sheet][date_header][idx] = datetime_to_string(
                        string_to_date(str(date_str))
                    )
                except Exception as e:
                    self.buffer_infile_exception(e)

            # Rename Animal ID -> Animal
            old_header = "Animal ID"
            new_header = loaders["SAMPLES"].DataHeaders.ANIMAL
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

            # Rename Researcher Name -> Researcher
            old_header = "Researcher Name"
            new_header = loaders["SAMPLES"].DataHeaders.HANDLER
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

        # 6. Tissues sheet

        # Tissues sheet mods
        sheet = TissuesLoader.DataSheetName

        if sheet in sheets_supplied:
            # Rename TraceBase Tissue Name -> Tissue
            old_header = "TraceBase Tissue Name"
            new_header = loaders["TISSUES"].DataHeaders.NAME
            dfs_dict[sheet][new_header] = dfs_dict[sheet].pop(old_header)

        # 7. Treatments sheet (nothing to do)
        # 8. Compounds sheet (we do not have this data - it wasn't in the original v2 format)
        # 9. LC Protocols sheet (nothing to do)

        # 10. Sequences sheet (we do not have this data - it wasn't in the original v2 format).  Note, this generates a
        #     placeholder record that the user must edit.

        sheet = SequencesLoader.DataSheetName
        placeholder_seqname = "Anonymous, unknown, unknown, 1999-04-01"
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.SEQNAME][
            0
        ] = placeholder_seqname
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.OPERATOR][0] = "Anonymous"
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.LCNAME][0] = "unknown"
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.INSTRUMENT][0] = "unknown"
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.DATE][0] = "1999-04-01"
        dfs_dict[sheet][loaders["SEQUENCES"].DataHeaders.NOTES][0] = loaders[
            "SEQUENCES"
        ].V2_PLACEHOLDER_NOTE

        paes = []
        for col in [
            loaders["SEQUENCES"].DataHeaders.SEQNAME,
            loaders["SEQUENCES"].DataHeaders.OPERATOR,
            loaders["SEQUENCES"].DataHeaders.LCNAME,
            loaders["SEQUENCES"].DataHeaders.INSTRUMENT,
            loaders["SEQUENCES"].DataHeaders.DATE,
            loaders["SEQUENCES"].DataHeaders.NOTES,
        ]:
            paes.append(
                PlaceholderAdded(
                    self.version_number,
                    self.derived_loaders[self.LatestLoaderName].version_number,
                    file=self.friendly_file,
                    sheet=sheet,
                    column=col,
                    rownum=2,
                )
            )

        self.load_statuses.set_load_exception(
            PlaceholdersAdded(
                self.version_number,
                self.derived_loaders[self.LatestLoaderName].version_number,
                paes,
                file=self.friendly_file,
                sheet=sheet,
            ),
            self.ConversionHeading,
            top=True,
            default_is_error=not self.validate,  # Warning for researchers, error for curators
            default_is_fatal=self.validate,  # Fatal in validate mode in order to pass the exception to the template
        )

        # 11. Peak Annotation Files and Peak Annotations Details sheets
        # We can obtain the peak annotation files (and determine their formats) from the PeakAnnotationFilesLoader's
        # peak_annot_files instance attribute, if it was defined/supplied to the StudyLoader constructor's
        # annot_files_dict argument (which is propagated to the PeakAnnotationFilesLoader's constructor).

        files_sheet = PeakAnnotationFilesLoader.DataSheetName
        deets_sheet = MSRunsLoader.DataSheetName

        pafl: PeakAnnotationFilesLoader = loaders["FILES"]
        file_header = loaders["FILES"].DataHeaders.FILE
        format_header = loaders["FILES"].DataHeaders.FORMAT
        defseqname_header = loaders["FILES"].DataHeaders.SEQNAME

        padl: MSRunsLoader = loaders["HEADERS"]
        sample_header = padl.DataHeaders.SAMPLENAME
        header_header = padl.DataHeaders.SAMPLEHEADER
        mzxml_header = padl.DataHeaders.MZXMLNAME
        annot_header = padl.DataHeaders.ANNOTNAME
        seqname_header = padl.DataHeaders.SEQNAME
        skip_header = padl.DataHeaders.SKIP

        annotsample_header = PeakAnnotationsLoader.DataHeaders.SAMPLEHEADER

        # This assumes that neither of these sheets existed at the outset (because they weren't a part of v2)
        file_i = 0
        detail_i = 0
        if pafl.annot_files_dict is not None:
            for filename, filepath in sorted(pafl.annot_files_dict.items()):
                # Append to the file column
                dfs_dict[files_sheet][file_header][file_i] = filename

                # Determine the format for the format column
                df_dict = read_from_file(filepath, sheet=None)
                matching_formats = PeakAnnotationsLoader.determine_matching_formats(
                    df_dict
                )

                if len(matching_formats) == 1:
                    dfs_dict[files_sheet][format_header][file_i] = matching_formats[0]

                    # TODO: Encapsulate this code in a method like StudyLoader.get_loader_class
                    if matching_formats[0] == AccucorLoader.format_code:
                        loader_class = AccucorLoader
                    elif matching_formats[0] == IsocorrLoader.format_code:
                        loader_class = IsocorrLoader
                    elif matching_formats[0] == IsoautocorrLoader.format_code:
                        loader_class = IsoautocorrLoader
                    elif matching_formats[0] == UnicorrLoader.format_code:
                        loader_class = UnicorrLoader
                    else:
                        raise ValueError(
                            f"Unrecognized format code: {matching_formats}."
                        )

                    converted_df = loader_class(df=df_dict, file=filepath).df
                    sample_headers = sorted(converted_df[annotsample_header].unique())

                    for sh in sample_headers:
                        dfs_dict[deets_sheet][sample_header][detail_i] = (
                            padl.guess_sample_name(sh)
                        )
                        dfs_dict[deets_sheet][header_header][detail_i] = sh
                        dfs_dict[deets_sheet][mzxml_header][detail_i] = None
                        dfs_dict[deets_sheet][annot_header][detail_i] = filename
                        dfs_dict[deets_sheet][seqname_header][
                            detail_i
                        ] = placeholder_seqname
                        dfs_dict[deets_sheet][skip_header][detail_i] = (
                            padl.SKIP_STRINGS[0] if Sample.is_a_blank(sh) else None
                        )

                        detail_i += 1

                else:
                    dfs_dict[files_sheet][format_header][file_i] = None

                # Fill a None into the Default Sequence column
                dfs_dict[files_sheet][defseqname_header][file_i] = placeholder_seqname

                # Increment for the next row
                file_i += 1

        # self.orig_df is created by the ConvertedTableLoader constructor to save the original input dict of dataframes
        # (or dataframe).  Here though, we set df_dict to use internally as the converted data.  Neither TableLoader nor
        # ConvertedTableLoader support the converted data as being a dict of dataframes.  Both assume a single dataframe
        # only.
        # TODO: Add support for a dict of dataframes in both TableLoader and ConvertedTableLoader, using the
        # DataSheetName as the way to get the dataframe each class is meant to handle.
        self.df_dict = dict(
            (sheet, pd.DataFrame.from_dict(dfs_dict[sheet]))
            for sheet in dfs_dict.keys()
        )

        # TableLoader checks the dataframe (df) against the StudyLoader class attributes.  In the case of this class,
        # that means it's checking the sheets (as if they were headers), so we need to modify the simulated df that
        # ConvertedTableLoader created to represent the sheets as empty columns.  This should also be addressed in the
        # above TODO.
        tmp_df_dict = dict((sheet, {}) for sheet in dfs_dict.keys())
        self.df = pd.DataFrame.from_dict(tmp_df_dict)

        # The ConvertedTableLoader constructor also creates an empty df for the sheets (to treat them as headers).  This
        # class will ignore that instance attribute because we're modifying the behavior to keep the dict of dataframes.
        return self.df
