from __future__ import annotations

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
from DataRepo.loaders.protocols_loader import ProtocolsLoader
from DataRepo.loaders.samples_loader import SamplesLoader
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.loaders.studies_loader import StudiesLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.loaders.tracers_loader import TracersLoader
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
    MultipleStudyDocVersions,
    NoSamples,
    PlaceholderAdded,
    PlaceholdersAdded,
    RecordDoesNotExist,
    UnknownStudyDocVersion,
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


class StudyLoader(ConvertedTableLoader, ABC):
    """Loads an entire study doc (i.e. all of its sheets - not just the Study model)."""

    @property
    @abstractmethod
    def version_number(self) -> str:
        """The version of the study doc"""
        pass

    @property
    @abstractmethod
    def ConversionHeading(self) -> str:
        """Category name under which conversion errors and warnings will be found.  (str)"""
        pass

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
        DEFAULTS=None,
        ERRORS=None,
    )
    Models: List[Model] = []
    # No FieldToDataValueConverter needed

    # TODO: Support for a dict of dataframes should be introduced here by setting the DataSheetName to None
    # Unused (currently)
    DataSheetName = "Defaults"

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
        DEFAULTS=None,
        ERRORS=None,
    )

    # NOTE: The instance copies this and adds to the copy
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
        DEFAULTS=None,
        ERRORS=None,
    )

    DataSheetDisplayOrder = [
        STUDY_SHEET,
        ANIMALS_SHEET,
        SAMPLES_SHEET,
        FILES_SHEET,
        HEADERS_SHEET,
        SEQUENCES_SHEET,
        INFUSATES_SHEET,
        TRACERS_SHEET,
        TREATMENTS_SHEET,
        TISSUES_SHEET,
        COMPOUNDS_SHEET,
        LCPROTOCOLS_SHEET,
        DEFAULTS_SHEET,
        ERRORS_SHEET,
    ]

    def __init__(self, *args, **kwargs):
        """Constructor.

        Args:
            Superclass Args:
                df (Optional[Dict[str, pandas dataframe]]): Data, e.g. as parsed from an excel file.  *See
                    ConvertedTableLoader.*
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
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ValueError
        Returns:
            None
        """
        # NOTE: self.load_data() requires the file argument to have been provided to this constructor.
        if kwargs.get("defer_rollback") is False:
            raise ProgrammingError(
                "Modifying the following superclass constructor arguments is prohibited by StudyLoader: "
                "[defer_rollback]."
            )

        # Custom dataframe storage for this class, since we want to handle df as a dict of dataframes, but still take
        # advantage of the TableLoader checks on the sheets as if they were columns.
        self.df_dict = None

        self.annot_files_dict = kwargs.pop("annot_files_dict", {})

        clkwa = self.CustomLoaderKwargs._asdict()
        clkwa["FILES"]["annot_files_dict"] = self.annot_files_dict
        # This occludes the CustomLoaderKwargs class attribute (which we copied and are leaving unchanged)
        # Just note that only the instance has annot_files_dict
        self.CustomLoaderKwargs = self.DataTableHeaders(**clkwa)

        self.missing_study_record_exceptions = []
        self.missing_sample_record_exceptions = []
        self.no_sample_record_exceptions = []
        self.missing_tissue_record_exceptions = []
        self.missing_treatment_record_exceptions = []
        self.missing_compound_record_exceptions = []
        self.load_statuses = MultiLoadStatus()

        self.derived_loaders = {}
        for loader_class in StudyLoader.__subclasses__():
            self.derived_loaders[loader_class.__name__] = loader_class

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

        self.check_study_class_attributes()

    @classmethod
    def check_study_class_attributes(cls):
        """Basically just error-checks that the sheet display keys are equivalent to the load order keys."""
        if set(cls.DataTableHeaders._fields) != set(cls.DataSheetDisplayOrder):
            raise ProgrammingError(
                "DataTableHeaders and DataSheetDisplayOrder must have the same sheet keys"
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
                MultiLoadStatus
            Buffers:
                ValueError
        Returns:
            None
        """
        if self.file is None:
            raise AggregatedErrors().buffer_error(
                ValueError(
                    f"The [file] argument to {type(self).__name__}() is required."
                )
            )
        elif not is_excel(self.file):
            raise AggregatedErrors().buffer_error(
                ValueError(
                    f"'{self.file}' is not an excel file.  StudyLoader's file argument requires excel."
                )
            )

        if self.file is not None and self.df is None:
            raise ConditionallyRequiredArgs(
                "'df' is required to have been supplied to the constructor if 'file' was supplied/defined."
            )

        if self.df_dict is None or not isinstance(self.df_dict, dict):
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
        loaders = self.get_loader_instances(sheets_to_make=file_sheets)

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
        for loader_key in self.Loaders._fields:
            if loader_key not in loaders.keys():
                continue
            loader: TableLoader = loaders[loader_key]
            try:
                loader.load_data()
            except Exception as e:
                self.package_group_exceptions(e)

        enable_caching_updates()

        self.create_grouped_exceptions()

        # If we're in validate mode, raise the MultiLoadStatus Exception whether there were errors or not, so
        # that we can roll back all changes and pass all the status data to the validation interface via this
        # exception.
        if self.validate:
            # If we are in validate mode, we raise the entire load_statuses object whether the load failed or
            # not, so that we can report the load status of all load files, including successful loads.  It's
            # like Dry Run mode, but exclusively for the validation interface.
            raise self.load_statuses

        # If there were actual errors, raise an AggregatedErrorsSet exception inside the atomic block to cause
        # a rollback of everything
        if not self.load_statuses.is_valid:
            raise self.load_statuses.get_final_exception()

        if not self.dry_run:
            delete_all_caches()

        # dry_run and defer_rollback are handled by the load_data wrapper

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
            "dry_run": self.dry_run,
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
            return loader_class._get_column_types(headers, optional_mode=True)

        # TODO Get rid of (/refactor) the ProtocolsLoader to not use this "DataHeadersExcel" class attribute
        if hasattr(loader_class, "DataHeadersExcel"):
            return loader_class._get_column_types(
                loader_class.DataHeadersExcel, optional_mode=True
            )

        return loader_class._get_column_types(optional_mode=True)

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
        for exc_cls, buffer in [
            (MissingStudies, self.missing_study_record_exceptions),
            (MissingSamples, self.missing_sample_record_exceptions),
            (NoSamples, self.no_sample_record_exceptions),
            (MissingTissues, self.missing_tissue_record_exceptions),
            (MissingTreatments, self.missing_treatment_record_exceptions),
            (MissingCompounds, self.missing_compound_record_exceptions),
        ]:
            self.extract_missing_records_exception(aes, exc_cls, buffer)

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
        mrecs_excs = aes.modify_exception_type(
            missing_class, is_fatal=False, is_error=False
        )
        for mrecs_exc in mrecs_excs:
            buffer.extend(mrecs_exc.exceptions)

    def create_grouped_exceptions(self):
        """
        This method compiles group-level exceptions, raises an AggregatedErrorsSet exception if fatal errors have been
        aggregated for any load file.
        """

        exc_cls: MissingModelRecordsByFile
        exc_lst: List[RecordDoesNotExist]
        for exc_cls, exc_lst, load_key, succinct in [
            (
                AllMissingStudies,
                self.missing_study_record_exceptions,
                "All Studies Exist in the Database",
                False,
            ),
            (
                AllMissingSamples,
                self.missing_sample_record_exceptions,
                "All Samples Exist in the Database",
                False,
            ),
            (
                AllMissingSamples,
                self.no_sample_record_exceptions,
                "No Files are Missing All Samples",
                True,
            ),
            (
                AllMissingTissues,
                self.missing_tissue_record_exceptions,
                "All Tissues Exist in the Database",
                False,
            ),
            (
                AllMissingTreatments,
                self.missing_treatment_record_exceptions,
                "All Treatments Exist in the Database",
                False,
            ),
            (
                AllMissingCompounds,
                self.missing_compound_record_exceptions,
                "All Compounds Exist in the Database",
                False,
            ),
        ]:
            # Add this load key (if not already present anong the load keys).
            # This is what causes passes green status checks on the validation status report.
            self.load_statuses.update_load(load_key)

            # Collect all the missing samples in 1 error to add to the animal sample table file
            if len(exc_lst) > 0:
                self.load_statuses.set_load_exception(
                    exc_cls(exc_lst, succinct=succinct),
                    load_key,
                    top=True,
                )

    @classmethod
    def determine_matching_versions(cls, df_dict) -> List[str]:
        """Given a dict of dataframes, return a list of the version numbers of the matching versions.

        Args:
            df_dict (Dict[str,pd.DataFrame]|pd.DataFrame)
        Exceptions:
            None
        Returns:
            matching_version_numbers (List[str]): Version numbers of the matching study doc versions.
        """
        matching_version_numbers: List[str] = []
        CurrentStudyLoader: Optional[Type[StudyLoader]] = None
        supplied_sheets = set(list(df_dict.keys()))

        common_data: dict = defaultdict(lambda: defaultdict(list))

        for study_loader_subcls in cls.__subclasses__():
            common_data[study_loader_subcls.__name__]["loaders"].append(
                study_loader_subcls
            )

            # Sanity check to ensure that the CurrentStudyLoader is valid
            if study_loader_subcls.__name__ == cls.LatestLoaderName:
                # This is the latest version, which is handled below
                CurrentStudyLoader = study_loader_subcls

            if isinstance(df_dict, dict):
                # "headers" (from TableLoader) are "sheets" in this loader, because it is overloaded.  This loader has
                # no column headers of its own.  It just defines all the sheets and their individual loaders.
                expected_sheets = set(
                    study_loader_subcls.OrigDataHeaders._asdict().values()  # type: ignore[attr-defined]
                    # Not sure how to satisfy mypy here.  OrigDataHeaders is a namedtuple and has an _asdict
                    # attribute/method.  This strategy is an override of the superclass, which is where the attribute
                    # comes from.
                )
                common_sheets = list(expected_sheets.intersection(supplied_sheets))
                # TODO: This debug data should be tracked and be used/[resented when there is no version match.  For
                # now, keep it as a debug print.
                print(
                    f"V{study_loader_subcls.version_number} EXPECTED SHEETS: {expected_sheets} SUPPLIED SHEETS: "
                    f"{supplied_sheets}"
                )
                match = False
                if len(common_sheets) > 0:
                    common_data[study_loader_subcls.__name__]["sheets"] = common_sheets
                    # So far so good.  Let's assume it's a match unless the headers in each sheet say otherwise...
                    match = True
                    for sheet in common_sheets:
                        expected_headers = set(
                            study_loader_subcls.get_required_headers(sheet)
                        )
                        supplied_headers = set(list(df_dict[sheet].columns))
                        # TODO: This debug data should be tracked and be used/[resented when there is no version match.
                        # For now, keep it as a debug print.
                        print(
                            f"V{study_loader_subcls.version_number} SHEET {sheet} REQUIRED HEADERS: {expected_headers} "
                            f"SUPPLIED HEADERS: {supplied_headers}"
                        )
                        if not expected_headers <= supplied_headers:
                            # TODO: This debug data should be tracked and be used/[resented when there is no version
                            # match. For now, keep it as a debug print.
                            print(f"NOT {study_loader_subcls.version_number}")
                            match = False
                            break

                if match:
                    matching_version_numbers.append(
                        str(study_loader_subcls.version_number)
                    )

            else:  # pd.DataFrame
                # All we can do here (currently) is check that the headers in the dataframe are a subset of the
                # flattened original headers (from all the sheets).  It would be possible to do the determination by
                # specific sheet header contents if the class attributes were populated differently, but that can be
                # done via a refactor.
                supplied_headers = set(list(df_dict.columns))
                expected_headers = set(
                    study_loader_subcls.get_required_headers(sheet=None)
                )
                if expected_headers <= supplied_headers:
                    matching_version_numbers.append(
                        str(study_loader_subcls.version_number)
                    )

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

        return matching_version_numbers

    @classmethod
    def get_supported_versions(cls) -> List[str]:
        """Get a list of all supported version numbers (of the infile [aka, the 'study doc'])."""
        return [str(subcls.version_number) for subcls in StudyLoader.__subclasses__()]

    @classmethod
    def get_loader_class(cls, df_dict, version=None):
        loader_class: TableLoader

        if version is not None:
            version_numbers = [version]
        else:
            version_numbers = StudyLoader.determine_matching_versions(df_dict)

        if len(version_numbers) == 1:
            if version_numbers[0] == StudyV2Loader.version_number:
                loader_class = StudyV2Loader
            elif version_numbers[0] == StudyV3Loader.version_number:
                loader_class = StudyV3Loader
            else:
                raise InvalidStudyDocVersion(
                    f"Unrecognized version number: {version_numbers}."
                )
        elif len(version_numbers) == 0:
            raise UnknownStudyDocVersion(
                "Unable to determine study doc version.  Please supply one of the supported formats: "
                f"{StudyLoader.get_supported_versions()}."
            )
        else:
            raise MultipleStudyDocVersions(
                "Unable to identify study doc version.  Please supply one of these multiple matching formats: "
                f"{version_numbers}."
            )

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

    ConversionHeading = f"Study Doc v{version_number} Conversion Check"

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
        StudiesLoader.DataSheetName: [
            getattr(StudiesLoader.DataHeaders, hk)
            for hk in StudiesLoader.flatten_ndim_strings(
                StudiesLoader.DataRequiredHeaders
            )
        ],
        AnimalsLoader.DataSheetName: [
            getattr(AnimalsLoader.DataHeaders, hk)
            for hk in AnimalsLoader.flatten_ndim_strings(
                AnimalsLoader.DataRequiredHeaders
            )
        ],
        SamplesLoader.DataSheetName: [
            getattr(SamplesLoader.DataHeaders, hk)
            for hk in SamplesLoader.flatten_ndim_strings(
                SamplesLoader.DataRequiredHeaders
            )
        ],
        SequencesLoader.DataSheetName: [
            getattr(SequencesLoader.DataHeaders, hk)
            for hk in SequencesLoader.flatten_ndim_strings(
                SequencesLoader.DataRequiredHeaders
            )
        ],
        MSRunsLoader.DataSheetName: [
            getattr(MSRunsLoader.DataHeaders, hk)
            for hk in MSRunsLoader.flatten_ndim_strings(
                MSRunsLoader.DataRequiredHeaders
            )
        ],
        PeakAnnotationFilesLoader.DataSheetName: [
            getattr(PeakAnnotationFilesLoader.DataHeaders, hk)
            for hk in PeakAnnotationFilesLoader.flatten_ndim_strings(
                PeakAnnotationFilesLoader.DataRequiredHeaders
            )
        ],
        ProtocolsLoader.DataSheetName: [
            getattr(ProtocolsLoader.DataHeadersExcel, hk)
            for hk in ProtocolsLoader.flatten_ndim_strings(
                ProtocolsLoader.DataRequiredHeaders
            )
        ],
        TissuesLoader.DataSheetName: [
            getattr(TissuesLoader.DataHeaders, hk)
            for hk in TissuesLoader.flatten_ndim_strings(
                TissuesLoader.DataRequiredHeaders
            )
        ],
        InfusatesLoader.DataSheetName: [
            getattr(InfusatesLoader.DataHeaders, hk)
            for hk in InfusatesLoader.flatten_ndim_strings(
                InfusatesLoader.DataRequiredHeaders
            )
        ],
        TracersLoader.DataSheetName: [
            getattr(TracersLoader.DataHeaders, hk)
            for hk in TracersLoader.flatten_ndim_strings(
                TracersLoader.DataRequiredHeaders
            )
        ],
        CompoundsLoader.DataSheetName: [
            getattr(CompoundsLoader.DataHeaders, hk)
            for hk in CompoundsLoader.flatten_ndim_strings(
                CompoundsLoader.DataRequiredHeaders
            )
        ],
        LCProtocolsLoader.DataSheetName: [
            getattr(LCProtocolsLoader.DataHeaders, hk)
            for hk in LCProtocolsLoader.flatten_ndim_strings(
                LCProtocolsLoader.DataRequiredHeaders
            )
        ],
        StudyLoader.DataSheetName: [  # "Defaults"
            getattr(StudyLoader.DefaultsHeaders, hk)
            for hk in StudyLoader.flatten_ndim_strings(
                StudyLoader.DefaultsRequiredValues
            )
        ],
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

    ConversionHeading = f"Study Doc v{version_number} Conversion Check"

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

            study_study_code_header = loaders["STUDY"].DataHeaders.CODE
            study_study_name_header = loaders["STUDY"].DataHeaders.NAME
            study_study_desc_header = loaders["STUDY"].DataHeaders.DESCRIPTION

            study_dict = {
                study_study_code_header: {},
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
                    study_dict[study_study_code_header][new_i] = None
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
                    # trace in the console.  I should fix this so that buffered errors from the comnversion get their
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
                # Ensure what we get is a string, then convert it to a date (not a datetime)
                dfs_dict[sheet][date_header][idx] = datetime_to_string(
                    string_to_date(str(date_str))
                )

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

                # Fill a None into the sequence name column
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
