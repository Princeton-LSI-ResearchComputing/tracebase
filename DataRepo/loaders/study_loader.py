from collections import namedtuple
from typing import Dict, List, Type

from django.db import ProgrammingError
from django.db.models import Model

from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.compounds_loader import CompoundsLoader
from DataRepo.loaders.infusates_loader import InfusatesLoader
from DataRepo.loaders.lcprotocols_loader import LCProtocolsLoader
from DataRepo.loaders.msruns_loader import MSRunsLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
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
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AggregatedErrorsSet,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingStudies,
    AllMissingTissues,
    AllMissingTreatments,
    MissingCompounds,
    MissingModelRecordsByFile,
    MissingRecords,
    MissingSamples,
    MissingStudies,
    MissingTissues,
    MissingTreatments,
    MultiLoadStatus,
    NoSamples,
    RecordDoesNotExist,
)
from DataRepo.utils.file_utils import get_sheet_names, is_excel, read_from_file


class StudyLoader(TableLoader):
    """Loads an entire study doc (i.e. all of its sheets - not just the Study model)."""

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
    DataRequiredHeaders = [
        STUDY_SHEET,
        ANIMALS_SHEET,
        SAMPLES_SHEET,
        SEQUENCES_SHEET,
        HEADERS_SHEET,
        FILES_SHEET,
    ]

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
        Exceptions:
            Raises:
                AggregatedErrors
            Buffers:
                ValueError
        Returns:
            None
        """
        # NOTE: self.load_data() requires the file argument to have been provided to this constructor.
        if (
            kwargs.get("df") is not None
            or kwargs.get("defaults_df") is not None
            or kwargs.get("defer_rollback") is not None
        ):
            ValueError(
                f"The following superclass constructor arguments are prohibited by {type(self).__name__}: [df, "
                "defaults_df, defer_rollback]."
            )

        self.annot_files_dict = kwargs.pop("annot_files_dict", {})

        clkwa = self.CustomLoaderKwargs._asdict()
        clkwa["FILES"]["annot_files_dict"] = self.annot_files_dict
        # This occludes the CustomLoaderKwargs class attribute (which we copied and are leaving unchanged)
        # Just note that only the instance has annot_files_dict
        self.CustomLoaderKwargs = self.DataTableHeaders(**clkwa)

        super().__init__(*args, **kwargs)

        self.missing_study_record_exceptions = []
        self.missing_sample_record_exceptions = []
        self.no_sample_record_exceptions = []
        self.missing_tissue_record_exceptions = []
        self.missing_treatment_record_exceptions = []
        self.missing_compound_record_exceptions = []
        self.load_statuses = MultiLoadStatus(
            # Tell the MultiLoadStatus object we're going to try to load these files
            load_keys=[
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

        file_sheets = get_sheet_names(self.file)
        sheet_names = self.get_sheet_names_tuple()
        loaders = self.Loaders

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

        common_args = {
            "dry_run": self.dry_run,
            "defer_rollback": True,
            "defaults_sheet": self.defaults_sheet,
            "file": self.file,
            "filename": self.get_friendly_filename(),
            "defaults_file": self.defaults_file,
            "_validate": self.validate,
        }

        # TODO: Add support for custom args for every loader for each of these (given multiple inputs, e.g. a file for
        # each input: animals.csv, samples.tsv, etc)
        # file, user_headers, headers, extra_headers, defaults

        # TODO: Add support for custom study name delimiter to the loader
        # TODO: Add support for custom tracer name delimiter to the loader
        # TODO: Add support for custom isotope positions delimiter to the loader
        # TODO: Add support for custom synonyms delimiter to the loader

        disable_caching_updates()

        # This cycles through the loaders in the order in which they were defined in the namedtuple
        for loader_key in loaders._fields:
            if getattr(loaders, loader_key) is None:
                continue
            sheet = getattr(sheet_names, loader_key)
            loader_class: Type[TableLoader] = getattr(loaders, loader_key)
            custom_args = getattr(self.CustomLoaderKwargs, loader_key)

            if sheet in file_sheets:

                try:
                    # Build the keyword arguments to read_from_file
                    rffkwargs = {"sheet": sheet}
                    dtypes = self.get_loader_class_dtypes(loader_class)
                    if dtypes is not None and len(dtypes.keys()) > 0:
                        rffkwargs["dtype"] = dtypes

                    # Create a loader instance (e.g. CompoundsLoader())
                    loader = loader_class(
                        df=read_from_file(self.file, **rffkwargs),
                        data_sheet=sheet,
                        **common_args,
                        **custom_args,
                    )
                    # Load its data
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
            return loader_class._get_column_types(headers)

        # TODO Git rid of (/refactor) the ProtocolsLoader to not use this "DataHeadersExcel" class attribute
        if hasattr(loader_class, "DataHeadersExcel"):
            return loader_class._get_column_types(loader_class.DataHeadersExcel)

        return loader_class._get_column_types()

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

            # Collect all the missing samples in 1 error to add to the animal sample table file
            if len(exc_lst) > 0:
                self.load_statuses.set_load_exception(
                    exc_cls(exc_lst, succinct=succinct),
                    load_key,
                    top=True,
                )
