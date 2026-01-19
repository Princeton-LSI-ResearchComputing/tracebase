import base64
import os.path
from pathlib import Path
import warnings
from collections import defaultdict
from io import BytesIO
from typing import Dict, List, Optional, Type

import numpy as np
import pandas as pd
import xlsxwriter
import xlsxwriter.worksheet
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist, PermissionDenied
from django.db.utils import ProgrammingError
from django.forms import ValidationError, formset_factory
from django.views.generic.edit import FormView

from DataRepo.forms import create_BuildSubmissionForm
from DataRepo.loaders.animals_loader import AnimalsLoader
from DataRepo.loaders.base.table_column import ColumnReference
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
from DataRepo.loaders.study_loader import StudyLoader
from DataRepo.loaders.tissues_loader import TissuesLoader
from DataRepo.loaders.tracers_loader import TracersLoader
from DataRepo.models.archive_file import ArchiveFile
from DataRepo.models.compound import Compound
from DataRepo.models.infusate import Infusate
from DataRepo.models.infusate_tracer import InfusateTracer
from DataRepo.models.lc_method import LCMethod
from DataRepo.models.msrun_sequence import MSRunSequence
from DataRepo.models.peak_group import PeakGroup
from DataRepo.models.protocol import Protocol
from DataRepo.models.sample import Sample
from DataRepo.models.tracer import Tracer
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    AllMissingCompounds,
    AllMissingSamples,
    AllMissingStudies,
    AllMissingTissues,
    AllMissingTreatments,
    DuplicatePeakAnnotationFileName,
    FileFromInputNotFound,
    InvalidDtypeDict,
    InvalidPeakAnnotationFileFormat,
    InvalidStudyDocVersion,
    IsotopeParsingError,
    MissingDataAdded,
    MultiLoadStatus,
    MultiplePeakAnnotationFileFormats,
    MultipleStudyDocVersions,
    NoSamples,
    PeakAnnotationFileConflict,
    UnknownPeakAnnotationFileFormat,
    UnknownStudyDocVersion,
)
from DataRepo.utils.file_utils import get_sheet_names, is_excel, read_from_file
from DataRepo.utils.infusate_name_parser import (
    parse_infusate_name_with_concs,
    parse_tracer_string,
)
from DataRepo.utils.text_utils import autowrap


class BuildSubmissionView(FormView):
    template_name = "submission/submission.html"
    success_url = ""
    submission_url = settings.SUBMISSION_FORM_URL
    row_key_delim = "__DELIM__"
    none_vals = [
        "",  # Empty string is inferred to be None.  You cannot search for empty strings in the DB anyway.
        "nan",  # Derived from numpy
        "None",  # none_vals is(/should be) evaluated with the value inside str() to handle weird types
        "dummy",  # Some studies used this on rows for blank samples
        "NaT",  # This happens when a dfs_dict entry is changed to None (e.g. when replacing a "dummy" value)
        "<NA>",  # This happens when the type is a pandas nullable type and the cell was empty
    ]

    # TODO: Until all sheets are supported, this variable will be used to filter the sheets obtained from StudyLoader
    build_sheets = [
        StudiesLoader.DataSheetName,
        AnimalsLoader.DataSheetName,
        SamplesLoader.DataSheetName,
        ProtocolsLoader.DataSheetName,
        TissuesLoader.DataSheetName,
        CompoundsLoader.DataSheetName,
        TracersLoader.DataSheetName,
        InfusatesLoader.DataSheetName,
        LCProtocolsLoader.DataSheetName,
        SequencesLoader.DataSheetName,
        MSRunsLoader.DataSheetName,
        PeakAnnotationFilesLoader.DataSheetName,
        PeakGroupConflicts.DataSheetName,
    ]

    def __init__(self):
        if settings.READONLY:
            raise PermissionDenied

        super().__init__()

        self.autofill_dict = defaultdict(lambda: defaultdict(dict))
        self.autofill_dict[StudiesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[AnimalsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[SamplesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TissuesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[ProtocolsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[MSRunsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[CompoundsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TracersLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[InfusatesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[LCProtocolsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[SequencesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[PeakAnnotationFilesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[PeakGroupConflicts.DataSheetName] = defaultdict(dict)

        # For static dropdowns, we will use validation_autofill_dict.  Any value encountered in autofill_dict that is a
        # dict, when adding autofill values, will be skipped and the following dict will be populated:
        # validation_autofill_dict[sheet][header][rowindex] = {
        #     "validate": "list",
        #     "source": values_list,
        #     "show_error": show_error,
        #     "dropdown": True,
        # }
        self.validation_autofill_dict = defaultdict(
            lambda: defaultdict(lambda: defaultdict(dict))
        )

        # These are used to help determine whether to validate or only autofill.  If a non-autofill column has any
        # values, it will trigger validate mode
        self.autofill_columns = {
            StudiesLoader.DataSheetName: [
                StudiesLoader.DataHeaders.NAME,  # Consumes
            ],
            AnimalsLoader.DataSheetName: [
                AnimalsLoader.DataHeaders.NAME,  # Consumes
                AnimalsLoader.DataHeaders.TREATMENT,  # Feeds
                AnimalsLoader.DataHeaders.INFUSATE,  # Feeds
                AnimalsLoader.DataHeaders.STUDY,  # Feeds
            ],
            SamplesLoader.DataSheetName: [
                SamplesLoader.DataHeaders.SAMPLE,  # Consumes
                SamplesLoader.DataHeaders.TISSUE,  # Feeds
                SamplesLoader.DataHeaders.ANIMAL,  # Feeds
            ],
            TissuesLoader.DataSheetName: [
                TissuesLoader.DataHeaders.NAME,  # Consumes
            ],
            ProtocolsLoader.DataSheetName: [
                ProtocolsLoader.DataHeadersExcel.NAME,
            ],
            MSRunsLoader.DataSheetName: [
                MSRunsLoader.DataHeaders.SAMPLEHEADER,  # Consumes
                MSRunsLoader.DataHeaders.SAMPLENAME,  # Consumes
                MSRunsLoader.DataHeaders.ANNOTNAME,  # Consumes
                MSRunsLoader.DataHeaders.SKIP,  # Consumes
                MSRunsLoader.DataHeaders.SEQNAME,  # Feeds
            ],
            CompoundsLoader.DataSheetName: [
                CompoundsLoader.DataHeaders.NAME,  # Consumes
                CompoundsLoader.DataHeaders.FORMULA,  # Consumes
            ],
            TracersLoader.DataSheetName: [
                TracersLoader.DataHeaders.ID,  # Consumes
                TracersLoader.DataHeaders.NAME,  # Consumes
                TracersLoader.DataHeaders.COMPOUND,  # Feeds and Consumes
                TracersLoader.DataHeaders.ELEMENT,  # Consumes
                TracersLoader.DataHeaders.MASSNUMBER,  # Consumes
                TracersLoader.DataHeaders.LABELCOUNT,  # Consumes
                TracersLoader.DataHeaders.LABELPOSITIONS,  # Consumes
            ],
            InfusatesLoader.DataSheetName: [
                InfusatesLoader.DataHeaders.NAME,  # Consumes
                InfusatesLoader.DataHeaders.ID,  # Consumes
                InfusatesLoader.DataHeaders.TRACERGROUP,  # Consumes
                InfusatesLoader.DataHeaders.TRACERNAME,  # Feeds and Consumes
                InfusatesLoader.DataHeaders.TRACERCONC,  # Consumes
            ],
            LCProtocolsLoader.DataSheetName: [
                LCProtocolsLoader.DataHeaders.NAME,  # Consumes
                LCProtocolsLoader.DataHeaders.TYPE,  # Consumes
                LCProtocolsLoader.DataHeaders.RUNLEN,  # Consumes
            ],
            SequencesLoader.DataSheetName: [
                SequencesLoader.DataHeaders.SEQNAME,  # Consumes
                SequencesLoader.DataHeaders.OPERATOR,  # Consumes
                SequencesLoader.DataHeaders.LCNAME,  # Feeds and Consumes
                SequencesLoader.DataHeaders.INSTRUMENT,  # Consumes
                SequencesLoader.DataHeaders.DATE,  # Consumes
            ],
            PeakAnnotationFilesLoader.DataSheetName: [
                PeakAnnotationFilesLoader.DataHeaders.FILE,  # Consumes
                PeakAnnotationFilesLoader.DataHeaders.FORMAT,  # Consumes
                PeakAnnotationFilesLoader.DataHeaders.SEQNAME,  # Feeds
            ],
            PeakGroupConflicts.DataSheetName: [
                PeakGroupConflicts.DataHeaders.PEAKGROUP,  # Consumes
                PeakGroupConflicts.DataHeaders.SAMPLECOUNT,  # Consumes
                PeakGroupConflicts.DataHeaders.EXAMPLE,  # Consumes
                PeakGroupConflicts.DataHeaders.SAMPLES,  # Consumes
            ],
        }

        self.extracted_exceptions = defaultdict(lambda: {"errors": [], "warnings": []})
        self.valid = None
        self.state = None
        self.results = {}
        self.exceptions = {}
        self.ordered_keys = []
        self.load_status_data = MultiLoadStatus()

        self.studies_loader = StudiesLoader()
        self.animals_loader = AnimalsLoader()
        self.samples_loader = SamplesLoader()
        self.tissues_loader = TissuesLoader()
        self.treatments_loader = ProtocolsLoader(
            headers=ProtocolsLoader.DataHeadersExcel
        )
        self.compounds_loader = CompoundsLoader()
        self.tracers_loader = TracersLoader()
        self.infusates_loader = InfusatesLoader()
        self.lcprotocols_loader = LCProtocolsLoader()
        self.sequences_loader = SequencesLoader()
        self.msruns_loader = MSRunsLoader()
        self.peakannotfiles_loader = PeakAnnotationFilesLoader()
        self.peakgroupconflicts_loader = PeakGroupConflicts()
        self.peak_annotations_loaders = []
        self.loaders: Dict[str, TableLoader] = {
            StudiesLoader.DataSheetName: self.studies_loader,
            AnimalsLoader.DataSheetName: self.animals_loader,
            SamplesLoader.DataSheetName: self.samples_loader,
            TissuesLoader.DataSheetName: self.tissues_loader,
            ProtocolsLoader.DataSheetName: self.treatments_loader,
            CompoundsLoader.DataSheetName: self.compounds_loader,
            TracersLoader.DataSheetName: self.tracers_loader,
            InfusatesLoader.DataSheetName: self.infusates_loader,
            LCProtocolsLoader.DataSheetName: self.lcprotocols_loader,
            SequencesLoader.DataSheetName: self.sequences_loader,
            MSRunsLoader.DataSheetName: self.msruns_loader,
            PeakAnnotationFilesLoader.DataSheetName: self.peakannotfiles_loader,
            PeakGroupConflicts.DataSheetName: self.peakgroupconflicts_loader,
        }

        self.output_study_filename = "study.xlsx"
        self.autofill_only_mode = True
        self.dfs_dict = self.create_or_repair_study_dfs_dict()
        self.init_row_group_nums()
        self.study_file = None
        self.study_filename = None
        self.study_file_sheets = []
        self.annot_files_dict: Dict[str, str] = {}
        self.peak_annot_files = None
        self.peak_annot_filenames = []
        self.annot_file_metadata = {}

        # Data validation (e.g. dropdown menus) will be applied to the last fleshed row plus this offset
        self.validation_offset = 20

        # Excel cell styles
        border = 1
        border_color = "#D9D9D9"
        self.header_fmt = {
            "border": border,
            "bold": True,
        }
        self.data_row1_fmt = {
            "border": border,
            "left_color": border_color,
            "right_color": border_color,
            "bottom_color": border_color,
        }
        self.data_rown_fmt = {
            "border": border,
            "border_color": border_color,
        }
        self.excel_formats = {
            "database": {
                "fmt1": None,  # The format objects must be created from the workbook
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#D7E4BC"},
            },
            "autofill": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#E1EBCD"},
            },
            "optional": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#FFFFFF"},
            },
            "required": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#DCE6F1"},
            },
            "error": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#F2DCDB"},
            },
            "warning": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#FDE9D9"},
            },
            "readonly": {
                "fmt1": None,
                "fmtn": None,
                "fmth": None,
                "dict": {"bg_color": "#F2F2F2"},
            },
        }

    def get_form_class(self):
        return create_BuildSubmissionForm()

    def set_files(
        self,
        study_file=None,
        study_filename: Optional[str] = None,
        peak_annot_files=None,
        peak_annot_filenames=None,
    ):
        """
        This method allows the files to be set.  It takes 2 different optional params for file names (that are used in
        reporting) to accommodate random temporary file names.  If file names are not supplied, the basename of the
        actual files is used for reporting.
        """
        if peak_annot_files is None:
            peak_annot_files = []
        if peak_annot_filenames is None:
            peak_annot_filenames = []
        self.all_infile_names = []

        self.study_file = study_file
        self.study_filename = study_filename
        if study_filename is None and study_file is not None:
            self.study_filename = str(os.path.basename(study_file))

        if self.study_filename is not None:
            self.output_study_filename = self.study_filename
            self.all_infile_names.append(self.study_filename)

        if study_file is not None and is_excel(study_file):
            self.study_file_sheets = get_sheet_names(study_file)

        self.peak_annot_files = peak_annot_files
        if len(peak_annot_filenames) == 0 and len(peak_annot_files) > 0:
            peak_annot_filenames = [str(os.path.basename(f)) for f in peak_annot_files]

        if (
            len(peak_annot_filenames) == 0
            and len(peak_annot_filenames) > 0
            and (
                len(peak_annot_files) == 0
                or len(peak_annot_filenames) != len(peak_annot_files)
            )
        ):
            raise ProgrammingError(
                f"The number of peak annotation file names [{len(peak_annot_filenames)}] must be equal to the "
                f"number of peak annotation files [{peak_annot_files}]."
            )

        # Initializing a few variables before the same loop below that processes the files using the sheet loaders
        # We need an initialized self.annot_files_dict in order for get_or_create_dfs_dict to be able to convert a v2
        # study doc to a fleshed v3 study doc.
        # While we're at it, we'll also initialize self.peak_annot_filenames and self.peak_annotations_loaders.
        self.annot_files_dict.clear()
        self.peak_annot_filenames = []
        self.peak_annotations_loaders = []
        user_file_formats: Dict[str, str] = {}
        self.files_with_format_errors = []

        if peak_annot_files is not None and len(peak_annot_files) > 0:
            for index, peak_annot_file in enumerate(peak_annot_files):
                # Save the user's file name (different from the actual nonsense filename from the web form)
                peak_annot_filename = peak_annot_filenames[index]
                self.all_infile_names.append(peak_annot_filename)
                self.peak_annot_filenames.append(peak_annot_filename)
                if (
                    peak_annot_filename in self.annot_files_dict.keys()
                    and self.annot_files_dict[peak_annot_filename] != peak_annot_file
                ):
                    self.load_status_data.set_load_exception(
                        DuplicatePeakAnnotationFileName(peak_annot_filename),
                        peak_annot_filename,
                        top=False,
                    )
                    continue

                print("LLL CHECKING ANNOT FILES")
                # Check to see if this file was already loaded, but edited
                existing_annots = ArchiveFile.objects.filter(filename=peak_annot_filename)
                if existing_annots.count() > 0:
                    path_obj = Path(peak_annot_file)
                    computed_checksum = ArchiveFile.hash_file(path_obj)
                    differing_annot_files: List[ArchiveFile] = []
                    existing_annot: ArchiveFile
                    for existing_annot in existing_annots:
                        if computed_checksum != existing_annot.checksum:
                            differing_annot_files.append(existing_annot)

                    if len(differing_annot_files) > 0:
                        self.load_status_data.set_load_exception(
                            PeakAnnotationFileConflict(peak_annot_filename, differing_annot_files),
                            peak_annot_filename,
                            top=False,
                            default_is_error=False,
                        )

                # Map the user's filename to the web form file path
                self.annot_files_dict[peak_annot_filename] = peak_annot_file

        # Get an initial dfs_dict (a dict representation of the output study doc, either created or as obtained from the
        # user)
        self.dfs_dict = self.get_or_create_dfs_dict()
        self.init_row_group_nums()

        # Now that self.animal_sample_file, self.peak_annotation_files, and self.dfs_dict have been set, determine
        # validation readiness
        self.determine_study_file_validation_readiness()

        if self.study_file is not None:
            # The purpose of this block is to allow the user to specify the peak annotation file format (incase it's
            # ambiguous).  We just want a loader object to be able to read the file to extract missing samples and
            # compounds in the event we're not ready to validate.

            if PeakAnnotationFilesLoader.DataSheetName in get_sheet_names(
                self.study_file
            ):
                # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
                # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
                # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
                # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
                # in order to avoid accidental int(/etc) types...
                # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make
                # sure this solution is comprehensive before I refactor this code (more)
                dtypes, aes = PeakAnnotationFilesLoader._get_column_types(
                    optional_mode=True
                )

                df = self.read_from_file(
                    self.study_file,
                    sheet=PeakAnnotationFilesLoader.DataSheetName,
                    dtype=dtypes,
                )

                pafl = PeakAnnotationFilesLoader(
                    df=df,
                    file=self.study_file,
                    filename=self.study_filename,
                )
                pafl.aggregated_errors_object.merge_aggregated_errors_object(aes)

                for _, row in pafl.df.iterrows():
                    if pafl.is_row_empty(row):
                        continue

                    # "actual" means not the original file from the form submission, but the nonsense filename created
                    # by the browser
                    user_filename, _, user_format_code = pafl.get_file_and_format(row)

                    if pafl.is_skip_row():
                        continue

                    supported = PeakAnnotationsLoader.get_supported_formats()

                    if user_format_code is not None and user_format_code in supported:
                        user_file_formats[user_filename] = user_format_code
                    elif user_format_code is not None:
                        _, user_filename_only = os.path.split(user_filename)
                        self.load_status_data.set_load_exception(
                            InvalidPeakAnnotationFileFormat(
                                user_format_code,
                                supported,
                                annot_file=user_filename_only,
                                file=self.study_file,
                                sheet=PeakAnnotationFilesLoader.DataSheetName,
                                column=PeakAnnotationFilesLoader.DataHeaders.FORMAT,
                                rownum=row.name + 2,
                                suggestion=(
                                    f"Please enter the correct '{PeakAnnotationFilesLoader.DataHeaders.FORMAT}' for "
                                    f"'{PeakAnnotationFilesLoader.DataHeaders.FILE}' '{user_filename_only}' in the "
                                    f"'{PeakAnnotationFilesLoader.DataSheetName}' sheet of '{self.study_filename}'.  "
                                    "Note that format determination is made using sheet names and column headers.  If "
                                    "a csv or tsv was supplied, the determination can be ambiguous due to common "
                                    "header names."
                                ),
                            ),
                            user_filename_only,
                            top=False,
                        )

        # Now we will sort through the peak annotation files that were supplied and create loader objects for them to
        # extract autofill data
        peak_annot_loader_class: Type[PeakAnnotationsLoader]
        allowed_exts = [".csv", ".tsv"]
        exc: Exception

        if peak_annot_files is not None and len(peak_annot_files) > 0:
            for index, peak_annot_file in enumerate(peak_annot_files):
                peak_annot_filename = peak_annot_filenames[index]
                ext = list(os.path.splitext(peak_annot_filename))[1]

                # Now we will determine the format to decide which loader to create.
                # We will defer to the user's supplied format (if any)
                matching_formats = []
                print("Reading peak annotations file")
                # Do not enforce column types when we don't know what columns exist yet
                df = read_from_file(peak_annot_file, sheet=None)
                if peak_annot_filename in user_file_formats.keys():
                    user_format_code = user_file_formats[peak_annot_filename]
                    matching_formats = [user_format_code]
                else:
                    print("Determining format of peak annotation file")
                    matching_formats = PeakAnnotationsLoader.determine_matching_formats(
                        df
                    )

                if len(matching_formats) == 1:
                    if matching_formats[0] == AccucorLoader.format_code:
                        peak_annot_loader_class = AccucorLoader
                    elif matching_formats[0] == IsocorrLoader.format_code:
                        peak_annot_loader_class = IsocorrLoader
                    elif matching_formats[0] == IsoautocorrLoader.format_code:
                        peak_annot_loader_class = IsoautocorrLoader
                    elif matching_formats[0] == UnicorrLoader.format_code:
                        peak_annot_loader_class = UnicorrLoader
                    else:
                        raise ProgrammingError(
                            f"Unrecognized format code: {matching_formats}."
                        )

                    print("Creating peak annotations loader")
                    # Create an instance of the loader, appended onto the loaders
                    try:
                        self.peak_annotations_loaders.append(
                            peak_annot_loader_class(
                                # These are the essential arguments
                                df=df,
                                file=peak_annot_file,
                                filename=peak_annot_filename,
                                # We don't need the default sequence info - we only want to read the file
                            )
                        )
                    except AggregatedErrors as aes:
                        self.load_status_data.set_load_exception(
                            aes,
                            peak_annot_filename,
                        )
                        self.peak_annotations_loaders.append(None)
                elif (
                    ext in allowed_exts
                    and self.autofill_only_mode
                    and set(matching_formats) == set(["isocorr", "isoautocorr"])
                ):
                    # For the purposes of autofill, "isocorr" and "isoautocorr" versions are similar enough to be able
                    # to extract data using one or the other version
                    peak_annot_loader_class = IsocorrLoader
                    self.files_with_format_errors.append(peak_annot_filename)
                    print("Creating peak annotations loader")
                    # Create an instance of the loader, appended onto the loaders
                    try:
                        self.peak_annotations_loaders.append(
                            peak_annot_loader_class(
                                # These are the essential arguments
                                df=df,
                                file=peak_annot_file,
                                filename=peak_annot_filename,
                                # We don't need the default sequence info - we only want to read the file
                            )
                        )
                    except AggregatedErrors as aes:
                        self.load_status_data.set_load_exception(
                            aes,
                            peak_annot_filename,
                        )
                        self.peak_annotations_loaders.append(None)

                    # Log a warning for the user
                    suggestion = (
                        f"The format of peak annotation file '{peak_annot_filename}' is being temporarily set to "
                        "'isocorr' for sample name extraction.  You must add the correct format (one of "
                        f"{matching_formats}) on row {index + 2} to the 'File Format' column in the 'Peak Annotation "
                        "Files' sheet for this file."
                    )

                    exc = MultiplePeakAnnotationFileFormats(
                        matching_formats,
                        file=peak_annot_filename,
                        suggestion=suggestion,
                    )

                    self.load_status_data.set_load_exception(
                        exc,
                        peak_annot_filename,
                        top=False,
                        default_is_error=False,
                        default_is_fatal=True,
                    )
                else:
                    suggestion = (
                        "Unable to process the file.  Note that format determination is made using sheet names and "
                        "column headers.  If a csv or tsv was supplied, the determination can be ambiguous due to "
                        "common header names."
                    )

                    if len(matching_formats) == 0:
                        self.peak_annotations_loaders.append(None)
                        exc = UnknownPeakAnnotationFileFormat(
                            PeakAnnotationsLoader.get_supported_formats(),
                            file=peak_annot_filename,
                            suggestion=suggestion,
                        )
                    else:
                        self.peak_annotations_loaders.append(None)
                        exc = MultiplePeakAnnotationFileFormats(
                            matching_formats,
                            file=peak_annot_filename,
                            suggestion=suggestion,
                        )

                    self.load_status_data.set_load_exception(
                        exc,
                        peak_annot_filename,
                        top=False,
                    )

                # Go through the loaders and log any exceptions that occurred
                pal: PeakAnnotationsLoader = self.peak_annotations_loaders[-1]
                if pal is not None and len(pal.aggregated_errors_object.exceptions) > 0:
                    self.load_status_data.set_load_exception(
                        pal.aggregated_errors_object,
                        peak_annot_filename,
                    )

    def read_from_file(
        self, file, sheet=None, dtype=None, top=True, default_is_error=False
    ):
        """Wraps read_from_file in a try block.

        Args:
            file (str)
            sheet (str)
            dtype (dict): Types keyed by header.  At least 1 header must be present in the file.
            top (bool) [True]
            default_is_error (bool) [False]
        Exceptions:
            Buffered:
                InvalidDtypeDict
            Raises:
                None
        Returns:
            df (Union[DataFrame, Dict[str, DataFrame]])
        """
        try:
            df = read_from_file(file, sheet=sheet, dtype=dtype)
        except InvalidDtypeDict as idd:
            if (
                StudyLoader.ConversionHeading
                not in self.load_status_data.statuses.keys()
            ):
                self.load_status_data.set_load_exception(
                    idd,
                    StudyLoader.ConversionHeading,
                    top=top,
                    default_is_error=default_is_error,
                )
            # Avoid exception by not supplying dtypes & let pandas decide types (which can cause date problems)
            df = read_from_file(file, sheet=sheet)

        return df

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # "page" may have been added to the context by the posted form, in which case, it won't be in the GET data
        if "page" not in context.keys():
            page = self.request.GET.get("page", None)
            context["page"] = page
        else:
            page = context["page"]

        # Set the submission mode in the form based on the page
        # Mode on the start page is autofill
        mode = None
        if page is None or page == "Start":
            mode = "autofill"
        elif page == "Validate":
            mode = "validate"
        elif page == "Submit":
            context["submission_feedback_url"] = settings.FEEDBACK_URL
            context["submission_form_url"] = settings.SUBMISSION_FORM_URL
            context["submission_drive_doc_url"] = settings.SUBMISSION_DRIVE_DOC_URL
            context["submission_drive_type"] = settings.SUBMISSION_DRIVE_TYPE
            context["submission_drive_folder"] = settings.SUBMISSION_DRIVE_FOLDER
        elif page != "Fill In":
            raise ProgrammingError(f"Invalid page: {context['page']}")

        # Note, this logic developed due to the fact that form_invalid in the superclass was a formset, but the view
        # uses a regular form generated by javascript.  Once I overrode form_invalid to handle errors, this logic likely
        # no longer is needed, but it doesn't hurt, and it might help out if the code is edited later, so I'm going to
        # keep it.
        if (
            "form" not in context.keys()
            or not hasattr(context["form"], "fields")
            or not hasattr(context["form"].fields["mode"], "initial")
            or context["form"].fields["mode"].initial is None
            or context["form"].fields["mode"].initial not in ["autofill", "validate"]
        ):
            if "form" not in context.keys() or not hasattr(context["form"], "fields"):
                form_class = self.get_form_class()
                form = self.get_form(form_class)
            else:
                form = context["form"]

            form.fields["mode"].initial = mode
            context["form"] = form

        return context

    def dispatch(self, request, *args, **kwargs):
        return super(BuildSubmissionView, self).dispatch(request, *args, **kwargs)

    def post(self, *args, **kwargs):
        formset_class = formset_factory(self.get_form_class())
        formset = self.get_form(formset_class)

        if (
            not formset.is_valid()
            # mode is required in the first form only
            or "mode" not in formset.cleaned_data[0].keys()
            or formset.cleaned_data[0]["mode"] not in ["autofill", "validate"]
        ):
            return self.form_invalid(formset)

        return self.form_valid(formset)

    def form_valid(self, formset):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """

        # Process the annot file formset
        mode = None
        study_file_object = None
        study_file = None
        study_filename = None
        peak_annot_files = []
        peak_annot_filenames = []
        self.annot_file_metadata = {}
        rowform: dict
        for index, rowform in enumerate(formset.cleaned_data):
            # We only need/allow a single mode and study_doc, which is saved only in the first rowform
            if index == 0:
                mode = rowform["mode"]
                study_file_object = rowform.get("study_doc")
                if study_file_object is not None:
                    study_file = study_file_object.temporary_file_path()
                    study_filename = str(study_file_object)

            # Process the peak annotation files
            peak_annotation_file_object = rowform.get("peak_annotation_file")
            if peak_annotation_file_object is not None:
                peak_annot_file = peak_annotation_file_object.temporary_file_path()
                peak_annot_filename = str(peak_annotation_file_object)

                peak_annot_files.append(peak_annot_file)
                peak_annot_filenames.append(peak_annot_filename)

                self.annot_file_metadata[peak_annot_filename] = {
                    "operator": rowform.get("operator"),
                    "protocol": rowform.get("protocol"),
                    "instrument": rowform.get("instrument"),
                    # Date key is selected to be compatible with MSRunSequence.create_sequence_name
                    "date": rowform.get("run_date"),
                }

        self.load_status_data.clear_load()

        self.set_files(
            study_file,
            study_filename=study_filename,
            peak_annot_files=peak_annot_files,
            peak_annot_filenames=peak_annot_filenames,
        )

        debug = f"sf: {self.study_file} num pafs: {len(self.peak_annot_files)}"

        study_data = self.get_download_data()

        # Determine the page based on the mode submitted in the form (validate or autofill)
        # Example of cleaned_data:
        #   [
        #     {
        #       'study_doc': None,
        #       'peak_annotation_file': <TemporaryUploadedFile: col013a_negative_cor.xlsx
        #         (application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)>,
        #       'operator': 'Anonymous',
        #       'protocol': 'unknown',
        #       'instrument': 'QE (Q Exactive)',
        #       'run_date': '2024-09-18',
        #     },
        #     {
        #       'study_doc': None,
        #       'peak_annotation_file': <TemporaryUploadedFile: col013a_pos-highmz_cor.xlsx
        #         (application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)>,
        #       'operator': 'Anonymous',
        #       'protocol': 'unknown',
        #       'instrument': 'QE (Q Exactive)',
        #       'run_date': '2024-09-18',
        #     },
        #   ]
        if mode == "autofill":
            page = "Start"
        elif mode == "validate":
            page = "Validate"
        else:
            raise ProgrammingError(f"Invalid mode: {mode}")

        # We need a new regular form (not a formset)
        form_class = self.get_form_class()
        # Cannot use the form.fields["mode"].initial trick to set the value for the hidden "mode" field here, because
        # for some reason, we end up with a bound form here (and that method doesn't work on bound forms).  So we set
        # the initial value using the constructor method.
        form = form_class(initial={"mode": mode})

        return self.render_to_response(
            self.get_context_data(
                results=self.results,
                debug=debug,
                valid=self.valid,
                state=self.state,
                form=form,
                page=page,
                exceptions=self.exceptions,
                submission_url=self.submission_url,
                ordered_keys=self.ordered_keys,
                study_data=study_data,
                study_filename=self.output_study_filename,
                quiet_mode=self.autofill_only_mode,
            ),
        )

    def form_invalid(self, formset):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """
        # Get the mode from the unparsed data (since cleaned_data will not exist)
        mode = formset.__dict__["data"].get("form-0-mode")
        if mode == "autofill":
            page = "Start"
        elif mode == "validate":
            page = "Validate"
        else:
            raise ProgrammingError(f"Invalid mode: {mode}")

        # We need a new regular form (not a formset)
        form_class = self.get_form_class()
        # Cannot use the form.fields["mode"].initial trick to set the value for the hidden "mode" field here, because
        # for some reason, we end up with a bound form here (and that method doesn't work on bound forms).  So we set
        # the initial value using the constructor method.
        form = form_class(initial={"mode": mode})

        # The errors are stored in the formset object, but we're not using the formset object in the template.  We're
        # using a basic form and converting it into a formset on submit with javascript, so we need a way to transmit
        # errors.
        form_errors = []
        for formset_error in formset._errors:
            for errlst in formset_error.values():
                for errstr in errlst:
                    if errstr not in form_errors:
                        form_errors.append(errstr)

        return self.render_to_response(
            self.get_context_data(
                results=None,
                debug=None,
                valid=None,
                state=None,
                form=form,
                form_errors=form_errors,
                page=page,
                exceptions=None,
                submission_url=self.submission_url,
                ordered_keys=None,
                study_data=None,
                study_filename=None,
                quiet_mode=self.autofill_only_mode,
            ),
        )

    def get_download_data(self):
        """This does all the processing of the submission after set_files() has been called."""

        # Initialize a status object for the results for each input file
        # TODO: Make the MultiLoadStatus class more of a "status" class for multuple "categories" of things that
        # must succeed (as opposed to just load-related things)
        self.load_status_data.update_load(load_key=self.all_infile_names)

        if self.autofill_only_mode:
            # autofill_only_mode means that there was no study file submitted.  (The form validation guarantees that
            # we have at least 2 peak annotation file.)
            print("Extracting autofill information from file")

            # Extract autofill data directly from the peak annotation files
            self.extract_autofill_from_files_and_forms()
        else:
            print("Validating study")
            self.validate_study()

            print("extracting autofill information from exceptions")
            # Extract errors from the validation that can be used to autofill missing values in the study doc
            self.extract_autofill_from_exceptions(
                retain_as_warnings=not self.autofill_only_mode,
            )

        self.format_results_for_template()

        self.add_extracted_autofill_data()

        self.add_dynamic_dropdown_data()

        self.fill_missing_from_db()

        study_stream = BytesIO()

        xlsx_writer = self.create_study_file_writer(study_stream)

        self.annotate_study_excel(xlsx_writer)

        xlsx_writer.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        study_stream.seek(0)

        study_data = base64.b64encode(study_stream.read()).decode("utf-8")

        return study_data

    def init_row_group_nums(self):
        # We need to get the next available infusates sheet row group number
        inf_row_group_nums = [
            int(i)
            for i in self.dfs_dict[InfusatesLoader.DataSheetName][
                InfusatesLoader.DataHeaders.ID
            ].values()
            if i is not None
        ]
        self.next_infusate_row_group_num = 1
        if len(inf_row_group_nums) > 0:
            self.next_infusate_row_group_num = max(inf_row_group_nums) + 1

        # We need to get the next available tracers sheet row group number
        trcr_row_group_nums = [
            int(i)
            for i in self.dfs_dict[TracersLoader.DataSheetName][
                TracersLoader.DataHeaders.ID
            ].values()
            if str(i) not in self.none_vals
        ]
        self.next_tracer_row_group_num = 1
        if len(trcr_row_group_nums) > 0:
            self.next_tracer_row_group_num = max(trcr_row_group_nums) + 1

    def determine_study_file_validation_readiness(self):
        """Determines if the data is ready for validation by seeing if the study doc has any values in it other than
        sample names and if there are peak annotation files to populate it.  It does this by inspecting
        self.study_file, self.peak_annot_files, and the data parsed into self.dfs_dict from self.study_file.

        The purpose of this method is to avoid time consuming processing of a file that is only destined to produce
        missing sample name errors by identifying this futile case and setting self.autofill_only_mode to True (or
        False).

        Args:
            None
        Exceptions:
            None
        Returns:
            ready_for_validation (boolean): The opposite of the autofill_only_mode
        """
        if self.peak_annot_files is None or len(self.peak_annot_files) == 0:
            # Cannot do autofill-only if there is no source of autofill data (currently)
            # This can happen if there were exceptions (like missing required headers)
            self.autofill_only_mode = False
            return not self.autofill_only_mode

        if self.study_file is None or not self.dfs_dict_is_valid():
            # If there's no study data, we will want to autofill
            self.autofill_only_mode = True
            return not self.autofill_only_mode

        loader: TableLoader
        for loader in [
            # Loaders that aren't completely autofilled
            self.samples_loader,
            self.animals_loader,
            self.studies_loader,
            self.sequences_loader,
        ]:
            if loader.DataSheetName not in self.dfs_dict.keys():
                continue
            for header in loader.get_ordered_display_headers():
                # Validate if *any* optional data has been added to the samples sheet
                if (
                    # If the column is an autofill column or a column that feeds an autofill column, ignore it
                    header in self.autofill_columns[loader.DataSheetName]
                    # Skip columns that aren't present
                    or header not in self.dfs_dict[loader.DataSheetName].keys()
                ):
                    # Skipping the Sample header and any unknown header
                    continue

                for val in self.dfs_dict[loader.DataSheetName][header].values():
                    # TODO: Instead of doing this none_vals strategy, dynamically create a dataframe out of the dfs_dict
                    # and then use get_row_val and check if the result is None
                    # TODO: I should probably alter this strategy and check if all required columns have values on every
                    # row?
                    if val is not None and str(val) not in self.none_vals:
                        print(
                            f"GOING INTO VALIDATION MODE BECAUSE {loader.DataSheetName} has a value in column {header}"
                        )
                        # If *any* data has been manually added, we will validate
                        self.autofill_only_mode = False
                        return not self.autofill_only_mode

        # There is nothing that needs to be validated
        self.autofill_only_mode = True
        return not self.autofill_only_mode

    def annotate_study_excel(self, xlsx_writer):
        """Add annotations, formulas, colors (indicating errors/warning/required-values/read-only-values/etc).

        Also performs some formatting, such as setting the column width.

        Args:
            xlsx_writer (xlsxwriter): A study doc in an xlsx writer object.
        Exceptions:
            None
        Returns:
            None
        """
        # TODO: Use the xlsx_writer to decorate the excel sheets with errors/warnings as cell comments, colors to
        # indicate errors/warning/required-values/read-only-values, and formulas for inter-sheet population of
        # dropdowns.
        column_metadata = {
            StudiesLoader.DataSheetName: self.studies_loader.get_header_metadata(),
            AnimalsLoader.DataSheetName: self.animals_loader.get_header_metadata(),
            SamplesLoader.DataSheetName: self.samples_loader.get_header_metadata(),
            TissuesLoader.DataSheetName: self.tissues_loader.get_header_metadata(),
            ProtocolsLoader.DataSheetName: self.treatments_loader.get_header_metadata(),
            CompoundsLoader.DataSheetName: self.compounds_loader.get_header_metadata(),
            TracersLoader.DataSheetName: self.tracers_loader.get_header_metadata(),
            InfusatesLoader.DataSheetName: self.infusates_loader.get_header_metadata(),
            LCProtocolsLoader.DataSheetName: self.lcprotocols_loader.get_header_metadata(),
            SequencesLoader.DataSheetName: self.sequences_loader.get_header_metadata(),
            MSRunsLoader.DataSheetName: self.msruns_loader.get_header_metadata(),
            PeakAnnotationFilesLoader.DataSheetName: self.peakannotfiles_loader.get_header_metadata(),
            PeakGroupConflicts.DataSheetName: self.peakgroupconflicts_loader.get_header_metadata(),
        }
        for order_spec in StudyLoader.get_study_sheet_column_display_order():
            sheet = order_spec[0]
            if sheet not in self.build_sheets:
                # Skipping unsupported sheets
                continue

            worksheet = xlsx_writer.sheets[sheet]
            headers = order_spec[1]

            # Add comments to header cells
            for header in headers:
                comment = column_metadata[sheet][header].comment
                if comment is not None:
                    cell = self.header_to_cell(sheet=sheet, header=header)
                    wrapped, nlines, nchars = autowrap(comment)
                    font_width = 8
                    font_height = 10
                    line_height = 18
                    worksheet.write_comment(
                        cell,
                        wrapped,
                        {
                            "author": "TraceBase Dev Team",
                            "font_name": "Courier",  # Fixed width for box height/width calcs
                            "font_size": font_height,
                            "height": nlines * line_height,
                            "width": nchars * font_width,
                        },
                    )
            xlsx_writer.sheets[sheet].autofit()

        # Hide the peak groups conflict sheet if it is empty.  (This checks the number of rows in each column.)
        if (
            len(
                [
                    d
                    for d in self.dfs_dict[PeakGroupConflicts.DataSheetName].values()
                    if len(d.keys()) > 0
                ]
            )
            == 0
        ):
            print("HIDING EMPTY CONFLICTS SHEET")
            xlsx_writer.sheets[PeakGroupConflicts.DataSheetName].hide()

        self.add_dropdowns(xlsx_writer)
        self.add_formulas(xlsx_writer)
        self.hide_hidden_columns(xlsx_writer)

    def hide_hidden_columns(self, xlsx_writer):
        loader: TableLoader
        for loader in [
            self.studies_loader,
            self.animals_loader,
            self.samples_loader,
            self.tissues_loader,
            self.treatments_loader,
            self.compounds_loader,
            self.tracers_loader,
            self.infusates_loader,
            self.lcprotocols_loader,
            self.sequences_loader,
            self.msruns_loader,
            self.peakannotfiles_loader,
            self.peakgroupconflicts_loader,
        ]:
            sheet = loader.DataSheetName
            if sheet not in self.build_sheets or sheet not in self.dfs_dict.keys():
                # Skipping unsupported and not present (PeakGroupConflicts) sheets
                continue

            worksheet: xlsxwriter.worksheet.Worksheet = xlsx_writer.sheets[sheet]

            for hidden_header in loader.get_hidden_columns():
                print(f"HIDING {sheet}, {hidden_header}")
                letter = self.header_to_cell(
                    sheet=sheet, header=hidden_header, letter_only=True
                )
                worksheet.set_column(f"{letter}:{letter}", None, None, {"hidden": True})

    def add_dropdowns(self, xlsx_writer):
        print("Adding whole column dropdowns")
        self.add_whole_column_dropdowns(xlsx_writer)
        print("Adding autofill dropdowns")
        self.add_autofill_dropdowns(xlsx_writer)

    def add_whole_column_dropdowns(self, xlsx_writer):
        column_metadata = {
            StudiesLoader.DataSheetName: self.studies_loader.get_value_metadata(),
            AnimalsLoader.DataSheetName: self.animals_loader.get_value_metadata(),
            SamplesLoader.DataSheetName: self.samples_loader.get_value_metadata(),
            TissuesLoader.DataSheetName: self.tissues_loader.get_value_metadata(),
            ProtocolsLoader.DataSheetName: self.treatments_loader.get_value_metadata(),
            CompoundsLoader.DataSheetName: self.compounds_loader.get_value_metadata(),
            TracersLoader.DataSheetName: self.tracers_loader.get_value_metadata(),
            InfusatesLoader.DataSheetName: self.infusates_loader.get_value_metadata(),
            LCProtocolsLoader.DataSheetName: self.lcprotocols_loader.get_value_metadata(),
            SequencesLoader.DataSheetName: self.sequences_loader.get_value_metadata(),
            MSRunsLoader.DataSheetName: self.msruns_loader.get_value_metadata(),
            PeakAnnotationFilesLoader.DataSheetName: self.peakannotfiles_loader.get_value_metadata(),
            PeakGroupConflicts.DataSheetName: self.peakgroupconflicts_loader.get_value_metadata(),
        }
        for order_spec in StudyLoader.get_study_sheet_column_display_order():
            sheet = order_spec[0]
            if sheet not in self.build_sheets:
                # Skipping unsupported sheets
                continue

            last_validation_index = (
                self.get_next_row_index(sheet) + self.validation_offset
            )
            worksheet: xlsxwriter.worksheet.Worksheet = xlsx_writer.sheets[sheet]
            headers = order_spec[1]

            for header in headers:
                # static_choices can be set automatically via the field, manually, or via "current_choices" (e.g. a
                # distinct database query)
                static_choices = column_metadata[sheet][header].static_choices
                current_choices = column_metadata[sheet][header].current_choices
                dynamic_choices: ColumnReference = column_metadata[sheet][
                    header
                ].dynamic_choices

                if static_choices is None and dynamic_choices is None:
                    continue

                # Get the cell's letter designation as it will be in excel
                col_letter = self.header_to_cell(
                    sheet=sheet, header=header, letter_only=True
                )
                cell_range = f"{col_letter}2:{col_letter}{last_validation_index}"

                if static_choices is not None:
                    # Novel values are allowed for static choices when static choices are populated by a distinct
                    # database query
                    show_error = current_choices is False

                    values_list = sorted(
                        [
                            tpl[0]
                            for tpl in static_choices
                            if tpl is not None and tpl[0] is not None
                        ]
                    )

                    if len(str(values_list)) > 255:
                        # Excel limits dropdown lists to 255 items
                        warnings.warn(
                            f"The dropdown list for sheet {sheet}, column {header} exceeds Excel's limit of 255 "
                            "characters.  The list has been truncated.  Please consider changing the "
                            "DataColumnMetadata settings for the associated loader class."
                        )
                        while len(str(values_list)) > 252:
                            values_list.pop()
                        values_list = [*values_list[0:253], "..."]

                    # Add a dropdown menu for all cells in this column (except the header) to the last_validation_index
                    worksheet.data_validation(
                        cell_range,
                        {
                            "validate": "list",
                            "source": values_list,
                            "show_error": show_error,
                            "dropdown": True,
                        },
                    )
                elif dynamic_choices is not None:
                    next_ref_index = self.get_next_row_index(dynamic_choices.sheet)
                    last_ref_validation_index = next_ref_index + self.validation_offset
                    ref_col_letter = self.header_to_cell(
                        sheet=dynamic_choices.sheet,
                        header=dynamic_choices.header,
                        letter_only=True,
                    )
                    ref_cell_range = ""
                    if sheet != dynamic_choices.sheet:
                        ref_cell_range += f"'{dynamic_choices.sheet}'!"
                    ref_cell_range += f"${ref_col_letter}$2:${ref_col_letter}${last_ref_validation_index}"

                    print(
                        f"ADDING DYNAMIC CHOICES TO SHEET {sheet}, COLUMN {header}: {ref_cell_range}"
                    )
                    # Add a dropdown menu for all cells in this column (except the header) to the last_validation_index
                    worksheet.data_validation(
                        cell_range,
                        {
                            "validate": "list",
                            "source": ref_cell_range,
                            "show_error": False,
                            "dropdown": True,
                        },
                    )

    def add_autofill_dropdowns(self, xlsx_writer):
        """Populates individual cell dropdowns from the validation_autofill_dict."""
        for sheet in (
            StudiesLoader.DataSheetName,
            AnimalsLoader.DataSheetName,
            SamplesLoader.DataSheetName,
            TissuesLoader.DataSheetName,
            ProtocolsLoader.DataSheetName,
            CompoundsLoader.DataSheetName,
            TracersLoader.DataSheetName,
            InfusatesLoader.DataSheetName,
            LCProtocolsLoader.DataSheetName,
            SequencesLoader.DataSheetName,
            MSRunsLoader.DataSheetName,
            PeakAnnotationFilesLoader.DataSheetName,
            PeakGroupConflicts.DataSheetName,
        ):
            if sheet not in self.validation_autofill_dict.keys():
                continue

            worksheet: xlsxwriter.worksheet.Worksheet = xlsx_writer.sheets[sheet]

            for header in self.validation_autofill_dict[sheet].keys():
                letter = self.header_to_cell(
                    sheet=sheet,
                    header=header,
                    letter_only=True,
                )
                for rowindex, validation_dict in self.validation_autofill_dict[sheet][
                    header
                ].items():
                    rownum = rowindex + 2  # Indexed from 1, and skip the header
                    worksheet.data_validation(
                        f"{letter}{rownum}",
                        validation_dict,
                    )

    def add_formulas(self, xlsx_writer):
        loader: TableLoader
        for loader in [
            self.studies_loader,
            self.animals_loader,
            self.samples_loader,
            self.tissues_loader,
            self.treatments_loader,
            self.compounds_loader,
            self.tracers_loader,
            self.infusates_loader,
            self.lcprotocols_loader,
            self.sequences_loader,
            self.msruns_loader,
            self.peakannotfiles_loader,
            self.peakgroupconflicts_loader,
        ]:
            sheet = loader.DataSheetName
            if sheet not in self.build_sheets:
                # Skipping unsupported sheets
                continue

            last_validation_index = (
                self.get_next_row_index(sheet) + self.validation_offset
            )
            worksheet: xlsxwriter.worksheet.Worksheet = xlsx_writer.sheets[sheet]

            # Create a dict of header keys to excel column letter for all headers
            headers_dict = loader.get_headers()._asdict()
            header_letters_dict = {}
            for header_key, header in headers_dict.items():
                if header in self.dfs_dict[sheet].keys():
                    header_letters_dict[header_key] = self.header_to_cell(
                        sheet=sheet, header=header, letter_only=True
                    )

            column_metadata = loader.get_value_metadata()

            for header_key, header in headers_dict.items():
                if header not in self.dfs_dict[sheet].keys():
                    continue

                formula_template = column_metadata[header].formula

                if formula_template is None:
                    continue

                col_letter = header_letters_dict[header_key]

                # This substitutes instances of "{NAME}" (where "NAME" is a TableLoader.DataHeaders namedtuple
                # attribute) with the column letter for each header that occurs in the formula
                formula = formula_template.format(**header_letters_dict)

                for index in range(last_validation_index):
                    # Indexes start from 0.  Any value at index zero ends up in what excel labels as row 2 (because row
                    # numbers start at 1 and row 1 is the header).
                    rownum = index + 2

                    # Only fill in a formula if the cell doesn't already have a value.
                    # (Because custom-entered values from the user trump formulas).
                    if self.dfs_dict[sheet][header].get(index) is None:
                        print(
                            f"INSERTING FORMULA FOR SHEET {sheet}, COLUMN {header}, INDEX {index}, "
                            f"LOCATION {col_letter}{rownum}: {formula}"
                        )
                        worksheet.write_formula(
                            f"{col_letter}{rownum}", formula, None, ""
                        )

    def extract_autofill_from_files_and_forms(self):
        """Calls methods for extracting autofill data from the submitted forms (including from the submitted files)."""
        self.extract_autofill_from_study_doc()
        self.extract_autofill_from_sequence_forms()
        self.extract_autofill_from_peak_annotation_files_forms()

    def extract_autofill_from_study_doc(self):
        """Extracts data from various sheets of the study doc.

        Populates self.autofill_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if self.study_file is None:
            return

        self.extract_autofill_from_animals_sheet()
        self.extract_autofill_from_samples_sheet()
        self.extract_autofill_from_peak_annot_files_sheet()
        self.extract_autofill_from_peak_annot_details_sheet()
        self.extract_autofill_from_infusates_sheet()
        self.extract_autofill_from_tracers_sheet()
        self.extract_autofill_from_sequences_sheet()

    def extract_autofill_from_animals_sheet(self):
        if AnimalsLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = AnimalsLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=AnimalsLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = AnimalsLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)
        seen = {
            "infusates": defaultdict(dict),
            "tracers": defaultdict(dict),
            "treatments": defaultdict(dict),
            "studies": defaultdict(dict),
            "compounds": defaultdict(dict),
        }

        # Convenience shortcut
        inf_sheet_cols = self.dfs_dict[InfusatesLoader.DataSheetName]

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            infusate_name = loader.get_row_val(row, loader.headers.INFUSATE)
            if (
                infusate_name is not None
                and infusate_name not in seen["infusates"].keys()
                and infusate_name
                not in inf_sheet_cols[InfusatesLoader.DataHeaders.NAME].values()
            ):
                inf_data = parse_infusate_name_with_concs(infusate_name)
                self.extract_autofill_from_infusate_data(
                    inf_data,
                    infusate_name,
                    seen,
                )
                self.next_infusate_row_group_num += 1

            treatment_name = loader.get_row_val(row, loader.headers.TREATMENT)
            if (
                treatment_name is not None
                and treatment_name not in seen["treatments"].keys()
            ):
                self.autofill_dict[ProtocolsLoader.DataSheetName][treatment_name] = {
                    ProtocolsLoader.DataHeadersExcel.NAME: treatment_name
                }
                seen["treatments"][treatment_name] = True

            studies_str = loader.get_row_val(row, loader.headers.STUDY)
            if studies_str is not None:
                for study_str in studies_str.split(loader.StudyDelimiter):
                    study_name = study_str.strip()
                    if (
                        study_name is not None
                        and study_name != ""
                        and study_name not in seen["studies"].keys()
                    ):
                        self.autofill_dict[StudiesLoader.DataSheetName][study_name] = {
                            StudiesLoader.DataHeaders.NAME: study_name
                        }
                        seen["studies"][study_name] = True

    def extract_autofill_from_infusate_data(
        self,
        inf_data,
        infusate_name,
        seen,
    ):
        # Convenience shortcut
        trcr_sheet_cols = self.dfs_dict[TracersLoader.DataSheetName]

        for inf_trcr in inf_data["tracers"]:
            tracer_name = inf_trcr["tracer"]["unparsed_string"]
            conc = inf_trcr["concentration"]
            inf_unique_key = self.row_key_delim.join(
                [
                    str(val)
                    for val in [
                        # The order of these values is based on the first element in the InfusatesLoader's
                        # DataColumnUniqueConstraints, which is important for not duplicating data upon autofill
                        self.next_infusate_row_group_num,
                        tracer_name,
                        conc,
                        infusate_name,
                    ]
                ]
            )
            self.autofill_dict[InfusatesLoader.DataSheetName][inf_unique_key] = {
                InfusatesLoader.DataHeaders.NAME: infusate_name,
                InfusatesLoader.DataHeaders.ID: self.next_infusate_row_group_num,
                InfusatesLoader.DataHeaders.TRACERGROUP: inf_data["infusate_name"],
                InfusatesLoader.DataHeaders.TRACERNAME: tracer_name,
                InfusatesLoader.DataHeaders.TRACERCONC: conc,
            }

            # Now we can autofill the tracers sheet
            if (
                tracer_name is not None
                and tracer_name not in seen["tracers"].keys()
                and tracer_name
                not in trcr_sheet_cols[TracersLoader.DataHeaders.NAME].values()
            ):
                trcr_data = inf_trcr["tracer"]
                self.extract_autofill_from_tracer_data(trcr_data, seen)
                self.next_tracer_row_group_num += 1

        seen["infusates"][infusate_name] = True

    def extract_autofill_from_tracer_data(self, trcr_data, seen):
        tracer_name = trcr_data["unparsed_string"]
        # TODO: Get official/primary compound name from this string
        compound_name = self.get_existing_compound_primary_name(
            trcr_data["compound_name"]
        )
        for label_data in trcr_data["isotopes"]:
            element = label_data["element"]
            mass_number = label_data["mass_number"]
            count = label_data["count"]
            poss_list = label_data["positions"]
            poss_str = None
            if poss_list is not None and len(poss_list) > 0:
                poss_str = TracersLoader.POSITIONS_DELIMITER.join(
                    [str(pos) for pos in sorted(poss_list)]
                )
            unique_key = self.row_key_delim.join(
                [
                    str(val)
                    for val in [
                        # The order of these values is based on the first element in the TracersLoader's
                        # DataColumnUniqueConstraints, which is important for not duplicating data upon
                        # autofill
                        self.next_tracer_row_group_num,
                        tracer_name,
                        compound_name,
                        element,
                        mass_number,
                        count,
                        poss_str,
                    ]
                ]
            )
            self.autofill_dict[TracersLoader.DataSheetName][unique_key] = {
                TracersLoader.DataHeaders.ID: self.next_tracer_row_group_num,
                TracersLoader.DataHeaders.NAME: tracer_name,
                TracersLoader.DataHeaders.COMPOUND: compound_name,
                TracersLoader.DataHeaders.ELEMENT: element,
                TracersLoader.DataHeaders.MASSNUMBER: mass_number,
                TracersLoader.DataHeaders.LABELCOUNT: count,
                TracersLoader.DataHeaders.LABELPOSITIONS: poss_str,
            }

        seen["tracers"][tracer_name] = True

        self.extract_autofill_from_compound_name(compound_name, seen)

    def extract_autofill_from_compound_name(self, compound_str, seen):
        if compound_str is not None and compound_str not in seen["compounds"].keys():
            compound_name = self.get_existing_compound_primary_name(compound_str)
            self.autofill_dict[CompoundsLoader.DataSheetName][compound_name] = {
                CompoundsLoader.DataHeaders.NAME: compound_name
            }
            seen["compounds"][compound_name] = True

    def get_existing_compound_primary_name(self, compound_str):
        try:
            rec = Compound.compound_matching_name_or_synonym(compound_str)
            if rec is not None:
                return rec.name
        except (ValidationError, ObjectDoesNotExist):
            # This error will be raised upon the load attempt
            pass

        return compound_str

    def extract_autofill_from_samples_sheet(self):
        if SamplesLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = SamplesLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=SamplesLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = SamplesLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)
        seen = {
            "tissues": defaultdict(dict),
            "animals": defaultdict(dict),
        }

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            tissue_name = loader.get_row_val(row, loader.headers.TISSUE)
            if tissue_name is not None and tissue_name not in seen["tissues"].keys():
                self.autofill_dict[TissuesLoader.DataSheetName][tissue_name] = {
                    TissuesLoader.DataHeaders.NAME: tissue_name
                }
                seen["tissues"][tissue_name] = True

            animal_name = loader.get_row_val(row, loader.headers.ANIMAL)
            if animal_name is not None and animal_name not in seen["animals"].keys():
                self.autofill_dict[AnimalsLoader.DataSheetName][animal_name] = {
                    AnimalsLoader.DataHeaders.NAME: animal_name
                }
                seen["animals"][animal_name] = True

    def extract_autofill_from_peak_annot_files_sheet(self):
        if PeakAnnotationFilesLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = PeakAnnotationFilesLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=PeakAnnotationFilesLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = PeakAnnotationFilesLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)
        seen = {
            "sequences": defaultdict(dict),
            "lcprotocols": defaultdict(dict),
        }

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            annot_file = loader.get_row_val(row, loader.headers.FILE)
            seq_name = loader.get_row_val(row, loader.headers.SEQNAME)
            (
                operator,
                protocol,
                instrument,
                date,
            ) = self.extract_autofill_from_sequence_name(seq_name, seen)

            if annot_file is not None:
                annot_name = (os.path.split(annot_file))[1]

                if annot_name not in self.annot_file_metadata.keys():
                    self.annot_file_metadata = {
                        "operator": None,
                        "protocol": None,
                        "instrument": None,
                        "date": None,
                    }

                # The values in self.annot_file_metadata[annot_name] already are from the web form.  Those values trump
                # what we might find in the file already, so we only update with what we find in the file if it's not
                # set by the web form already.
                if (
                    self.annot_file_metadata[annot_name].get("operator") is None
                    or self.annot_file_metadata[annot_name].get("operator") == ""
                ):
                    self.annot_file_metadata[annot_name]["operator"] = operator

                if (
                    self.annot_file_metadata[annot_name].get("protocol") is None
                    or self.annot_file_metadata[annot_name].get("protocol") == ""
                ):
                    self.annot_file_metadata[annot_name]["protocol"] = protocol

                if (
                    self.annot_file_metadata[annot_name].get("instrument") is None
                    or self.annot_file_metadata[annot_name].get("instrument") == ""
                ):
                    self.annot_file_metadata[annot_name]["instrument"] = instrument

                if (
                    self.annot_file_metadata[annot_name].get("date") is None
                    or self.annot_file_metadata[annot_name].get("date") == ""
                ):
                    self.annot_file_metadata[annot_name]["date"] = date

    def extract_autofill_from_peak_annot_details_sheet(self):
        if MSRunsLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = MSRunsLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=MSRunsLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = MSRunsLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)

        # We're going to ignore the sample column.  It's way more likely it will have been auto-filled itself, and the
        # samples sheet is populated at the same time, so doing that here is just wasted cycles.  Instead, we're looking
        # for manually filled-in data to autofill elsewhere.
        seen = {
            "sequences": defaultdict(dict),
            "lcprotocols": defaultdict(dict),
        }

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            seq_name = loader.get_row_val(row, loader.headers.SEQNAME)
            self.extract_autofill_from_sequence_name(seq_name, seen)

            # We can actually fill in any missing seqnames in this sheet based on the file (and sample header)
            # columns...
            sample_header = loader.get_row_val(row, loader.headers.SAMPLEHEADER)
            file = loader.get_row_val(row, loader.headers.ANNOTNAME)
            if file in self.none_vals or sample_header in self.none_vals:
                continue
            filename = os.path.basename(file)

            # If there's no seqname for this row, but we have a complete seqname in self.annot_file_metadata
            # We have enough for autofill
            if seq_name in self.none_vals and (
                filename in self.annot_file_metadata.keys()
                and None not in self.annot_file_metadata[filename].values()
                and "" not in self.annot_file_metadata[filename].values()
            ):
                # Get the unique key used to identify this row
                unique_annot_deets_key = self.row_key_delim.join(
                    [sample_header, filename]
                )

                # If the autofill_dict hasn't been filled in already (i.e. don't overwrite what the user entered in the
                # web form)
                if (
                    unique_annot_deets_key
                    not in self.autofill_dict[MSRunsLoader.DataSheetName].keys()
                    or self.autofill_dict[MSRunsLoader.DataSheetName][
                        unique_annot_deets_key
                    ].get(MSRunsLoader.DataHeaders.SEQNAME)
                    is None
                ):
                    self.autofill_dict[MSRunsLoader.DataSheetName][
                        unique_annot_deets_key
                    ][
                        MSRunsLoader.DataHeaders.SEQNAME
                    ] = MSRunSequence.create_sequence_name(
                        **self.annot_file_metadata[filename]
                    )

    def extract_autofill_from_sequence_name(self, seq_name, seen):
        if seq_name is not None and seq_name not in seen["sequences"].keys():
            tpl = MSRunSequence.parse_sequence_name(seq_name)
            (
                operator,
                lc_protocol_name,
                instrument,
                date,
            ) = tpl
            self.autofill_dict[SequencesLoader.DataSheetName][seq_name] = {
                SequencesLoader.DataHeaders.SEQNAME: seq_name,
                SequencesLoader.DataHeaders.OPERATOR: operator,
                SequencesLoader.DataHeaders.LCNAME: lc_protocol_name,
                SequencesLoader.DataHeaders.INSTRUMENT: instrument,
                SequencesLoader.DataHeaders.DATE: date,
            }
            seen["sequences"][seq_name] = True

            self.extract_autofill_from_lcprotocol_name(lc_protocol_name, seen)

            return tpl

        return (None, None, None, None)

    def extract_autofill_from_lcprotocol_name(self, lc_protocol_name, seen):
        if (
            lc_protocol_name is not None
            and lc_protocol_name not in seen["lcprotocols"].keys()
        ):
            typ, runlen = LCMethod.parse_lc_protocol_name(lc_protocol_name)
            self.autofill_dict[LCProtocolsLoader.DataSheetName][lc_protocol_name] = {
                LCProtocolsLoader.DataHeaders.NAME: lc_protocol_name,
                LCProtocolsLoader.DataHeaders.TYPE: typ,
                LCProtocolsLoader.DataHeaders.RUNLEN: runlen,
            }
            seen["lcprotocols"][lc_protocol_name] = True

    def extract_autofill_from_infusates_sheet(self):
        if InfusatesLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = InfusatesLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=InfusatesLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = InfusatesLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)

        # Convenience shortcut
        trcr_sheet_cols = self.dfs_dict[TracersLoader.DataSheetName]

        seen = {
            "tracers": defaultdict(dict),
            "compounds": defaultdict(dict),
        }

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            tracer_name = loader.get_row_val(row, loader.headers.TRACERNAME)
            if (
                tracer_name is not None
                and tracer_name not in seen["tracers"].keys()
                and tracer_name
                not in trcr_sheet_cols[TracersLoader.DataHeaders.NAME].values()
            ):
                trcr_data = parse_tracer_string(tracer_name)
                self.extract_autofill_from_tracer_data(trcr_data, seen)
                self.next_tracer_row_group_num += 1

    def extract_autofill_from_tracers_sheet(self):
        if TracersLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = TracersLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=TracersLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = TracersLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)

        seen = {"compounds": defaultdict(dict)}

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            compound_name = loader.get_row_val(row, loader.headers.COMPOUND)
            self.extract_autofill_from_compound_name(compound_name, seen)

    def extract_autofill_from_sequences_sheet(self):
        if SequencesLoader.DataSheetName not in self.study_file_sheets:
            return

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet
        # how to address a type error from pandas when it encounters empty cells.  I have a comment in other
        # code that says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think
        # about it.  But not setting types works for now.  Setting optional_mode=True explicitly sets str types
        # in order to avoid accidental int(/etc) types...
        # 11/1/2024 - I figured out a good part of this to solve a different problem, but I still need to make sure this
        # solution is comprehensive before I refactor this code (more)
        dtypes, aes = SequencesLoader._get_column_types(optional_mode=True)

        df = self.read_from_file(
            self.study_file,
            sheet=SequencesLoader.DataSheetName,
            dtype=dtypes,
        )

        loader = SequencesLoader(
            df=df,
            file=self.study_file,
            filename=self.study_filename,
        )
        loader.aggregated_errors_object.merge_aggregated_errors_object(aes)

        seen = {"lcprotocols": defaultdict(dict)}

        for _, row in loader.df.iterrows():
            if loader.is_row_empty(row):
                continue

            lc_protocol_name = loader.get_row_val(row, loader.headers.LCNAME)
            self.extract_autofill_from_lcprotocol_name(lc_protocol_name, seen)

    def extract_autofill_from_sequence_forms(self):
        """Extracts sequence data from multiple peak annotation file and sequence forms.

        Populates self.autofill_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        for seq_data in self.annot_file_metadata.values():
            # This unique key is based on SequencesLoader.DataUniqueColumnConstraints
            seqname = MSRunSequence.create_sequence_name(**seq_data)
            # Note, this intentionally does not look for subsets of data, e.g. is N rows only have the operator (all the
            # same) and M rows have the same operator and same instrument, the result is 2 rows in the sequences sheet.
            # This assumes that the user has filled out as much information as they can in each case, and having those
            # rows represented in the Sequences sheet will prevent them from having to duplicate them.
            self.autofill_dict[SequencesLoader.DataSheetName][seqname] = {
                SequencesLoader.DataHeaders.OPERATOR: seq_data["operator"],
                SequencesLoader.DataHeaders.LCNAME: seq_data["protocol"],
                SequencesLoader.DataHeaders.INSTRUMENT: seq_data["instrument"],
                SequencesLoader.DataHeaders.DATE: seq_data["date"],
            }

    def extract_autofill_from_peak_annotation_files_forms(self):
        """Extracts data from multiple peak annotation files and the accompanying form sequence metadata that can be
        used to populate a made-from-scratch study doc.

        Populates self.autofill_dict.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # Example: allreps_dict[compounds_str][sample][filename][sample_header] = 0
        allreps_dict = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        )
        all_samples = {}
        # This checks to see if there are multiple scans of a sample in 1 file
        sample_dupes_exist = False
        for i in range(len(self.peak_annot_files)):
            peak_annot_filename = self.peak_annot_filenames[i]
            seq_data: dict = self.annot_file_metadata.get(peak_annot_filename, {})
            partial_seqname = None
            if len([v for v in seq_data.values() if v not in self.none_vals]) > 0:
                partial_seqname = MSRunSequence.create_sequence_name(**seq_data)
            complete_seqname = None
            if "" not in seq_data.values() and None not in seq_data.values():
                complete_seqname = partial_seqname

            # If the file format could not be determined, the loader will be None
            if self.peak_annotations_loaders[i] is None:
                print(f"SKIPPING UNKNOWN FORMAT FILE: {peak_annot_filename}")
                continue

            peak_annot_loader: PeakAnnotationsLoader = self.peak_annotations_loaders[i]

            self.autofill_dict[PeakAnnotationFilesLoader.DataSheetName][
                peak_annot_filename
            ] = {
                PeakAnnotationFilesLoader.DataHeaders.FILE: peak_annot_filename,
                # The format was assigned either by the user in the file or automatically determined.  Doesn't matter
                # how we got it, it's now a candidate for autfill.
                PeakAnnotationFilesLoader.DataHeaders.FORMAT: (
                    peak_annot_loader.format_code
                    if peak_annot_filename not in self.files_with_format_errors
                    else None
                ),
                # Always fill in what we have of the seqname
                PeakAnnotationFilesLoader.DataHeaders.SEQNAME: partial_seqname,
            }

            # Extracting samples - using vectorized access of the dataframe because it's faster and we don't need error
            # tracking per line of the input file.
            print("EXTRACTING SAMPLES")
            sample_headers = peak_annot_loader.df[
                peak_annot_loader.headers.SAMPLEHEADER
            ].unique()
            samples = defaultdict(lambda: defaultdict(int))
            for sample_header in sample_headers:
                if str(sample_header) in self.none_vals:
                    continue

                sample_name = MSRunsLoader.guess_sample_name(sample_header)

                # TODO: This should use parsing of the Peak Annotation Details sheet to decide on blanks/skips
                # instead of only guessing
                skip = None
                if Sample.is_a_blank(sample_name) is True:
                    # Automatically skip samples that appear to be blanks
                    # Leave as None if False - It will make the column easier for the user to read
                    skip = MSRunsLoader.SKIP_STRINGS[0]

                if skip is None:
                    print(f"FOUND SAMPLE {sample_name}")
                    self.autofill_dict[SamplesLoader.DataSheetName][sample_name] = {
                        SamplesLoader.DataHeaders.SAMPLE: sample_name
                    }

                    if (
                        sample_name in samples.keys()
                        and sample_header not in samples[sample_name].keys()
                    ):
                        sample_dupes_exist = True

                    # samples holds the samples that we will use to look for multiple representations, but since skipped
                    # samples will not be loaded, there's no reason to pick a file from which to load them
                    samples[sample_name][sample_header] = 0
                    all_samples[sample_name] = 0

                # This unique key is based on MSRunsLoader.DataUniqueColumnConstraints
                unique_annot_deets_key = self.row_key_delim.join(
                    [sample_header, peak_annot_filename]
                )
                self.autofill_dict[MSRunsLoader.DataSheetName][
                    unique_annot_deets_key
                ] = {
                    MSRunsLoader.DataHeaders.SAMPLEHEADER: sample_header,
                    MSRunsLoader.DataHeaders.SAMPLENAME: sample_name,
                    # No point in entering the mzXML. It will default to this in its loader:
                    # MSRunsLoader.DataHeaders.MZXMLNAME: f"{sample_header}.mzXML",
                    MSRunsLoader.DataHeaders.ANNOTNAME: peak_annot_filename,
                    # This only fills in the seqname if it is complete, so it can be autofilled if the researcher fills
                    # in the rest of the Peak Annotation Files sheet's seqname values
                    MSRunsLoader.DataHeaders.SEQNAME: complete_seqname,
                    MSRunsLoader.DataHeaders.SKIP: skip,
                }

            # Extracting compounds - using vectorized access of the dataframe because it's faster and we don't need
            # error tracking per line of the input file.
            print("EXTRACTING COMPOUNDS")
            pgname_strs = [
                peak_annot_loader.fix_elmaven_compound(c)
                for c in peak_annot_loader.df[
                    peak_annot_loader.headers.COMPOUND
                ].to_list()
            ]
            if peak_annot_loader.headers.FORMULA in peak_annot_loader.df.columns:
                formulas = peak_annot_loader.df[
                    peak_annot_loader.headers.FORMULA
                ].to_list()
            else:
                formulas = [None] * len(pgname_strs)
            seen = {}
            pgnames = []
            for index in range(len(pgname_strs)):
                formula = formulas[index]
                compound_synonyms_str = str(pgname_strs[index]).strip()

                # If we've seen this combo before, continue
                seen_key = f"{compound_synonyms_str},{str(formula)}"
                if seen_key in seen.keys():
                    continue
                seen[seen_key] = 0

                # We don't need all of the exception information that would come from missing required values.  We only
                # want the missing compound information, so continue if the compound is missing.  (We don't care if the
                # formula is None, because formulas are not unique in the compound model.)
                if compound_synonyms_str in self.none_vals:
                    continue

                pgname = PeakGroup.compound_synonyms_to_peak_group_name(
                    [
                        s.strip().lower()
                        for s in compound_synonyms_str.split(PeakGroup.NAME_DELIM)
                    ]
                )
                if pgname not in pgnames:
                    pgnames.append(pgname)
                    print(f"RECORDING REPRESENTATIONS FOR {pgname}")
                    # For every sample from the file
                    for sample in samples.keys():
                        # For every sample header (that matches the sample after having extracted the scan labels)
                        for sample_header in samples[sample].keys():
                            allreps_dict[pgname][sample][peak_annot_filename][
                                sample_header
                            ] = 0

                # Get compounds - Note that the result of the second arg (list of compounds), will buffer errors in the
                # peak_annot_loader if any compounds are not in the database.  We don't need that here, because we're
                # not validating, so we supply buffer_error=False
                compound_recs_dict = peak_annot_loader.get_peak_group_compounds_dict(
                    names_str=compound_synonyms_str, buffer_errors=False
                )

                # Report compounds by primary name (if found) or by the provided synonym (if not found).
                # And populate the autofill_dict.
                for compound_synonym, compound_rec in compound_recs_dict.items():
                    if compound_rec is not None:
                        # We could override the formula here with what we get from the database, but the job of this
                        # method is to represent what's in the file.  If there's a discrepancy, it should be handled
                        # in the load.
                        compound_name = compound_rec.name
                    else:
                        compound_name = compound_synonym.strip()

                    print(f"FOUND COMPOUND {compound_name} {formula}")

                    # If there are any discrepancies with the database, they will be caught by the load attempt.
                    self.autofill_dict[CompoundsLoader.DataSheetName][compound_name] = {
                        CompoundsLoader.DataHeaders.NAME: compound_name,
                        CompoundsLoader.DataHeaders.FORMULA: formula,
                    }

            if peak_annot_loader.headers.FORMULA in peak_annot_loader.df.columns:
                print("EXTRACTING AND INCLUDING ALL COMPOUNDS WITH MATCHING FORMULAS")
                for formula in np.unique(formulas):
                    if formula in self.none_vals:
                        continue
                    for compound_rec in Compound.objects.filter(
                        formula__iexact=formula
                    ):
                        if (
                            compound_rec.name
                            not in self.autofill_dict[
                                CompoundsLoader.DataSheetName
                            ].keys()
                        ):
                            print(
                                f"ADDING COMPOUND (BY FORMULA) {compound_rec.name} {compound_rec.formula}"
                            )
                            self.autofill_dict[CompoundsLoader.DataSheetName][
                                compound_rec.name
                            ] = {
                                CompoundsLoader.DataHeaders.NAME: compound_rec.name,
                                CompoundsLoader.DataHeaders.FORMULA: compound_rec.formula,
                            }

        db_samples_exist = (
            Sample.objects.filter(name__in=list(all_samples.keys())).count() > 0
        )
        multiple_files = len(self.peak_annot_files) > 1

        # We don't need to look for multiple representations if there's only 1 file, no samples pre-exist in the DB, and
        # there are not duplicate columns (with different scan labels) in the file
        if multiple_files or db_samples_exist or sample_dupes_exist:

            # Initialize an intermediate dict to construct sets of files that have common samples
            # Example: filesets_dict[compounds_str][filenames_str][sample][sample_header] = 0
            filesets_dict = defaultdict(
                lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
            )

            print(
                "PREPARING EXISTING PEAK GROUPS FOR THE MULTIPLE REPRESENTATIONS SEARCH"
            )

            # Now let's look for existing potential conflicts already in the DB (to add to allreps_dict
            # before processing it)
            for pgrec in PeakGroup.objects.filter(
                msrun_sample__sample__name__in=list(all_samples.keys())
            ):
                pgname = pgrec.name.lower()
                # If the compound is one we're loading and either the sample didn't have this compound in the files or
                # this is an unseen file, record the representation.
                if pgname in allreps_dict.keys() and (
                    pgrec.msrun_sample.sample.name not in allreps_dict[pgname].keys()
                    or pgrec.peak_annotation_file.filename
                    not in allreps_dict[pgname][pgrec.msrun_sample.sample.name].keys()
                ):
                    # NOTE: We cannot know the original sample header since it is never saved in the DB
                    allreps_dict[pgname][pgrec.msrun_sample.sample.name][
                        pgrec.peak_annotation_file.filename
                    ]["unknown"] = 0

            print("LOOKING FOR MULTIPLE REPRESENTATIONS")

            # Populate filesets_dict so that we can determine all the samples they have in common.  This basically (for
            # each common compound) takes every grouping of files for each sample and creates a list of common samples
            # for that file grouping.  We will then use that compound and that list of common samples to be able to be
            # used as a unique constraint in the sheet and then, when the file is used for loading, we will use the
            # compound, samples, and the selected file to know which other files with that compound and sample to skip.
            # For every compound (...sorted synonym set) from the file
            for pgname in allreps_dict.keys():
                # For every sample from the file
                for sample in allreps_dict[pgname].keys():
                    # If this sample is in multiple peak annotation files
                    if len(allreps_dict[pgname][sample].keys()) > 1:
                        file_set_key = ";".join(
                            sorted(allreps_dict[pgname][sample].keys())
                        )
                        for sample_header in allreps_dict[pgname][sample].keys():
                            filesets_dict[pgname][file_set_key][sample][
                                sample_header
                            ] = 0

            print("POPULATING MULTIPLE REPRESENTATIONS AUTOFILL")

            # Now that we have the file sets for files containing measurements for common samples, we can populate the
            # autofill for the Peak Group Conflicts sheet
            for pgname in sorted(filesets_dict.keys()):
                for file_set_key in filesets_dict[pgname].keys():
                    files_list = list(file_set_key.split(";"))
                    samples = sorted(
                        filesets_dict[pgname][file_set_key].keys(), key=str.casefold
                    )
                    samples_str = ";".join(samples)
                    unique_row_key = f"{pgname}:{samples_str}"
                    example = samples[0]
                    print(f"MULTREP: {pgname} IN {file_set_key} HAS {example}")
                    self.autofill_dict[PeakGroupConflicts.DataSheetName][
                        unique_row_key
                    ] = {
                        PeakGroupConflicts.DataHeaders.PEAKGROUP: pgname,
                        PeakGroupConflicts.DataHeaders.ANNOTFILE: {
                            "validate": "list",
                            "source": files_list,
                            "show_error": False,
                            "dropdown": True,
                        },
                        PeakGroupConflicts.DataHeaders.SAMPLECOUNT: len(samples),
                        PeakGroupConflicts.DataHeaders.EXAMPLE: example,
                        PeakGroupConflicts.DataHeaders.SAMPLES: samples_str,
                    }

    def extract_autofill_from_exceptions(self, retain_as_warnings=True):
        """Remove exceptions related to references to missing underlying data and extract their data.

        This produces a dict named autofill_dict keyed on sheet name.

        This method detects:
        - AllMissingSamples
        - AllMissingTissues
        - AllMissingTreatments
        - AllMissingCompounds
        - AllMissingStudies
        - NoSamples

        Args:
            retain_as_warnings (boolean): Track extracted error and warning exceptions as warnings.  If False, no pre-
                existing errors or warnings will be reported.
        Exceptions:
            Buffers:
                MissingDataAdded
            Raises:
                None
        Returns:
            None
        """
        self.extracted_exceptions = defaultdict(lambda: {"errors": [], "warnings": []})
        # Init the autofill dict for the subsequent calls
        self.autofill_dict = defaultdict(lambda: defaultdict(dict))
        self.autofill_dict[StudiesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[SamplesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TissuesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[ProtocolsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[MSRunsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[CompoundsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[AnimalsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[TracersLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[InfusatesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[LCProtocolsLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[SequencesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[PeakAnnotationFilesLoader.DataSheetName] = defaultdict(dict)
        self.autofill_dict[PeakGroupConflicts.DataSheetName] = defaultdict(dict)

        # For every AggregatedErrors objects associated with a file or category
        for load_key in [
            k
            for k, v in self.load_status_data.statuses.items()
            if v["aggregated_errors"] is not None
        ]:
            # For each exception class we want to extract from the AggregatedErrors object (in order to both "fix" the
            # data and to remove related errors that those fixes address)
            for exc_class, sheet, header, message in [
                (
                    AllMissingCompounds,
                    CompoundsLoader.DataSheetName,
                    CompoundsLoader.DataHeaders.NAME,
                    f"FIXED: Missing compounds have been added to the {CompoundsLoader.DataSheetName} sheet",
                ),
                (
                    AllMissingSamples,
                    SamplesLoader.DataSheetName,
                    SamplesLoader.DataHeaders.SAMPLE,
                    f"FIXED: Missing samples have been added to the {SamplesLoader.DataSheetName} sheet",
                ),
                (
                    AllMissingStudies,
                    StudiesLoader.DataSheetName,
                    StudiesLoader.DataHeaders.NAME,
                    f"FIXED: Missing studies have been added to the {StudiesLoader.DataSheetName} sheet",
                ),
                (
                    AllMissingTissues,
                    TissuesLoader.DataSheetName,
                    TissuesLoader.DataHeaders.NAME,
                    f"FIXED: Missing tissues have been added to the {TissuesLoader.DataSheetName} sheet",
                ),
                (
                    AllMissingTreatments,
                    ProtocolsLoader.DataSheetName,
                    ProtocolsLoader.DataHeadersExcel.NAME,
                    f"FIXED: Missing treatments have been added to the {ProtocolsLoader.DataSheetName} sheet",
                ),
                (
                    NoSamples,
                    SamplesLoader.DataSheetName,
                    SamplesLoader.DataHeaders.SAMPLE,
                    f"FIXED: Missing samples have been added to the {SamplesLoader.DataSheetName} sheet",
                ),
            ]:

                # Modify exceptions of exc_class in the AggregatedErrors object because they are going to be fixed, and
                # the user will be confused if they see a missing Tissues warning associated with a file and then see a
                # passed status of "All tissues are in the database".
                for exc in self.load_status_data.get_exception_type(
                    load_key, exc_class
                ):
                    # Every matched exception will be made into a warning
                    if retain_as_warnings:
                        self.extracted_exceptions[exc_class.__name__][
                            "warnings"
                        ].append(exc)

                    # A missing value is only extracted and inserted into the autofill_dict if that row is not already
                    # present in the dfs_dict (which can happen if there was an error creating the record), so only set
                    # the status message as "fixed" if the autofill was at all populated
                    status_message = None
                    if self.extract_all_missing_values(exc, sheet, header):
                        status_message = message

                    # We will set it as a warning in any case because either it was autofilled or some other error
                    # prevented its creation and the missing sample error was the result of another error.  We draw
                    # attention to the original error by making this one a warning.
                    self.load_status_data.modify_exception(
                        load_key, exc, status_message=status_message, is_error=False
                    )

    def extract_all_missing_values(self, exc, sheet, header):
        """Extracts autofill data from supplied AllMissing* exception and puts it in self.autofill_dict.

        Example:
            self.autofill_dict = {
                "Samples": {unique_record_key: {header: sample_name}},  # Fills in entries here
                "Tissues": defaultdict(dict),
                "Treatments": defaultdict(dict),
            }
        Assumptions:
            1. `exc.search_terms` is a list of singular unique values because the database query that hit an error was
            searching using a single unique field.  Note that if some other query were performed (e.g. using a non-
            unique field or a combination of fields or the search term was a substring of a record's value for the
            field), then this will be wrong.  If there are code changes that cause that to happen, the construction of
            the exceptions will have to be refactored.
        Args:
            exc (MissingModelRecordsByFile): An exception object containing data about missing records from a model.
            sheet (str)
            header (str)
        Exceptions:
            None
        Returns:
            added (bool): Whether the missing value was added to the autofill dict.
        """
        added = False
        for unique_val in exc.search_terms:
            # Check that the dfs_dict doesn't already have a row for this value.  The dfs_dict is structured like this:
            # {sheet: {header1: {0: rowindex0_value, 1: rowindex1_value}}}
            # where you must check all values of the inner dict.  Note that records can be reported as missing even if
            # the rows are present (which is why this check is necessary) if the row for the record could not be loaded
            # due to an error.  E.g. A sample could not be created because the animal could not be created because the
            # infusate selected was a mismatch for the infusate in the infusates sheet.
            if (
                sheet not in self.dfs_dict.keys()
                or header not in self.dfs_dict[sheet].keys()
                or unique_val not in self.dfs_dict[sheet][header].values()
            ):
                added = True
                self.autofill_dict[sheet][unique_val] = {header: unique_val}

        return added

    def add_extracted_autofill_data(self):
        """Appends new rows from self.autofill_dict to self.dfs_dict.

        Populate the dfs_dict sheets (keys) with some basal data (e.g. tissues in the tissues sheet), then add data that
        was missing, as indicated via the exceptions generated from having tried to load the submitted accucor/isocorr
        (and/or existing study doc), to the template by appending rows.

        Args:
            None
        Exceptions:
            Buffers:
                MissingDataAdded
            Raises:
                None
        Returns:
            None
        """
        # NOTE: If add_autofill_data added data without looking at the existing data, it could inadvertently add data
        # that already exists in the file.  So, in order to see if the data already exists, we need to tell
        # add_autofill_data how to construct the unique key (via the 2nd arg in each of the calls below) to be able to
        # see if rows already exist or not.  We use a potentially composite key based on the unique column constraints
        # because there can exist the same individual value multiple times in a column and we do not want to prevent an
        # add if it adds a unique row).

        data_added = False

        loader: TableLoader
        for loader in [
            self.studies_loader,
            self.animals_loader,
            self.samples_loader,
            self.tissues_loader,
            self.treatments_loader,
            self.compounds_loader,
            self.infusates_loader,
            self.tracers_loader,
            self.msruns_loader,
            self.sequences_loader,
            self.lcprotocols_loader,
            self.peakannotfiles_loader,
            self.peakgroupconflicts_loader,
        ]:
            # This is a list of column headers in a sheet whose combination defines a unique row
            # We need it to find existing rows that match the composite key in the autofitt dict, so that we don't
            # accidentally add duplicate data
            unique_column_combo_list = loader.header_keys_to_names(
                # Specifically selected unique column constraint group - used to find existing rows
                # If any desired index happens to not be 0, this will need to be a loop variable
                loader.DataUniqueColumnConstraints[0]
            )

            if self.add_autofill_data(
                loader.DataSheetName,
                unique_column_combo_list,
            ):
                data_added = True

        if data_added and not self.autofill_only_mode:
            # Add a warning about added data
            added_warning = MissingDataAdded(file=self.output_study_filename)
            self.load_status_data.set_load_exception(
                added_warning,
                "Autofill Note",
                top=True,
                default_is_error=False,
                default_is_fatal=False,
            )

    def add_autofill_data(self, sheet, row_key_headers):
        """This method, given a sheet name (and called by add_extracted_autofill_data), adds the data from
        self.autofill_dict[sheet] to self.dfs_dict[sheet], starting at the first empty row.

        Note the structures of the source dicts involved:
        - self.autofill_dict[sheet] is structured like {unique_key_str: {header1: value1, header2: value2}}
        - self.dfs_dict[sheet] is structured like {header1: {0: rowindex0_value, 1: rowindex1_value}}

        Assumptions:
            1. sheet is a key in both self.dfs_dict and self.autofill_dict.
            2. self.dfs_dict[sheet] and self.autofill_dict[sheet] each contain a dict.
            3. None of the data in self.autofill_dict[sheet] is already present on an existing row.
            4. The keys in the self.dfs_dict[sheet] dict are contiguous integers starting from 0 (which must be true at
            the time of implementation).
        Args:
            sheet (string): The name of the sheet, which is the first key in both the self.autofill_dict and
                self.dfs_dict.
            row_key_headers (List[str]): These are the headers whose values are used to construct the unique row keys.
                E.g. The unique row key for the Compounds sheet is "Name".  The value of that key from the autofill_dict
                is used to see if a row already exists in the dfs_dict where the autofill should occur.  If one doesn't
                exist, a new row is created.
        Exceptions:
            None
        Returns:
            None
        """
        # Get the first row index where we will be adding the first new row (we will be incrementing this)
        next_empty_index = self.get_next_row_index(sheet)
        current_last_index = next_empty_index - 1 if next_empty_index > 0 else 0
        data_added = False

        # Iterate over the records of new data
        for unique_row_key, sheet_dict in self.autofill_dict[sheet].items():

            # We're going to iterate over the headers present in the dfs_dict, but we need to keep track if any headers
            # in the sheet_dict are absent in the dfs_dict, so we can catch it up after the loop
            headers_present = dict((k, False) for k in sheet_dict.keys())

            # Check to see if this row already exists
            index = next_empty_index
            existing_index = self.get_existing_dfs_index(
                sheet, row_key_headers, unique_row_key
            )
            if existing_index is not None:
                index = existing_index

            # For the columns in the sheet (dfs_dict)
            for header in self.dfs_dict[sheet].keys():
                print(f"ADDING TO COLUMN {header}")
                # If the header is one we're adding data to
                if header in sheet_dict.keys():
                    print(f"LOOKING TO FILL IN {sheet_dict[header]} ON ROW {index}")
                    # Record that the header was found
                    headers_present[header] = True

                    # Only fill in the value if it doesn't already have a value.
                    # Custom-entered values from the user trump autofill.
                    if self.dfs_dict[sheet][header].get(index) is None:
                        # If the value from the autofill dict is a dict, e.g.:
                        # {
                        #     "validate": "list",
                        #     "source": [1, 2, 3],
                        #     "show_error": False,
                        #     "dropdown": True,
                        # }
                        # ...it has to be added later by xlsxwriter, so buffer it in self.validation_autofill_dict
                        if isinstance(sheet_dict[header], dict):
                            print(
                                f"FILLING IN VALIDATION FOR COLUMN {header} ON ROW {index}"
                            )
                            self.validation_autofill_dict[sheet][header][index] = (
                                sheet_dict[header]
                            )
                        else:
                            print(f"FILLING IN {sheet_dict[header]} ON ROW {index}")
                            # Add the new data
                            self.dfs_dict[sheet][header][index] = sheet_dict[header]
                            data_added = True
                    else:
                        print(
                            f"THE VALUE WAS NOT NONE: {self.dfs_dict[sheet][header].get(index)}, SO NOT OVERWRITING"
                        )

                else:
                    # Fill in the columns we're not adding data to with None
                    self.dfs_dict[sheet][header][index] = None

            # Now catch up any columns that were not present
            for missing_header in [
                h for h, present in headers_present.items() if not present
            ]:
                # Create the column
                self.dfs_dict[sheet][missing_header] = {}
                # Iterate over the missing rows and set them to None
                for missing_index in range(current_last_index):
                    self.dfs_dict[sheet][missing_header][missing_index] = None

                # Now set the new row value for the missing column
                if self.dfs_dict[sheet][header].get(index) is None:
                    self.dfs_dict[sheet][header][index] = sheet_dict[header]
                    data_added = True

            # Increment the new row number
            if existing_index is None:
                next_empty_index += 1

        return data_added

    def add_dynamic_dropdown_data(self):
        """This method adds data from the database that may or may not be needed for a given study, but is provided to
        make manual population of the sheets MUCH easier for the user.

        Example:
            In the case where we don't know the tracers, but we have the measured compounds (extracted from the peak
            annotations file, we can add any/all existing tracers that include those compounds, which in turn can be
            used to populate the infusates sheet, and in turn, populates the dropdowns for infusates in the Animals
            sheet.  This also makes it easier for the user to use the example tracer entries to add novel tracers.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        self.add_dynamic_dropdown_tracer_data()
        self.add_dynamic_dropdown_infusate_data()

    def add_dynamic_dropdown_tracer_data(self):
        """This uses the compound names in the Compounds sheet to query the database for tracers that include those
        compounds and populate the Tracers sheet with potentially useful data for the user.

        Assumptions:
            1. This assumes that the Tracer Name column is automatically filled in via excel formula.
        Limitations:
            1. This will not work with partially manually filled in tracer data - only with fully filled in rows.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if (
            CompoundsLoader.DataSheetName not in self.dfs_dict.keys()
            or TracersLoader.DataSheetName not in self.dfs_dict.keys()
        ):
            return

        compound_names = list(
            self.dfs_dict[CompoundsLoader.DataSheetName][
                CompoundsLoader.DataHeaders.NAME
            ].values()
        )

        recs_dict = dict(
            (rec._name(), rec)
            for rec in Tracer.objects.filter(compound__name__in=compound_names)
        )

        cols = self.dfs_dict[TracersLoader.DataSheetName]

        next_row_idx = self.get_next_row_index(TracersLoader.DataSheetName)

        tracer_rec: Tracer
        for name, tracer_rec in recs_dict.items():
            if name not in cols[TracersLoader.DataHeaders.NAME].values():
                for label_rec in tracer_rec.labels.all():
                    cols[TracersLoader.DataHeaders.ID][
                        next_row_idx
                    ] = self.next_tracer_row_group_num
                    cols[TracersLoader.DataHeaders.COMPOUND][
                        next_row_idx
                    ] = tracer_rec.compound.name
                    cols[TracersLoader.DataHeaders.MASSNUMBER][
                        next_row_idx
                    ] = label_rec.mass_number
                    cols[TracersLoader.DataHeaders.ELEMENT][
                        next_row_idx
                    ] = label_rec.element
                    cols[TracersLoader.DataHeaders.LABELCOUNT][
                        next_row_idx
                    ] = label_rec.count
                    if label_rec.positions is None:
                        cols[TracersLoader.DataHeaders.LABELPOSITIONS][
                            next_row_idx
                        ] = None
                    else:
                        poss_str = TracersLoader.POSITIONS_DELIMITER.join(
                            [str(ps) for ps in sorted(label_rec.positions)]
                        )
                        cols[TracersLoader.DataHeaders.LABELPOSITIONS][
                            next_row_idx
                        ] = poss_str
                    cols[TracersLoader.DataHeaders.NAME][
                        next_row_idx
                    ] = tracer_rec._name()
                    next_row_idx += 1
                self.next_tracer_row_group_num += 1

    def add_dynamic_dropdown_infusate_data(self):
        """This uses the tracer data in the Tracers sheet to query the database for infusates that include those
        tracers and populate the Infusates sheet with potentially useful data for the user.

        Assumptions:
            1. This assumes that the Infusate Name column is automatically filled in via excel formula.
        Limitations:
            1. This will not work with partially manually filled in infusate data - only with fully filled in rows.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if (
            TracersLoader.DataSheetName not in self.dfs_dict.keys()
            or InfusatesLoader.DataSheetName not in self.dfs_dict.keys()
        ):
            return

        # Get all the tracers from the tracers sheet (which is assumed to have already been populated by
        # add_dynamic_dropdown_tracer_data)
        tracer_names = dict(
            (name, 0)
            for name in list(
                self.dfs_dict[TracersLoader.DataSheetName][
                    TracersLoader.DataHeaders.NAME
                ].values()
            )
        )

        # Create a list of all the tracer record IDs present in the Tracers sheet
        tracer_ids = []

        for tn in tracer_names.keys():
            try:
                td = parse_tracer_string(tn)
            except IsotopeParsingError as ipe:
                self.load_status_data.set_load_exception(
                    ipe,
                    "Autofill Note",
                    top=False,
                    default_is_error=True,
                    default_is_fatal=True,
                )
                continue
            trcr = Tracer.objects.get_tracer(td)
            if trcr is not None:
                tracer_ids.append(trcr.pk)

        # Create a dict of infusates keyed on name
        recs_dict = dict(
            (itl_rec.infusate._name(), itl_rec.infusate)
            for itl_rec in InfusateTracer.objects.filter(tracer__id__in=tracer_ids)
            .order_by("infusate__id")
            .distinct("infusate__id")
        )

        # Convenience shortcut
        cols = self.dfs_dict[InfusatesLoader.DataSheetName]

        # Determine the index of the next empty row and the next infusate row group number
        next_row_idx = self.get_next_row_index(InfusatesLoader.DataSheetName)

        infusate_rec: Infusate
        for name, infusate_rec in recs_dict.items():

            if name not in cols[InfusatesLoader.DataHeaders.NAME].values():

                # Only add the current infusate if all its tracers are in the tracers sheet
                all_tracers_present = True
                for itl in infusate_rec.tracer_links.all():
                    if itl.tracer.id not in tracer_ids:
                        all_tracers_present = False
                        break

                if all_tracers_present:

                    for itl_rec in infusate_rec.tracer_links.all():
                        cols[InfusatesLoader.DataHeaders.ID][
                            next_row_idx
                        ] = self.next_infusate_row_group_num
                        cols[InfusatesLoader.DataHeaders.TRACERGROUP][
                            next_row_idx
                        ] = infusate_rec.tracer_group_name
                        cols[InfusatesLoader.DataHeaders.TRACERNAME][
                            next_row_idx
                        ] = itl_rec.tracer._name()
                        cols[InfusatesLoader.DataHeaders.TRACERCONC][
                            next_row_idx
                        ] = itl_rec.concentration
                        cols[InfusatesLoader.DataHeaders.NAME][
                            next_row_idx
                        ] = infusate_rec._name()

                        next_row_idx += 1

                    self.next_infusate_row_group_num += 1

    def fill_missing_from_db(self):
        """After missing data has been autofilled (extracted from both exceptions and/or from the input files, e.g. the
        user added a tissue in the samples sheet and it got added to the tissues sheet), this goes through the dfs_dict
        and uses the filled-in values on the row to search the DB for matching entire records, and fills in any columns
        that are empty.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        self.fill_missing_compound_data_from_db()

    def fill_missing_compound_data_from_db(self):
        """Traverse the rows of the compounds sheet in the self.dfs_dict and use the DB to fill in column values.

        This intentionally leaves potentially incorrect filled in values.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        if CompoundsLoader.DataSheetName not in self.dfs_dict.keys():
            return

        cols = self.dfs_dict[CompoundsLoader.DataSheetName]

        names = list(
            self.dfs_dict[CompoundsLoader.DataSheetName][
                CompoundsLoader.DataHeaders.NAME
            ].values()
        )

        recs_dict = dict(
            (rec.name, rec) for rec in Compound.objects.filter(name__in=names)
        )

        for rowidx in cols[CompoundsLoader.DataHeaders.NAME].keys():
            name = cols[CompoundsLoader.DataHeaders.NAME][rowidx]
            if name in recs_dict.keys():
                if cols[CompoundsLoader.DataHeaders.HMDB_ID][rowidx] is None:
                    cols[CompoundsLoader.DataHeaders.HMDB_ID][rowidx] = recs_dict[
                        name
                    ].hmdb_id
                if cols[CompoundsLoader.DataHeaders.FORMULA][rowidx] is None:
                    cols[CompoundsLoader.DataHeaders.FORMULA][rowidx] = recs_dict[
                        name
                    ].formula
                if cols[CompoundsLoader.DataHeaders.SYNONYMS][rowidx] is None:
                    syn_str = CompoundsLoader.SYNONYMS_DELIMITER.join(
                        sorted(
                            list(
                                recs_dict[name].synonyms.values_list("name", flat=True)
                            )
                        )
                    )
                    cols[CompoundsLoader.DataHeaders.SYNONYMS][rowidx] = syn_str

    def get_existing_dfs_index(self, sheet, row_key_headers, unique_row_key):
        """This determines if a row already exists in dfs_dict that matches the provided unique_row_key.

        Note, the row_key_headers are used on each row in the dfs_dict to construct its unique key and compare it to the
        provided unique_row_key.  If no matching row exists, None is returned.

        Args:
            sheet (str)
            row_key_headers (List[str])
            unique_row_key (str)
        Exceptions:
            None
        Returns:
            index (Optional[int])
        """
        first_empty_index = self.get_next_row_index(sheet)
        if first_empty_index is None:
            return None
        for index in range(first_empty_index):
            # This assumes that the row_key_headers all have values (since they are required keys)
            cur_row_key = self.row_key_delim.join(
                [
                    str(self.dfs_dict[sheet][hdr].get(index, ""))
                    for hdr in row_key_headers
                ]
            )
            if cur_row_key == unique_row_key:
                # This assumes that the value combo is unique
                return index
        return None

    def header_to_cell(self, sheet, header, letter_only=False):
        """Convert a sheet name and header string into the corresponding excel cell location, e.g. "A1".

        Args:
            sheet (string): Name of the target sheet
            header (string): Name of the column header
            letter_only (boolean): Whether to include the row number
        Exceptions:
            Raises:
                ValueError
            Buffers:
                None
        Returns:
            (string): Cell location or column letter
        """
        headers = []
        if sheet == StudiesLoader.DataSheetName:
            headers = self.studies_loader.get_ordered_display_headers()
        elif sheet == AnimalsLoader.DataSheetName:
            headers = self.animals_loader.get_ordered_display_headers()
        elif sheet == SamplesLoader.DataSheetName:
            headers = self.samples_loader.get_ordered_display_headers()
        elif sheet == ProtocolsLoader.DataSheetName:
            headers = self.treatments_loader.get_ordered_display_headers()
        elif sheet == TissuesLoader.DataSheetName:
            headers = self.tissues_loader.get_ordered_display_headers()
        elif sheet == CompoundsLoader.DataSheetName:
            headers = self.compounds_loader.get_ordered_display_headers()
        elif sheet == TracersLoader.DataSheetName:
            headers = self.tracers_loader.get_ordered_display_headers()
        elif sheet == InfusatesLoader.DataSheetName:
            headers = self.infusates_loader.get_ordered_display_headers()
        elif sheet == LCProtocolsLoader.DataSheetName:
            headers = self.lcprotocols_loader.get_ordered_display_headers()
        elif sheet == SequencesLoader.DataSheetName:
            headers = self.sequences_loader.get_ordered_display_headers()
        elif sheet == MSRunsLoader.DataSheetName:
            headers = self.msruns_loader.get_ordered_display_headers()
        elif sheet == PeakAnnotationFilesLoader.DataSheetName:
            headers = self.peakannotfiles_loader.get_ordered_display_headers()
        elif sheet == PeakGroupConflicts.DataSheetName:
            headers = self.peakgroupconflicts_loader.get_ordered_display_headers()
        else:
            raise ValueError(f"Invalid sheet: [{sheet}].")
        column_letter = xlsxwriter.utility.xl_col_to_name(headers.index(header))
        if letter_only:
            return column_letter
        return f"{column_letter}1"

    def get_next_row_index(self, sheet):
        """Retrieves the next row index from self.dfs_dict[sheet] (from the first arbitrary column).

        Assumptions:
            1. This assumes each column has the same number of rows
        Args:
            sheet (string): Name of the sheet to get the last row from
        Exceptions:
            None
        Returns:
            last_index (int): 0 if there are no rows, otherwise the max row index + 1.
        """
        for hdr in self.dfs_dict[sheet].keys():
            if len(self.dfs_dict[sheet][hdr].keys()) == 0:
                return 0
            return max(self.dfs_dict[sheet][hdr].keys()) + 1
        return 0

    def get_or_create_dfs_dict(self):
        """Get or create dataframes dict templates for each sheet in self.animal_sample_file as a dict keyed on sheet.

        Generate a dict for the returned study doc (based on either the study doc supplied or create a fresh one).

        Note that creation populates the dfs_dict sheets (keys) with some basal data (e.g. tissues in the tissues
        sheet), but the "get" method does not.  Any missing data will need to be added via exceptions.  The intent
        being, that we initially give users all the data.  If they remove any that's not relevant to their study, that's
        fine.  We only want to add data if another sheet references it and it's not there.  That will be handled outside
        of this method.

        Args:
            None
        Exceptions:
            None
        Returns:
            dict of dicts: dataframes-style dicts dict keyed on sheet name
        """
        if self.study_file is None:
            return self.create_or_repair_study_dfs_dict()
        dfs_dict = self.get_study_dfs_dict()
        # If there was a conversion issue, the dfs_dict will be empty
        if dfs_dict is None or len(dfs_dict.keys()) == 0:
            # There will have been an exception buffered, so let's just fall back to a default template for them to use
            return self.create_or_repair_study_dfs_dict()
        # This fills in missing sheets and columns
        return self.create_or_repair_study_dfs_dict(dfs_dict)

    def create_or_repair_study_dfs_dict(
        self, dfs_dict: Optional[Dict[str, dict]] = None
    ):
        """Create dataframe template dicts for each sheet in self.animal_sample_file as a dict keyed on sheet.

        Treatments and tissues dataframes are populated using all of the data in the database for their models.
        Animals and Samples dataframes are not populated.

        Limitations:
            1. Missing data is not attempted to be auto-filled by this method.
            2. Unrecognized sheets or columns are not removed.
        Args:
            dfs_dict (dict of dicts): Supply this if you want to "fill in" missing sheets only.
        Exceptions:
            None
        Returns:
            dfs_dict (dict of dicts): pandas' style list-dicts keyed on sheet name
        """
        if dfs_dict is None:
            # Setting sheet to None reads all sheets and returns a dict keyed on sheet name
            return {
                StudiesLoader.DataSheetName: self.studies_loader.get_dataframe_template(),
                AnimalsLoader.DataSheetName: self.animals_loader.get_dataframe_template(),
                SamplesLoader.DataSheetName: self.samples_loader.get_dataframe_template(),
                ProtocolsLoader.DataSheetName: self.treatments_loader.get_dataframe_template(
                    populate=True,
                    filter={"category": Protocol.ANIMAL_TREATMENT},
                ),
                TissuesLoader.DataSheetName: self.tissues_loader.get_dataframe_template(
                    populate=True
                ),
                CompoundsLoader.DataSheetName: self.compounds_loader.get_dataframe_template(),
                TracersLoader.DataSheetName: self.tracers_loader.get_dataframe_template(),
                InfusatesLoader.DataSheetName: self.infusates_loader.get_dataframe_template(),
                LCProtocolsLoader.DataSheetName: self.lcprotocols_loader.get_dataframe_template(
                    populate=True
                ),
                SequencesLoader.DataSheetName: self.sequences_loader.get_dataframe_template(),
                MSRunsLoader.DataSheetName: self.msruns_loader.get_dataframe_template(),
                PeakAnnotationFilesLoader.DataSheetName: self.peakannotfiles_loader.get_dataframe_template(),
                PeakGroupConflicts.DataSheetName: self.peakgroupconflicts_loader.get_dataframe_template(),
            }

        if (
            StudiesLoader.DataSheetName in dfs_dict.keys()
            and len(dfs_dict[StudiesLoader.DataSheetName].keys()) > 0
        ):
            self.fill_in_missing_columns(
                dfs_dict,
                StudiesLoader.DataSheetName,
                self.studies_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[StudiesLoader.DataSheetName] = (
                self.studies_loader.get_dataframe_template()
            )

        if (
            AnimalsLoader.DataSheetName in dfs_dict.keys()
            and len(dfs_dict[AnimalsLoader.DataSheetName].keys()) > 0
        ):
            self.fill_in_missing_columns(
                dfs_dict,
                AnimalsLoader.DataSheetName,
                self.animals_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[AnimalsLoader.DataSheetName] = (
                self.animals_loader.get_dataframe_template()
            )

        if (
            SamplesLoader.DataSheetName in dfs_dict.keys()
            and len(dfs_dict[SamplesLoader.DataSheetName].keys()) > 0
        ):
            self.fill_in_missing_columns(
                dfs_dict,
                SamplesLoader.DataSheetName,
                self.samples_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[SamplesLoader.DataSheetName] = (
                self.samples_loader.get_dataframe_template()
            )

        if ProtocolsLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                ProtocolsLoader.DataSheetName,
                self.treatments_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[ProtocolsLoader.DataSheetName] = (
                self.treatments_loader.get_dataframe_template(
                    populate=True,
                    filter={"category": Protocol.ANIMAL_TREATMENT},
                )
            )

        if TissuesLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                TissuesLoader.DataSheetName,
                self.tissues_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[TissuesLoader.DataSheetName] = (
                self.tissues_loader.get_dataframe_template(populate=True)
            )

        if CompoundsLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                CompoundsLoader.DataSheetName,
                self.compounds_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[CompoundsLoader.DataSheetName] = (
                self.compounds_loader.get_dataframe_template()
            )

        if TracersLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                TracersLoader.DataSheetName,
                self.tracers_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[TracersLoader.DataSheetName] = (
                self.tracers_loader.get_dataframe_template()
            )

        if InfusatesLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                InfusatesLoader.DataSheetName,
                self.infusates_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[InfusatesLoader.DataSheetName] = (
                self.infusates_loader.get_dataframe_template()
            )

        if LCProtocolsLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                LCProtocolsLoader.DataSheetName,
                self.lcprotocols_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[LCProtocolsLoader.DataSheetName] = (
                self.lcprotocols_loader.get_dataframe_template(populate=True)
            )

        if SequencesLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                SequencesLoader.DataSheetName,
                self.sequences_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[SequencesLoader.DataSheetName] = (
                self.sequences_loader.get_dataframe_template()
            )

        if MSRunsLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                MSRunsLoader.DataSheetName,
                self.msruns_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[MSRunsLoader.DataSheetName] = (
                self.msruns_loader.get_dataframe_template()
            )

        if PeakAnnotationFilesLoader.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                PeakAnnotationFilesLoader.DataSheetName,
                self.peakannotfiles_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[PeakAnnotationFilesLoader.DataSheetName] = (
                self.peakannotfiles_loader.get_dataframe_template()
            )

        if PeakGroupConflicts.DataSheetName in dfs_dict.keys():
            self.fill_in_missing_columns(
                dfs_dict,
                PeakGroupConflicts.DataSheetName,
                self.peakgroupconflicts_loader.get_dataframe_template(),
            )
        else:
            dfs_dict[PeakGroupConflicts.DataSheetName] = (
                self.peakgroupconflicts_loader.get_dataframe_template()
            )

        return dfs_dict

    @classmethod
    def fill_in_missing_columns(cls, dfs_dict, sheet, template):
        """Takes a dict of pandas-style dicts (dfs_dict), the sheet name (which is the key to the dict), and a template
        dict by which to check the completeness of the data in the dfs_dict, and modifies the dfs_dict to add
        missing columns (with the same number of rows as the existing data).

        Assumptions:
            1. The dfs_dict has at least 1 defined column.
        Args:
            dfs_dict (dict of dicts): This is a dict presumed to have been parsed from a file
            sheet (string): Name of the sheet key in the dfs_dict that the template corresponds to.  The sheet is
                assumed to be present as a key in dfs_dict.
            tamplate (dict of dicts): A dict containing all of the expected columns (the values are not used).
        Exceptions:
            None
        Returns:
            None
        """
        sheet_dict = dfs_dict[sheet]
        headers = list(sheet_dict.keys())
        first_present_header_key = headers[0]
        # Assumes all columns have the same number of rows
        num_rows = len(sheet_dict[first_present_header_key])
        modified = False
        for header in template.keys():
            if header not in sheet_dict.keys():
                modified = True
                sheet_dict[header] = dict((i, None) for i in range(num_rows))
        if modified:
            # No need to change anything if nothing was missing
            dfs_dict[sheet] = sheet_dict

    def get_study_dfs_dict(self):
        """Read in each sheet in self.study_file as a dict of dicts keyed on sheet (filling in any missing sheets and
        columns).

        Args:
            None
        Exceptions:
            None
        Returns:
            dfs_dict (dict of dicts): pandas-style dicts dict keyed on sheet name
        """
        load_status_data = MultiLoadStatus(load_keys=self.all_infile_names)

        # TODO: I would like to provide dtypes to manage the types we get back, but I have not figured out yet how
        # to address a type error from pandas when it encounters empty cells.  I have a comment in other code that
        # says that supplying keep_default_na=True fixes it, but that didn't work.  I have to think about it.  But
        # not setting types works for now.  I might need to explicitly set str in some places to avoid accidental
        # int types...
        dfd = self.read_from_file(
            self.study_file,
            sheet=None,
            dtype=self.get_study_dtypes_dict(),
        )

        dfs_dict = None

        try:
            # This creates the current version StudyLoader.
            loader_class: Type[StudyLoader] = StudyLoader.get_derived_class(dfd)
            sl: StudyLoader = loader_class(
                df=dfd,
                file=self.study_file,
                filename=self.study_filename,
                annot_files_dict=self.annot_files_dict,
            )
            dict_of_dataframes = sl.df_dict

            # We're not ready yet for actual dataframes.  It will be easier to move forward with dicts to be able to add
            # data
            dfs_dict = {}
            for k, v in dict_of_dataframes.items():
                dfs_dict[k] = v.to_dict()

            # create_or_repair_study_dfs_dict, if given a dict, will fill in any missing sheets and columns with empty
            # row values
            self.create_or_repair_study_dfs_dict(dfs_dict=dfs_dict)
        except (
            InvalidStudyDocVersion,
            UnknownStudyDocVersion,
            MultipleStudyDocVersions,
        ) as sve:
            load_status_data.set_load_exception(
                sve,
                StudyLoader.ConversionHeading,
                top=True,
            )
        except MultiLoadStatus as mls:
            load_status_data = mls

        self.load_status_data = load_status_data

        return dfs_dict

    def get_study_dtypes_dict(self):
        """Retrieve the dtype data for every sheet in one 2-dimensional dict (keyed on sheet and header).

        NOTE: The returned dict is not what the pandas' read methods take directly.  The returned 2D dict can only be
        used by read_from_file, which reads each sheet individually and retrieves each sheet's specific dtype dict from
        what is returned here.

        Args:
            None
        Exceptions:
            None
        Returns:
            dtypes (Dict[str, Dict[str, type]]): dtype dicts keyed by sheet name
        """
        ldr_types = {}
        ldr: TableLoader
        for ldr in [
            self.studies_loader,
            self.animals_loader,
            self.samples_loader,
            self.treatments_loader,
            self.tissues_loader,
            self.compounds_loader,
            self.tracers_loader,
            self.infusates_loader,
            self.lcprotocols_loader,
            self.sequences_loader,
            self.msruns_loader,
            self.peakannotfiles_loader,
            self.peakgroupconflicts_loader,
        ]:
            ldr_types[ldr.DataSheetName] = ldr.get_column_types(optional_mode=True)

        return ldr_types

    def format_results_for_template(self):
        """This populates:
        - self.valid
        - self.results
        - self.exceptions
        - self.ordered_keys
        based on the contents of self.load_status_data

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        results = {}
        exceptions = {}
        ordered_keys = []

        for load_key in self.load_status_data.get_ordered_status_keys():
            # The load_key is the absolute path, but we only want to report errors in the context of the file's name
            short_load_key = os.path.basename(load_key)

            ordered_keys.append(short_load_key)
            results[short_load_key] = self.load_status_data.statuses[load_key]["state"]

            exceptions[short_load_key] = []
            # Get the AggregatedErrors object
            aes = self.load_status_data.statuses[load_key]["aggregated_errors"]
            # aes is None if there were no exceptions
            aes: AggregatedErrors
            if aes is not None:
                for exc in aes.exceptions:
                    exc_str = aes.get_buffered_exception_summary_string(
                        exc, numbered=False, typed=False
                    )
                    exceptions[short_load_key].append(
                        {
                            "message": exc_str,
                            "is_error": exc.is_error,
                            "type": type(exc).__name__,
                            # TODO: The conditional was added as a precaution (to be safe/fast). Remove it and just
                            # assume aes_status_message is present, when I have time to check it.
                            "fixed": (
                                exc.aes_status_message
                                if hasattr(exc, "aes_status_message")
                                else None
                            ),
                        }
                    )

        self.valid = self.load_status_data.is_valid
        self.state = self.load_status_data.state
        self.results = results
        self.exceptions = exceptions
        self.ordered_keys = ordered_keys

    # No need to disable autoupdates adding the @no_autoupdates decorator to this function because supplying
    # `validate=True` automatically disables them
    def validate_study(self):
        load_status_data = MultiLoadStatus(load_keys=self.all_infile_names)

        try:
            # Generate the df_dict, accounting for the data types by supplying the dtype argument.  We will use
            # optional_mode to avoid pandas errors about types that do not allow empty values.
            df_dict = self.read_from_file(
                self.study_file, sheet=None, dtype=self.get_study_dtypes_dict()
            )

            loader_class = StudyLoader.get_derived_class(df_dict)
            sl: StudyLoader = loader_class(
                df=df_dict,
                file=self.study_file,
                filename=self.study_filename,
                _validate=True,
                annot_files_dict=self.annot_files_dict,
            )
            sl.load_data()
        except (
            InvalidStudyDocVersion,
            UnknownStudyDocVersion,
            MultipleStudyDocVersions,
        ) as sve:
            load_status_data.set_load_exception(
                sve,
                StudyLoader.ConversionHeading,
                top=True,
            )
        except MultiLoadStatus as mls:
            load_status_data = mls

        # Remove exceptions about missing peak annotation files when the peak annotation files were not submitted
        if len(self.annot_files_dict.keys()) == 0:
            load_status_data.remove_exception_type(
                self.study_filename, FileFromInputNotFound
            )

        self.load_status_data = load_status_data

        return self.load_status_data.is_valid

    def create_study_file_writer(self, stream_obj: BytesIO):
        """Returns an xlsxwriter for an excel file created from the self.dfs_dict.

        The created writer will output to the supplied stream_obj.

        Args:
            stream_obj (BytesIO)
        Exceptions:
            None
        Returns:
            xlsx_writer (xlsxwriter)
        """
        xlsx_writer = pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            stream_obj, engine="xlsxwriter"
        )

        # Create the cell format objects we will need
        workbook = xlsx_writer.book
        self.create_format_objects(workbook)

        for order_spec in StudyLoader.get_study_sheet_column_display_order():
            sheet = order_spec[0]
            columns = order_spec[1]

            if sheet not in self.build_sheets:
                # Skipping unsupported sheets
                continue

            # Error-check the ordered sheets/columns
            if sheet not in self.dfs_dict.keys():
                raise KeyError(
                    f"Sheet [{sheet}] from the StudyLoader not in self.dfs_dict: {self.dfs_dict.keys()}"
                )
            else:
                missing_headers = []
                for header in columns:
                    if header not in self.dfs_dict[sheet].keys():
                        missing_headers.append(header)
                if len(missing_headers) > 0:
                    KeyError(
                        f"The following headers for sheet [{sheet}] obtained from the StudyLoader are not in "
                        f"self.dfs_dict[{sheet}]: {missing_headers}"
                    )

            # Create a dataframe and add it as an excel object to an xlsx_writer sheet
            pd.DataFrame.from_dict(self.dfs_dict[sheet]).to_excel(
                excel_writer=xlsx_writer,
                sheet_name=sheet,
                columns=columns,
                index=False,
            )

            # Color the database cells
            worksheet: xlsxwriter.worksheet.Worksheet = xlsx_writer.sheets[sheet]
            column_metadata = self.loaders[sheet].get_value_metadata()
            for header in columns:
                cell_letter = self.header_to_cell(sheet, header, letter_only=True)
                if column_metadata[header].readonly:
                    # Format the entire column of readonly(/formula) columns
                    worksheet.set_column(
                        f"{cell_letter}:{cell_letter}",
                        None,
                        self.excel_formats["readonly"]["fmtn"],
                    )
                    # Exclude(/overwrite) the format of the header
                    worksheet.conditional_format(
                        f"{cell_letter}1:{cell_letter}1",
                        {
                            "type": "no_errors",
                            "format": self.excel_formats["readonly"]["fmth"],
                        },
                    )
                elif column_metadata[header].required:
                    # Format just the headers of required columns
                    worksheet.conditional_format(
                        f"{cell_letter}1:{cell_letter}1",
                        {
                            "type": "no_errors",
                            "format": self.excel_formats["required"]["fmth"],
                        },
                    )
                else:
                    # Format just the headers of optional columns
                    worksheet.conditional_format(
                        f"{cell_letter}1:{cell_letter}1",
                        {
                            "type": "no_errors",
                            "format": self.excel_formats["optional"]["fmth"],
                        },
                    )

        return xlsx_writer

    def create_format_objects(self, workbook):
        for fmt_key in self.excel_formats.keys():
            # Format for the header row
            self.excel_formats[fmt_key]["fmth"] = workbook.add_format(
                {**self.excel_formats[fmt_key]["dict"], **self.header_fmt}
            )
            # Format for the first row of data
            self.excel_formats[fmt_key]["fmt1"] = workbook.add_format(
                {**self.excel_formats[fmt_key]["dict"], **self.data_row1_fmt}
            )
            # Format for subsequent rows
            self.excel_formats[fmt_key]["fmtn"] = workbook.add_format(
                {**self.excel_formats[fmt_key]["dict"], **self.data_rown_fmt}
            )

    def dfs_dict_is_valid(self):
        """This determines whether self.dfs_dict is correctly structured (not populated), e.g. all required headers are
        present.

        Args:
            None
        Exceptions:
            None
        Returns:
            valid (boolean)
        """
        existing_sheets = [
            s[0]
            for s in StudyLoader.get_study_sheet_column_display_order()
            if s[0] in self.dfs_dict.keys()
        ]
        if self.dfs_dict is None or set(existing_sheets) < set(self.build_sheets):
            return False

        return (
            # Required headers present in each sheet
            StudiesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[StudiesLoader.DataSheetName])
            ).check_dataframe_headers()
            and SamplesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[SamplesLoader.DataSheetName])
            ).check_dataframe_headers()
            and AnimalsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[AnimalsLoader.DataSheetName])
            ).check_dataframe_headers()
            and TissuesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[TissuesLoader.DataSheetName])
            ).check_dataframe_headers()
            and ProtocolsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[ProtocolsLoader.DataSheetName]),
                headers=ProtocolsLoader.DataHeadersExcel,
            ).check_dataframe_headers()
            and CompoundsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[CompoundsLoader.DataSheetName])
            ).check_dataframe_headers()
            and TracersLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[TracersLoader.DataSheetName])
            ).check_dataframe_headers()
            and InfusatesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[InfusatesLoader.DataSheetName])
            ).check_dataframe_headers()
            and LCProtocolsLoader(
                df=pd.DataFrame.from_dict(
                    self.dfs_dict[LCProtocolsLoader.DataSheetName]
                )
            ).check_dataframe_headers()
            and SequencesLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[SequencesLoader.DataSheetName])
            ).check_dataframe_headers()
            and MSRunsLoader(
                df=pd.DataFrame.from_dict(self.dfs_dict[MSRunsLoader.DataSheetName])
            ).check_dataframe_headers()
            and PeakAnnotationFilesLoader(
                df=pd.DataFrame.from_dict(
                    self.dfs_dict[PeakAnnotationFilesLoader.DataSheetName]
                )
            ).check_dataframe_headers()
            and (
                # This is an optional sheet
                PeakGroupConflicts.DataSheetName not in self.dfs_dict.keys()
                or PeakGroupConflicts(
                    df=pd.DataFrame.from_dict(
                        self.dfs_dict[PeakGroupConflicts.DataSheetName]
                    )
                ).check_dataframe_headers()
            )
        )
