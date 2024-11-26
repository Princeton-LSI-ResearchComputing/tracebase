import argparse
from typing import Type

from django.core.management import CommandError

from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.study_loader import StudyLoader, StudyV3Loader
from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils.exceptions import (
    InvalidStudyDocVersion,
    MultipleStudyDocVersions,
    OptionsNotAvailable,
    UnknownStudyDocVersion,
)
from DataRepo.utils.file_utils import read_from_file


class Command(LoadTableCommand):
    """Command to load all sheets of an entire study doc.

    NOTE: loader_class takes a derived class of TableLoader named StudyLoader, but anywhere it refers to columns and
    headers, in this context, it's referring to sheets and tabs.
    """

    help = "Loads all data from a study doc (e.g. Animals, Samples, Compounds, etc) into the database."

    # This is the default loader_class version
    loader_class: Type[TableLoader] = StudyV3Loader

    def __init__(self, *args, **kwargs):
        # Don't require any options (i.e. don't require the --infile option)
        super().__init__(
            *args,
            custom_loader_init=True,
            **kwargs,
        )

    def add_arguments(self, parser):
        # Add the options provided by the superclass
        super().add_arguments(parser)

        parser.add_argument(
            "--mzxml-dir",
            type=str,
            help="The root directory of all mzXML files (containing instrument run data) associated with the study.",
            default=None,
            required=False,
        )

        # This option overrides dynamic format determination.
        parser.add_argument(
            "--infile-version",
            type=str,
            help=(
                f"{{{StudyLoader.get_supported_versions()}}} Version of the study doc (--infile).  "
                "Default: dynamically determined version."
            ),
            choices=StudyLoader.get_supported_versions(),
            required=False,
        )

        parser.add_argument(
            "--exclude-sheets",
            type=str,
            help=(
                f"[all] {{{[lc.DataSheetName for lc in StudyLoader.get_loader_classes()]}}} Load all sheets except "
                "those supplied here.  (Use default sheet names in place of any custom names.)"
            ),
            nargs="+",
            choices=[lc.DataSheetName for lc in StudyLoader.get_loader_classes()],
            default=[],
            required=False,
        )

        # TODO: Remove this after all dependent code has been updated for the new version of this script
        parser.add_argument(
            # Legacy support - catch this option and issue an error if it is used.
            "study_params",
            type=argparse.FileType("r"),
            nargs="?",
            help=argparse.SUPPRESS,
        )

    # TODO: Support for a dict of dataframes should be introduced in LoadtableCommand.get_dataframe and this override of
    # that method should be removed
    def get_dataframe(self, **kwargs):
        """Parses data from the infile.  This is an override of the superclass's method in order to return a dict of
        dataframes of all sheets instead of a dataframe of the cls.DataSheetName sheet.

        Args:
            kwargs (dict): Ignored superclass arguments.
        Exceptions:
            None
        Returns:
            (Dict[str, pandas DataFrame])
        """
        if self.options is None:
            raise OptionsNotAvailable()

        file = self.get_infile()
        if file is None:
            return None

        return read_from_file(file, sheet=None)

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadTableCommand._handler, which handles:
            - Retrieving the base-class-provided option values (and fills in the defaults provided by the loader_class)
            - Atomic transactions with optionally deferred rollback
            - Exception handling:
                - DryRun Exceptions
                - Contextualization of exceptions to the associated input in the file
            - Validation
                - Header and data type
                - Unique file constraints

        Args:
            options (dict of strings): String values provided on the command line by option name.
        Raises:
            None
        Returns:
            None
        """
        # TODO: Remove this after all dependent code has been updated for the new version of this script
        if options["study_params"] is not None:
            raise CommandError(
                "By supplying a positional argument, it looks like you're trying to call the old version of this "
                "script.  This script has been renamed.  Use `pythong manage.py legacy_load_study ...` instead."
                f"{options}"
            )

        try:
            df = read_from_file(self.get_infile(), sheet=None)
            self.loader_class = StudyLoader.get_derived_class(
                df,
                version=options.get("infile_version"),
            )
        except InvalidStudyDocVersion as isdv:
            raise CommandError(str(isdv)).with_traceback(isdv.__traceback__)
        except UnknownStudyDocVersion as usdv:
            raise CommandError(str(usdv) + "  See --infile-version.").with_traceback(
                usdv.__traceback__
            )
        except MultipleStudyDocVersions as msdv:
            raise CommandError(str(msdv) + "  See --infile-version.").with_traceback(
                msdv.__traceback__
            )

        # We can now instantiate the StudyV{number}Loader, since we know the study doc version
        self.init_loader(
            mzxml_dir=options.get("mzxml_dir"),
            exclude_sheets=options.get("exclude_sheets"),
        )

        self.load_data()
