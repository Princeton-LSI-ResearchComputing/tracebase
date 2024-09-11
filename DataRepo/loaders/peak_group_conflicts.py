import os
from collections import defaultdict, namedtuple
from typing import Dict, List

from django.db.models import Model

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.models.peak_group import PeakGroup
from DataRepo.utils.exceptions import (
    InfileError,
    generate_file_location_string,
)


class PeakGroupConflicts(TableLoader):
    """This class does not load any data, but processes a sheet meant to allow researchers to select peak annotation
    files from which a peak group should be loaded when there exist multiple representations of that compound for the
    same samples from the same sequence."""

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    PEAKGROUP_KEY = "PEAKGROUP"
    SEQNAME_KEY = "SEQNAME"
    ANNOTFILE_KEY = "ANNOTFILE"

    DataSheetName = "Peak Group Conflicts"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "PEAKGROUP",
            "SEQNAME",
            "ANNOTFILE",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        PEAKGROUP="Peak Group Conflict",
        SEQNAME="Sequence Name",
        ANNOTFILE="Selected Peak Annotation File",
    )

    # List of required header keys
    DataRequiredHeaders = [
        PEAKGROUP_KEY,
        SEQNAME_KEY,
        ANNOTFILE_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        PEAKGROUP_KEY: str,
        SEQNAME_KEY: str,
        ANNOTFILE_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [PEAKGROUP_KEY, SEQNAME_KEY],
    ]

    # A mapping of database field to column.  Only set when 1 field maps to 1 column.  Omit others.
    # NOTE: The sample headers are always different, so we cannot map those here
    FieldToDataHeaderKey: Dict[str, dict] = {}

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        PEAKGROUP=TableColumn.init_flat(
            name=DataHeaders.PEAKGROUP,
            field=PeakGroup.name,
            # TODO: Replace static references to columns in other sheets with variables (once the circular import issue
            # is resolved)
            guidance=(
                "A peak group that exists in multiple peak annotation files derived from the same MS Run Sequence.  "
                "Only 1 peak group may represent each compound.  Note that different synonymns of the same compound "
                "are treated as qualitatively different compounds (to support for example, stereo-isomers)."
            ),
            format="Note that the order and case of the compound synonyms could differ in each file.",
        ),
        SEQNAME=TableColumn.init_flat(
            name=DataHeaders.SEQNAME,
            help_text=(
                "The MS Run Sequence that multiple peak annotation files containing overlapping peak groups were "
                "derived from."
            ),
            type=str,
            format=(
                "Comma-delimited string combining the values from these columns from the "
                f"{SequencesLoader.DataSheetName} sheet in this order:\n"
                f"- {SequencesLoader.DataHeaders.OPERATOR}\n"
                f"- {SequencesLoader.DataHeaders.LCNAME}\n"
                f"- {SequencesLoader.DataHeaders.INSTRUMENT}\n"
                f"- {SequencesLoader.DataHeaders.DATE}"
            ),
            dynamic_choices=ColumnReference(
                loader_class=SequencesLoader,
                loader_header_key=SequencesLoader.SEQNAME_KEY,
            ),
        ),
        ANNOTFILE=TableColumn.init_flat(
            name=DataHeaders.ANNOTFILE,
            help_text=(
                "There must exist only one peak group compound for every sample and sequence.  Sometimes a compound "
                "can show up in multiple scans (e.g. in positive and negative mode scans).  You must select the file "
                "containing the best representation of each compound.  Using the provided drop-downs, select the peak "
                "annotation file from which this peak group should be loaded.  That compound in the remaining files "
                "will be skipped.  Note, each drop-down contains only the peak annotation files containing the peak "
                "group compound for that row."
            ),
            reference=ColumnReference(
                loader_class=PeakAnnotationFilesLoader,
                loader_header_key=PeakAnnotationFilesLoader.FILE_KEY,
            ),
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models: List[Model] = []

    def load_data(self):
        """This does not actually load data.  It just returns a dict of the data in the sheet.  However, an error
        buffered in this method should prevent loading of data in the PeakAnnotationsLoader.

        Args:
            None
        Exceptions:
            None
        Returns:
            self.get_selected_representations()
        """
        return self.get_selected_representations()

    def get_selected_representations(self):
        """This returns a dict of the data in the sheet.

        Args:
            None
        Exceptions:
            Buffers:
                InfileError
            Raises:
                None
        Returns:
            selected_representations (Dict[str, Dict[str, Optional[str]]]): A dict of selected peak annotation files for
                every sequence and sorted and lower-cased peak group name.  The file will be None, if there was an
                error associated with that compound and/or sequence.
        """
        selected_representations = defaultdict(lambda: defaultdict(str))

        all_representations = self.get_all_representations()

        self.check_for_duplicate_resolutions(all_representations)

        # Now construct the dict we will return, with the resolutions that are unambiguous
        for seqname in all_representations.keys():
            for pgname in all_representations[seqname].keys():
                if len(all_representations[seqname][pgname].keys()) == 1:
                    annot_file = list(all_representations[seqname][pgname].keys())[0]
                    selected_representations[seqname][pgname] = annot_file
                else:
                    # We've already issues errors on the duplicates above.  If we were to allow them all to load, it
                    # would result in multiplke representation errors in the PeakAnnotationsLoader, which would be a
                    # redundant error and is thus unnecessary.  By adding None here, it will cause the peak group to be
                    # skipped in all peak annotation files in the PeakAnnotationsLoader.
                    selected_representations[seqname][pgname] = None

        return selected_representations

    def get_all_representations(self):
        """This returns a dict of the data in the sheet.

        Args:
            None
        Exceptions:
            None
        Returns:
            all_representations (Dict[str, Dict[str, Dict[str, List[int]]]]): A dict of all peak annotation files and
                the rows they were on, for every sequence and sorted and lower-cased peak group name.
        """
        all_representations = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for _, row in self.df.iterrows():
            # Grab the values from each column
            pgname_str = self.get_row_val(row, self.headers.PEAKGROUP)
            seqname = self.get_row_val(row, self.headers.SEQNAME)
            annot_file_str = self.get_row_val(row, self.headers.ANNOTFILE)

            if self.is_skip_row():
                continue

            # NOTE: This does not check validity of the peak group name (i.e. it does not check if the delimited
            # compound synonyms exist in the database)
            pgname = PeakGroup.NAME_DELIM.join(
                sorted(
                    [
                        ns.strip().lower()
                        for ns in pgname_str.split(PeakGroup.NAME_DELIM)
                    ]
                )
            )
            annot_file = list(os.path.split(annot_file_str))[1]

            all_representations[seqname][pgname][annot_file].append(self.rownum)

        return all_representations

    def check_for_duplicate_resolutions(self, all_representations):
        """This checks for duplicate resolutions/rows and buffers either errors or warnings.

        This is necessary because peak group names can be equivalent due to case and/or order (when there are multiple
        compounds).

        Args:
            all_representations (Dict[str, Dict[str, Dict[str, List[int]]]]): A dict of all peak annotation files and
                the rows they were on, for every sequence and sorted and lower-cased peak group name.
        Exceptions:
            Buffers:
                InfileError
            Raises:
                None
        Returns:
            None
        """
        dupes = defaultdict(lambda: defaultdict(int))

        # This is more efficiently constructed in the get_all_representations method, but much easier to read the
        # overall code when separate.

        # Build the dupes dict
        for seqname in all_representations.keys():
            for pgname in all_representations[seqname].keys():
                multiple_resolutions = (
                    len(all_representations[seqname][pgname].keys()) > 1
                )
                for rows in all_representations[seqname][pgname].values():
                    # NOTE: While the seqname and pgname combo are required to be unique, that requirement is applied
                    # BEFORE the peak group name is sorted and lower-cased, so it needs to be checked again.
                    if multiple_resolutions or len(rows) > 1:
                        dupes[seqname][pgname] += len(rows)

        # Process any duplicate (equivalent or conflicting) resolutions
        for seqname in dupes.keys():
            for pgname in dupes[seqname].keys():
                if len(all_representations[seqname][pgname].keys()) == 1:
                    # Just issue a warning if all the row duplicates select the same file
                    annot_file = list(all_representations[seqname][pgname].keys())[0]
                    self.aggregated_errors_object.buffer_exception(
                        InfileError(
                            (
                                f"Multiple equivalent resolutions for '{self.DataHeaders.PEAKGROUP}' '{pgname}' in %s "
                                f"on rows: {all_representations[seqname][pgname][annot_file]}."
                            ),
                            file=self.friendly_file,
                            sheet=self.sheet,
                            column=self.DataHeaders.PEAKGROUP,
                        ),
                        is_error=False,
                        is_fatal=self.validate,
                    )
                else:
                    # Issue a fatal error if the resolution to the conflict is different on duplicate rows
                    deets = "\n\t".join(
                        [
                            f"'{file}', on row(s): {all_representations[seqname][pgname][file]}"
                            for file in all_representations[seqname][pgname].keys()
                        ]
                    )
                    sheetref = generate_file_location_string(
                        file=self.friendly_file, sheet=self.sheet
                    )
                    self.aggregated_errors_object.buffer_error(
                        InfileError(
                            (
                                f"Multiple differing resolutions for '{self.DataHeaders.PEAKGROUP}' '{pgname}' in %s:\n"
                                f"\t{deets}\nNote, the peak group names may differ by case and/or compound order."
                            ),
                            file=self.friendly_file,
                            sheet=self.sheet,
                            suggestion=f"Delete all but one of the above rows from {sheetref}.",
                        ),
                    )
