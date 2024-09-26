import os
from collections import defaultdict, namedtuple
from typing import Dict, List

from django.db.models import Model

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.loaders.peak_annotation_files_loader import (
    PeakAnnotationFilesLoader,
)
from DataRepo.models.peak_group import PeakGroup
from DataRepo.utils.exceptions import (
    InfileError,
    generate_file_location_string,
)


class PeakGroupConflicts(TableLoader):
    """This class does not load any data, but processes a sheet meant to allow researchers to select peak annotation
    files from which a peak group should be loaded when there exist multiple representations of that compound for the
    same samples."""

    SAMPLE_DELIMITER = ";"

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    PEAKGROUP_KEY = "PEAKGROUP"
    ANNOTFILE_KEY = "ANNOTFILE"
    SAMPLECOUNT_KEY = "SAMPLECOUNT"
    EXAMPLE_KEY = "EXAMPLE"
    SAMPLES_KEY = "SAMPLES"

    DataSheetName = "Peak Group Conflicts"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "PEAKGROUP",
            "ANNOTFILE",
            "SAMPLECOUNT",
            "EXAMPLE",
            "SAMPLES",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        PEAKGROUP="Peak Group Conflict",
        ANNOTFILE="Selected Peak Annotation File",
        SAMPLECOUNT="Common Sample Count",
        EXAMPLE="Example Samples",
        SAMPLES="Common Samples",
    )

    # List of required header keys
    DataRequiredHeaders = [
        PEAKGROUP_KEY,
        ANNOTFILE_KEY,
        SAMPLES_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        PEAKGROUP_KEY: str,
        ANNOTFILE_KEY: str,
        SAMPLECOUNT_KEY: int,
        EXAMPLE_KEY: str,
        SAMPLES_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        [PEAKGROUP_KEY, SAMPLES_KEY],
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
                "A peak group that exists in multiple peak annotation files containing common samples.  Only 1 peak "
                "group may represent each compound per sample.  Note that different synonymns of the same compound "
                "are treated as qualitatively different compounds (to support for example, stereo-isomers)."
            ),
            format="Note that the order and case of the compound synonyms could differ in each file.",
        ),
        SAMPLES=TableColumn.init_flat(
            name=DataHeaders.SAMPLES,
            help_text=(
                "This column contains a sorted list of sample names that multiple peak annotation files have in "
                "common, and each measure the same peak group compound."
            ),
            type=str,
            format=f"A string of sample names delimited by '{SAMPLE_DELIMITER}'.",
        ),
        SAMPLECOUNT=TableColumn.init_flat(
            name=DataHeaders.SAMPLECOUNT,
            help_text=f"The number of {DataHeaders.SAMPLES} among the files listed for the given peak group compound.",
            type=int,
        ),
        EXAMPLE=TableColumn.init_flat(
            name=DataHeaders.EXAMPLE,
            help_text=(
                f"This column contains a sampling of the {DataHeaders.SAMPLES} between the files in the "
                f"{DataHeaders.ANNOTFILE} drop-down."
            ),
            type=str,
            format=f"A string of sample names delimited by '{SAMPLE_DELIMITER}'.",
        ),
        ANNOTFILE=TableColumn.init_flat(
            name=DataHeaders.ANNOTFILE,
            help_text=(
                "TraceBase will accept only one peak group measurement for each compound in a given sample.  Sometimes "
                "a compound can show up in multiple scans (e.g. in positive and negative mode scans).  You must select "
                "the file containing the best representation of each compound.  Using the provided drop-downs, select "
                "the peak annotation file from which this peak group should be loaded for the listed samples.  That "
                "compound in the remaining files will be skipped for those samples.  Note, each drop-down contains "
                "only the peak annotation files containing the peak group compound for that row."
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
            self.get_selected_representations(): Dict[str, Dict[str, Optional[str]]] - a dict of selected peak
                annotation files keyed on lower-cased and sorted peak group compounds and sample names.
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
                every sample and lower-cased peak group name.  The file will be None if there were conflicting conflict
                resolutions specified by the user for that sample and peak group compound (e.g. they duplicated a row
                and selected a different file one each).
        """
        selected_representations = defaultdict(lambda: defaultdict(str))

        all_representations = self.get_all_representations()

        self.check_for_duplicate_resolutions(all_representations)

        # Now construct the dict we will return, with the resolutions that are unambiguous
        samples_str: str
        for samples_str in all_representations.keys():
            for pgname in all_representations[samples_str].keys():
                if len(all_representations[samples_str][pgname].keys()) == 1:
                    selected_annot_file = list(
                        all_representations[samples_str][pgname].keys()
                    )[0]
                else:
                    # We get here when the user has messed with the sheet and possibly pasted in duplicate rows and
                    # selected a different file on each row.

                    # We've already issued errors on the duplicates above.  If we were to allow them all to load, it
                    # would result in multiple representation errors in the PeakAnnotationsLoader, which would be a
                    # redundant error and is thus unnecessary.  By adding None here, it will cause the peak group to
                    # be skipped in all peak annotation files in the PeakAnnotationsLoader.
                    selected_annot_file = None
                for sample in samples_str.strip().split(self.SAMPLE_DELIMITER):
                    selected_representations[sample.strip()][
                        pgname
                    ] = selected_annot_file

        return selected_representations

    def get_all_representations(self):
        """This returns a dict of the data in the sheet.

        Args:
            None
        Exceptions:
            None
        Returns:
            all_representations (Dict[str, Dict[str, Dict[str, List[int]]]]): A dict of all peak annotation files and
                the rows they were on, for every common sample combination and sorted and lower-cased peak group name.
        """
        all_representations = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for _, row in self.df.iterrows():
            # Grab the values from each column
            pgname_str = self.get_row_val(row, self.headers.PEAKGROUP)
            samples_str = self.get_row_val(row, self.headers.SAMPLES)
            annot_file_str = self.get_row_val(row, self.headers.ANNOTFILE)

            if self.is_skip_row():
                continue

            # NOTE: This does not check validity of the peak group name (i.e. it does not check if the delimited
            # compound synonyms exist in the database).  Note that this lower-cases the peak group name only for its
            # usage as a key in the all_representations dict, because the purpose of saving the compound synonyms in
            # PeakGroup is to be able to differentiate between qualitatively different versions of a compound linked to
            # the same compound record (e.g. stereo-isomers).  Making this case insensitive mitigates the possibility
            # that synonyms differing only by case create duplicate representations that are not qualitatively
            # different.  Note also that compound_synonyms_to_peak_group_name sorts the synonyms so that "citrate/
            # isocitrate" is not considered different from "isocitrate/citrate".  I.e. synonyms only differing by case
            # and/or order are considered multiple representations.
            pgname = PeakGroup.compound_synonyms_to_peak_group_name(
                [ns.strip().lower() for ns in pgname_str.split(PeakGroup.NAME_DELIM)]
            )
            annot_file = list(os.path.split(annot_file_str))[1]

            all_representations[samples_str][pgname][annot_file].append(self.rownum)

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
        for samples_str in all_representations.keys():
            for pgname in all_representations[samples_str].keys():
                multiple_resolutions = (
                    len(all_representations[samples_str][pgname].keys()) > 1
                )
                for rows in all_representations[samples_str][pgname].values():
                    # NOTE: While the samples string and pgname combo are required to be unique, that requirement is
                    # applied BEFORE the peak group name is sorted and lower-cased, so it needs to be checked again.
                    if multiple_resolutions or len(rows) > 1:
                        dupes[samples_str][pgname] += len(rows)

        # Process any duplicate (equivalent or conflicting) resolutions
        for samples_str in dupes.keys():
            for pgname in dupes[samples_str].keys():
                if len(all_representations[samples_str][pgname].keys()) == 1:
                    # Just issue a warning if all the row duplicates select the same file
                    annot_file = list(all_representations[samples_str][pgname].keys())[
                        0
                    ]
                    self.aggregated_errors_object.buffer_exception(
                        InfileError(
                            (
                                f"Multiple equivalent resolutions for '{self.DataHeaders.PEAKGROUP}' '{pgname}' in %s "
                                f"on rows: {all_representations[samples_str][pgname][annot_file]}."
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
                            f"'{file}', on row(s): {all_representations[samples_str][pgname][file]}"
                            for file in all_representations[samples_str][pgname].keys()
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
