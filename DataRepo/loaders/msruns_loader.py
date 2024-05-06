import os
import re
from collections import defaultdict, namedtuple
from pathlib import Path
from typing import Dict, Optional

import xmltodict
from django.db import transaction
from django.db.models import Max, Min, Q

from DataRepo.loaders.sequences_loader import SequencesLoader
from DataRepo.loaders.table_column import ColumnReference, TableColumn
from DataRepo.loaders.table_loader import TableLoader
from DataRepo.models import MSRunSample, MSRunSequence, PeakGroup
from DataRepo.models.archive_file import ArchiveFile, DataFormat, DataType
from DataRepo.models.sample import Sample
from DataRepo.models.utilities import update_rec
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    InfileError,
    MixedPolarityErrors,
    MutuallyExclusiveArgs,
    MzxmlParseError,
    RecordDoesNotExist,
    RequiredColumnValue,
    RequiredColumnValues,
)
from DataRepo.utils.file_utils import string_to_datetime


class MSRunsLoader(TableLoader):
    """Class to load the MSRunSample table."""

    # These are common suffixes repeatedly appended to accucor/isocorr sample names.  Researchers tend to do this in
    # order make the file names unique enough to collect them together in a single directory.  They tend to do that by
    # appending a string with a delimiting underscore that indicates polarities and/or scan ranges.  This is not
    # perfect.  The full pattern into which these patterns are included can be used to strip out these suffixes even if
    # they have been chained together (e.g. _pos_scan1).
    DEFAULT_SAMPLE_HEADER_SUFFIXES = [
        r"_pos",
        r"_neg",
        r"_scan[0-9]+",
    ]

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    SAMPLENAME_KEY = "SAMPLENAME"
    SAMPLEHEADER_KEY = "SAMPLEHEADER"
    MZXMLNAME_KEY = "MZXMLNAME"
    ANNOTNAME_KEY = "ANNOTNAME"
    SEQNAME_KEY = "SEQNAME"

    DataSheetName = "Peak Annotation Details"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "SAMPLENAME",
            "SAMPLEHEADER",
            "MZXMLNAME",
            "ANNOTNAME",
            "SEQNAME",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        SAMPLENAME="Sample Name",
        SAMPLEHEADER="Sample Data Header",
        MZXMLNAME="mzXML File Name",
        ANNOTNAME="Peak Annotation File Name",
        SEQNAME="Sequence Name",
    )

    # List of required header keys
    DataRequiredHeaders = [
        SAMPLENAME_KEY,
        [
            # Either the sample header or the mzXML file name (e.g. files not associated with a peak annot file)
            SAMPLEHEADER_KEY,
            MZXMLNAME_KEY,
        ],
        SEQNAME_KEY,
    ]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        SAMPLENAME_KEY: str,
        SAMPLEHEADER_KEY: str,
        MZXMLNAME_KEY: str,
        ANNOTNAME_KEY: str,
        SEQNAME_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [
        # All combined must be unique, but note that duplicates of SAMPLENAME_KEY, (SAMPLEHEADER_KEY or MZXMLNAME_KEY),
        # and SEQUENCE_KEY will be ignored.  Duplicates can exist if the same mzXML was used in multiple peak annotation
        # files.
        [SAMPLENAME_KEY, SAMPLEHEADER_KEY, MZXMLNAME_KEY, ANNOTNAME_KEY, SEQNAME_KEY],
    ]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        MSRunSample.__name__: {
            "sample": SAMPLENAME_KEY,
            "msrun_sequence": SEQNAME_KEY,
        },
        ArchiveFile.__name__: {
            "filename": MZXMLNAME_KEY,
        },
    }

    DataColumnMetadata = DataTableHeaders(
        SAMPLENAME=TableColumn.init_flat(
            name=DataHeaders.SAMPLENAME, field=MSRunSample.sample
        ),
        SAMPLEHEADER=TableColumn.init_flat(
            name=DataHeaders.SAMPLEHEADER,
            help_text=f"Sample header from {DataHeaders.ANNOTNAME}.",
        ),
        MZXMLNAME=TableColumn.init_flat(
            name=DataHeaders.MZXMLNAME,
            field=MSRunSample.ms_data_file,
            header_required=False,
            value_required=False,
        ),
        ANNOTNAME=TableColumn.init_flat(
            name=DataHeaders.ANNOTNAME,
            help_text=(
                "Name of the accucor or isocorr file that this sample was analyzed in, if any.  If the sample on this "
                f"row was included in a {DataHeaders.ANNOTNAME}, add the name of that file here.  If you are loading "
                f"an {DataHeaders.MZXMLNAME} that was not used in a {DataHeaders.ANNOTNAME}, leave this value empty."
            ),
            guidance=(
                f"You do not have the have an {DataHeaders.MZXMLNAME} associated with every {DataHeaders.SAMPLEHEADER} "
                f"from a {DataHeaders.ANNOTNAME} worked out ahead of time.  That association can be made at a later "
                "date."
            ),
            header_required=False,
            value_required=False,
        ),
        SEQNAME=TableColumn.init_flat(
            name=DataHeaders.SEQNAME,
            help_text=(
                f"The MSRun Sequence associated with the {DataHeaders.SAMPLENAME}, {DataHeaders.SAMPLEHEADER}, and/or "
                f"{DataHeaders.MZXMLNAME} on this row."
            ),
            guidance=(
                "Use the dropdowns to select values in this column.  If the dropdowns are empty or the "
                f"sequence is missing, add a row for it to the {SequencesLoader.DataSheetName} sheet."
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
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling.
    Models = [MSRunSample, PeakGroup, ArchiveFile]

    def __init__(self, *args, **kwargs):
        """Constructor.
        Args:
            Superclass Args:
                df (Optional[pandas dataframe]): Data, e.g. as parsed from a table-like file.
                dry_run (Optional[boolean]) [False]: Dry run mode.
                defer_rollback (Optional[boolean]) [False]: Defer rollback mode.  DO NOT USE MANUALLY - A PARENT SCRIPT
                    MUST HANDLE THE ROLLBACK.
                data_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                defaults_sheet (Optional[str]) [None]: Sheet name (for error reporting).
                file (Optional[str]) [None]: File name (for error reporting).
                user_headers (Optional[dict]): Header names by header key.
                defaults_df (Optional[pandas dataframe]): Default values data from a table-like file.
                defaults_file (Optional[str]) [None]: Defaults file name (None if the same as infile).
                headers (Optional[DefaultsTableHeaders namedtuple]): headers by header key.
                defaults (Optional[DefaultsTableHeaders namedtuple]): default values by header key.
            Derived (this) class Args:
                mzxml_files (Optional[str]): Paths to mzXML files.
                operator (Optional[str]): The researcher who ran the mass spec.  Mutually exclusive with defaults_df
                    (when it has a default for the operator column for the Sequences sheet).
                lc_protocol_name (Optional[str]): Name of the liquid chromatography method.  Mutually exclusive with
                    defaults_df (when it has a default for the lc_protocol_name column for the Sequences sheet).
                instrument (Optional[str]): Name of the mass spec instrument.  Mutually exclusive with defaults_df
                    (when it has a default for the instrument column for the Sequences sheet).
                date (Optional[str]): Date the Mass spec instrument was run.  Format: YYYY-MM-DD.  Mutually exclusive
                    with defaults_df (when it has a default for the date column for the Sequences sheet).
        Exceptions:
            None
        Returns:
            None
        """
        self.mzxml_files = kwargs.pop("mzxml_files", [])
        operator_default = kwargs.pop("operator", None)
        date_default = kwargs.pop("date", None)
        lc_protocol_name_default = kwargs.pop("lc_protocol_name", None)
        instrument_default = kwargs.pop("instrument", None)

        super().__init__(*args, **kwargs)

        # We are going to use defaults from the SequencesLoader if no dataframe (i.e. --infile) was provided
        seqloader = SequencesLoader(
            df=self.file,  # Only used for reporting errors with the defaults sheet
            defaults_df=self.defaults_df,
            defaults_file=self.defaults_file,
        )
        seqdefaults = seqloader.get_user_defaults()
        # TODO: Figure out a better way to handle buffered exceptions from another class that are only raised from a
        # specific method, so that methods raise them as a group instead of needing to incorporate instance loaders like
        # this for buffered errors
        self.aggregated_errors_object.merge_aggregated_errors_object(
            seqloader.aggregated_errors_object
        )

        if seqdefaults is not None:
            mutex_arg_errs = []
            mutex_def_errs = []

            # get the operator_default from either the defaults sheet/file or from the arg
            self.operator_default = seqdefaults[seqloader.DataHeaders.OPERATOR]
            if (
                self.operator_default is not None
                and operator_default is not None
                and self.operator_default != operator_default
            ):
                mutex_arg_errs.append("operator")
                mutex_def_errs.append(seqloader.DataHeaders.OPERATOR)
            elif self.operator_default is None and operator_default is not None:
                self.operator_default = operator_default

            # get the date_default from either the defaults sheet/file or from the arg
            self.date_default = seqdefaults[seqloader.DataHeaders.DATE]
            if (
                self.date_default is not None
                and date_default is not None
                and self.date_default != date_default
            ):
                mutex_arg_errs.append("date")
                mutex_def_errs.append(seqloader.DataHeaders.DATE)
            elif self.date_default is None and date_default is not None:
                self.date_default = date_default

            # get the lc_protocol_name_default from either the defaults sheet/file or from the arg
            self.lc_protocol_name_default = seqdefaults[seqloader.DataHeaders.LCNAME]
            if (
                self.lc_protocol_name_default is not None
                and lc_protocol_name_default is not None
                and self.lc_protocol_name_default != lc_protocol_name_default
            ):
                mutex_arg_errs.append("lc_protocol_name")
                mutex_def_errs.append(seqloader.DataHeaders.LCNAME)
            elif (
                self.lc_protocol_name_default is None
                and lc_protocol_name_default is not None
            ):
                self.lc_protocol_name_default = lc_protocol_name_default

            # get the instrument_default from either the defaults sheet/file or from the arg
            self.instrument_default = seqdefaults[seqloader.DataHeaders.INSTRUMENT]
            if (
                self.instrument_default is not None
                and instrument_default is not None
                and self.instrument_default != instrument_default
            ):
                mutex_arg_errs.append("instrument")
                mutex_def_errs.append(seqloader.DataHeaders.INSTRUMENT)
            elif self.instrument_default is None and instrument_default is not None:
                self.instrument_default = instrument_default

            if len(mutex_arg_errs) > 0:
                raise self.aggregated_errors_object.buffer_error(
                    MutuallyExclusiveArgs(
                        (
                            f"Multiple conflicting defaults defined via both arguments {mutex_arg_errs} and via the "
                            f"{seqloader.DataSheetName} defaults sheet/file {mutex_def_errs}: %s.  Please use either "
                            f"the {seqloader.DataSheetName} defaults sheet/file or the command line arguments (not "
                            "both)."
                        ),
                        file=(
                            self.defaults_file
                            if self.defaults_file is not None
                            else self.file
                        ),
                        sheet=self.sheet,
                        column=seqloader.DefaultsHeaders.DEFAULT_VALUE,
                    )
                )
        else:
            self.operator_default = operator_default
            self.date_default = date_default
            self.lc_protocol_name_default = lc_protocol_name_default
            self.instrument_default = instrument_default

        # This will contain metadata parsed from the mzXML files (and the created ArchiveFile records to be added to
        # MSRunSample records
        self.mzxml_dict = defaultdict(lambda: defaultdict(list))

    def load_data(self):
        """Loads the MSRunSequence table from the dataframe.
        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # 1. Traverse the supplied mzXML files
        #    - create ArchiveFile records.
        #    - Extract data from the mzxML files
        #    - store extracted metadata and ArchiveFile record objects in self.mzxml_dict, a 4D dict:
        #      {mzXML_name: {mzXML_dir: [{**metadata},...]}}
        # We need the directory to match the mzXML in the infile with the MSRunSequence name on the same row.  mzXML
        # files can easily have the same name and all users can reasonably be expected to know is their location and the
        # sequence they were a part of.  Normally, all that's needed is a name, but if that name is not unique, and
        # there are multiple sequences in the file, we need a way to distinguish them, and the path is that way.
        for mzxml_file in self.mzxml_files:
            try:
                self.get_or_create_mzxml_and_raw_archive_files(mzxml_file)
            except Exception:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                pass

        # 2. Traverse the infile
        #    - create MSRunSample records
        #    - associate them with the mzXML ArchiveFile records and Sample and MSRunSequence records, (keeping track of
        #      which mzXMLs have been added to MSRunSample records)
        if self.df is not None:
            for _, row in self.df.iterrows():
                try:
                    self.get_create_or_update_msrun_sample_from_row(row)
                except Exception:
                    # Exception handling was handled
                    # Continue processing rows to find more errors
                    pass

        # 3. Traverse leftover mzXML/ArchiveFile records unassociated with those processed in step 2, using:
        #    - The name of the mzXML automatically mapped to a sample name
        #    - The default researcher, date, lc-protocol-name, instrument supplied (if any were not supplied, error).
        # We don't want to complain about no sequence defaults being defined if the the sheet had everything, so let's
        # see if any leftover mzxml files actually exist first.
        if self.leftover_mzxml_files_exist():

            # Get the sequence defined by the defaults (for researcher, protocol, instrument, and date)
            msrun_sequence = self.get_msrun_sequence()

            for mzxml_basename in self.mzxml_dict.keys():

                # Guess the sample based on the mzXML file's basename
                sample_name = self.guess_sample_name(mzxml_basename)
                sample = self.get_sample_by_name(sample_name, from_mzxml=True)

                for mzxml_dir in self.mzxml_dict[mzxml_basename].keys():
                    for mzxml_metadata in self.mzxml_dict[mzxml_basename][mzxml_dir]:
                        try:
                            self.get_or_create_msrun_sample_from_mzxml(
                                sample, msrun_sequence, mzxml_metadata
                            )
                        except Exception:
                            # Exception handling was handled
                            # Continue processing rows to find more errors
                            pass

    @transaction.atomic
    def get_or_create_mzxml_and_raw_archive_files(self, mzxml_file):
        """Get or create ArchiveFile records for an mzXML file and a record for its raw file.  Updates self.mzxml_dict.
        Args:
            mzxml_file (str or Path object)
        Exceptions:
            Raises:
                DataType.DoesNotExist
                DataFormat.DoesNotExist
            Buffers:
                DataType.DoesNotExist
                DataFormat.DoesNotExist
        Returns:
            mzaf_rec (ArchiveFile)
            mzaf_created (boolean)
            rawaf_rec (ArchiveFile)
            rawaf_created (boolean)
        """
        # Get or create the ArchiveFile record for the mzXML
        try:
            mz_rec_dict = {
                # "filename": xxx,  # Gets automatically filled in by the override of get_or_create
                # "checksum": xxx,  # Gets automatically filled in by the override of get_or_create
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                "file_location": mzxml_file,  # Intentionally a string and not a File object
                "data_type": DataType.objects.get(code="ms_data"),
                "data_format": DataFormat.objects.get(code="mzxml"),
            }
            mzaf_rec, mzaf_created = ArchiveFile.objects.get_or_create(**mz_rec_dict)
            if mzaf_created:
                self.created(ArchiveFile.__name__)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            self.skipped(ArchiveFile.__name__)  # Skipping raw file below
            raise dne
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, mz_rec_dict)
            self.errored(ArchiveFile.__name__)
            self.skipped(ArchiveFile.__name__)  # Skipping raw file below
            # Raise to do a rollback
            raise e

        mzxml_dir, mzxml_filename = os.path.split(mzxml_file)

        # Parse out the polarity, mz_min, mz_max, raw_file_name, and raw_file_sha1
        mzxml_metadata, errs = self.parse_mzxml(mzxml_file)
        if len(errs.exceptions) > 0:
            self.aggregated_errors_object.merge_aggregated_errors_object(errs)

        # Get or create an ArchiveFile record for a raw file
        try:
            raw_rec_dict = {
                "filename": mzxml_metadata["raw_file_name"],
                "checksum": mzxml_metadata["raw_file_sha1"],
                # "imported_timestamp": xxx,  # Gets automatically filled in by the model
                # "file_location": xxx,  # We do not store raw files
                "data_type": DataType.objects.get(code="ms_data"),
                "data_format": DataFormat.objects.get(code="ms_raw"),
            }
            rawaf_rec, rawaf_created = ArchiveFile.objects.get_or_create(**raw_rec_dict)
            if rawaf_created:
                self.created(ArchiveFile.__name__)
            else:
                self.existed(ArchiveFile.__name__)
        except (DataType.DoesNotExist, DataFormat.DoesNotExist) as dne:
            self.aggregated_errors_object.buffer_error(dne)
            self.skipped(ArchiveFile.__name__)
            # Skipping mzXML file above (rolled back)
            self.skipped(ArchiveFile.__name__)
            raise dne
        except Exception as e:
            self.handle_load_db_errors(e, ArchiveFile, raw_rec_dict)
            self.errored(ArchiveFile.__name__)
            # Raise to do a rollback
            raise e

        # Add in the ArchiveFile record objects
        mzxml_metadata["mzaf_record"] = mzaf_rec
        mzxml_metadata["rawaf_record"] = rawaf_rec
        # And we'll use this for error reporting
        mzxml_metadata["mzxml_dir"] = mzxml_dir
        mzxml_metadata["mzxml_filename"] = mzxml_filename

        # We will use this to know when to add leftovers that were not in the infile
        mzxml_metadata["added"] = False

        # Save the metadata by mzxml name (which may not be unique, so we're using the record ID as a second key, so
        # that we can later associate a sample header (with the same non-unique issue) to its multiple mzXMLs).
        mzxml_basename, _ = os.path.splitext(mzxml_filename)
        self.mzxml_dict[mzxml_basename][mzxml_dir].append(mzxml_metadata)

        return (
            mzaf_rec,
            mzaf_created,
            rawaf_rec,
            rawaf_created,
        )

    @transaction.atomic
    def get_create_or_update_msrun_sample_from_row(self, row):
        """Takes a row from the Peak Annotation Details sheet/file and gets, creates, or updates MSRunSample records.
        Updates occur if a placeholder record was found to pre-exist.  This is so that mzXML files can be loaded at a
        later date.

        Updates self.mzxml_dict (via get_matching_mzxml_metadata) to denote which mzXML files were included in
        MSRunSample records identified from the row data.  This is later used to process leftover mzXML files that were
        not denoted in the peak annotation details file/sheet.

        Args:
            row (pandas dataframe row)
        Exceptions:
            Raises:
                None (that are created here)
            Buffers:
                RecordDoesNotExist
        Returns:
            rec (MSRunSample)
            created (boolean)
            updated (boolean)
        """
        created = False
        updated = False
        rec = None

        try:
            msrs_rec_dict = None
            sample_name = self.get_row_val(row, self.headers.SAMPLENAME)
            sample_header = self.get_row_val(row, self.headers.SAMPLEHEADER)
            mzxml_path = self.get_row_val(row, self.headers.MZXMLNAME)
            sequence_name = self.get_row_val(row, self.headers.SEQNAME)
            annot_name = self.get_row_val(row, self.headers.ANNOTNAME)

            sample = self.get_sample_by_name(sample_name)
            msrun_sequence = self.get_msrun_sequence(name=sequence_name)

            if sample is None or msrun_sequence is None or self.is_skip_row():
                self.skipped(MSRunSample.__name__)
                return rec, created, updated

            # Determine what actual file from the self.mzxml_dict matches the sample/headers/mzxml detailed on
            # this row so that we can be assured we've got the correct MSRunSequence record assigned from above.
            mzxml_metadata = self.get_matching_mzxml_metadata(
                sample_name,
                sample_header,
                mzxml_path,  # Might just be the filename
            )

            if mzxml_metadata["added"] is True:
                self.aggregated_errors_object.buffer_warning(
                    InfileError(
                        (
                            f"An MSRunSample record has already been associated with this mzXML: {mzxml_metadata}.  "
                            "This may be a duplicate mzXML file reference: %s."
                        ),
                        file=self.file,
                        sheet=self.sheet,
                        column=self.headers.MZXMLNAME,
                        rownum=self.rownum,
                    )
                )

            if mzxml_path is not None and mzxml_metadata is None:
                self.skipped(MSRunSample.__name__)
                return rec, created, updated

            # Mark this particular mz ArchiveFile record as having been added to an MSRunSample record (even though
            # retrieval and/or creation may fail below [which will buffer an error that eventually will be raised],
            # because the point is that we don't try and get or create it again as a "leftover" from the infile)
            mzxml_metadata["added"] = True

            msrs_rec_dict = {
                "msrun_sequence": msrun_sequence,
                "sample": sample,
                "polarity": mzxml_metadata["polarity"],
                "mz_min": mzxml_metadata["mz_min"],
                "mz_max": mzxml_metadata["mz_max"],
                "ms_raw_file": mzxml_metadata["rawaf_record"],
                "ms_data_file": mzxml_metadata["mzaf_record"],
            }

            # At this point, we have the right sample and sequence, and we either have no mzXML file or we have an mzXML
            # file with extracted metadata.  In either case, we need to...

            # See if there exists a matching placeholder record
            existing_placeholder_qs = MSRunSample.objects.filter(
                msrun_sequence=msrun_sequence,
                sample=sample,
                ms_data_file__isnull=True,
            )

            # We're going to assume that the placeholder unique constraint will prevent the possibility of multiple
            # placeholders returned.

            if mzxml_metadata["mzaf_record"] is None:
                if existing_placeholder_qs.count() == 0:
                    # Create a placeholder record
                    rec = MSRunSample.objects.create(**msrs_rec_dict)
                    created = True
                else:
                    rec = existing_placeholder_qs.get()

                # NOTE: We are going to allow peak groups from different peak annotation files to link to the same
                # placeholder record, but note that this means that we MUST not assign polarity, mz_min, or mz_max in a
                # placeholder record.  This also means that when we add an mzXML file to an MSRunSample placeholder
                # record, we must ensure that the peak groups are relinked to the correct record (either keeping a
                # placeholder for the unmatching ones or just updating the placeholder to add the mzXML)
            else:
                # We now need to determine if there's a placeholder record that we should update or whether we should
                # create a new record.
                if existing_placeholder_qs.count() == 0:
                    # Get or create a record with an mzXML file
                    # We can assume that the peak annotation files linked in the peak groups are correct (even if they
                    # differ, because the same mzXML could have been used in multiple peak annotation files)
                    rec, created = MSRunSample.objects.get_or_create(**msrs_rec_dict)
                else:
                    # A peak group can only link to 1 MSRunSample record, and thereby can only link to 1 mzXML file (for
                    # a particular sample).  For any particular peak group linked to the MSRunSample placeholder record,
                    # we can look at its peak data and sort the peak groups into 2 groups: those whose medMzs fall into
                    # the scan range and those that don't.  If all are in the scan range, we just update the placeholder
                    # to add the mzXML.  If none do, we just create a separate MSRunSample record.  And if it's split,
                    # we create a new MSRunSample record and the peak groups that fall in range will get updated links
                    # to the new record.

                    # This will be a greedy heuristic.  If there are any mzXMLs with overplapping scan ranges, the peak
                    # groups will end up linked to the first one that's loaded, because we don't look in MSRunSample
                    # records that have different mzsXML files in them - only in the placeholder records.

                    # A placeholder record exists, so we need to see if this concrete record should update the
                    # placeholder, be separately created, or, if it already exists, should be a delete/update combo (or
                    # an error)
                    existing_concrete_rec = MSRunSample.objects.filter(
                        **msrs_rec_dict
                    ).first()  # possibly None
                    existing_placeholder_rec = existing_placeholder_qs.get()

                    matching_peakgroups_qs, unmatching_peakgroups_qs = (
                        self.separate_placeholder_peak_groups(
                            msrs_rec_dict,
                            annot_name,
                            existing_placeholder_rec,
                        )
                    )

                    if (
                        matching_peakgroups_qs.count() > 0
                        and unmatching_peakgroups_qs.count() == 0
                    ):
                        # The concrete record matches all of the placeholder's peakgroups (i.e. a placeholder was
                        # created in the past and associated with accucor data and we now have an mzXML file for it).

                        updated = True

                        # If there is no existing concrete record
                        if existing_concrete_rec is None:
                            # Update the placeholder (making it a concrete record)
                            update_rec(existing_placeholder_rec, msrs_rec_dict)
                            rec = existing_placeholder_rec
                        else:
                            # The Peak Annotation Details sheet has newly associated 2 existing records: a placeholder
                            # and one with an mzXML.  This could happen if the mzXML was loaded without peak data and
                            # then peak data was loaded without having had an mzXML associated with it, then the Peak
                            # Annotation Details sheet is updated to associate the peak data (peak annotation file) with
                            # an mzXML, sample, and sequence).

                            # We need to do something different based on the existing concrete record's currently linked
                            # peak groups

                            if existing_concrete_rec.peak_groups.count() == 0:
                                # The existing record has no peak groups, so delete it and update the placeholder (since
                                # all its peakgroups match)
                                existing_concrete_rec.delete()
                                update_rec(existing_placeholder_rec, msrs_rec_dict)
                                rec = existing_placeholder_rec
                                # We're counting what is essentially a "merge" event here as an update event
                            else:
                                # Both the placeholder and the existing concrete MSRunSample records have peak groups,
                                # so update the peak groups (belonging to the placeholder to assign them to the existing
                                # concrete record) and delete the placeholder.

                                # Link the matching peak groups to the existing concrete MSRunSample record
                                matching_peakgroups_qs.update(
                                    msrun_sample=existing_concrete_rec
                                )
                                for pg_rec in matching_peakgroups_qs:
                                    pg_rec.full_clean()
                                    pg_rec.save()
                                # Delete the newly empty placeholder record, as we no longer need it.
                                existing_placeholder_rec.delete()
                                rec = existing_concrete_rec
                                # Count the PeakGroup records as having been updated.
                                self.updated(
                                    PeakGroup.__name__,
                                    num=matching_peakgroups_qs.count(),
                                )

                    elif matching_peakgroups_qs.count() == 0:
                        # The concrete record matches none of the placeholder's peakgroups (i.e. this is either for an
                        # as-yet unanalyzed mzXML file or its data just hasn't been loaded yet).

                        # If there is no existing concrete record
                        if existing_concrete_rec is None:
                            created = True
                            rec = MSRunSample.objects.create(**msrs_rec_dict)
                        else:
                            rec = existing_concrete_rec

                    elif (
                        matching_peakgroups_qs.count() > 0
                        and unmatching_peakgroups_qs.count() > 0
                    ):
                        # Some peak groups linked to the placeholder record appear to match the mzXML file.  Others do
                        # not.

                        # Not setting updated to True, because we're not changing the existing concrete MSRunSample
                        # record.  Instead, we are updating the matching peakgroup records to link to it (insteasd of
                        # (formerly) to the placeholder record).

                        # If there is no existing concrete record
                        if existing_concrete_rec is None:
                            created = True

                            # Create a new concrete MSRunSample record
                            existing_concrete_rec = MSRunSample.objects.create(
                                **msrs_rec_dict
                            )

                        # Now, update the matching peakgroups to link to the concrete record
                        matching_peakgroups_qs.update(
                            msrun_sample=existing_concrete_rec
                        )
                        for pg_rec in matching_peakgroups_qs:
                            pg_rec.full_clean()
                            pg_rec.save()
                        # We are not deleting the placeholder, because it still has some of its own peak groups.

                        rec = existing_concrete_rec

                        self.updated(
                            PeakGroup.__name__, num=matching_peakgroups_qs.count()
                        )

            if created:
                self.created(MSRunSample.__name__)
            elif updated:
                self.updated(MSRunSample.__name__)
            else:
                self.existed(MSRunSample.__name__)

        except Exception as e:
            self.handle_load_db_errors(e, MSRunSample, msrs_rec_dict)
            self.errored(MSRunSample.__name__)
            # Trigger a rollback of this record only
            raise e

        return rec, created, updated

    def get_sample_by_name(self, sample_name, from_mzxml=False):
        """Get a Sample record by name.
        Args:
            sample_name (string)
            from_mzxml (boolean): Whether the sample_name supplied was extracted from an mzXML file name or not (so that
                the error can reference it if not found)
        Exceptions:
            Raises:
                None
            Buffers:
                RecordDoesNotExist
        Returns:
            Optional[Sample]
        """
        rec = None
        try:
            rec = Sample.objects.get(name=sample_name)
        except Sample.DoesNotExist:
            if from_mzxml:
                file = (
                    self.file
                    if self.file is not None
                    else f"the {self.DataSheetName} sheet/file"
                )
                self.aggregated_errors_object.buffer_error(
                    RecordDoesNotExist(
                        Sample,
                        {"name": sample_name},
                        file=file,
                        message=(
                            f"{Sample.__name__} record matching the mzXML file's basename [{sample_name}] "
                            "does not exist.  Please identify the associated sample and add a row with it, the "
                            f"matching mzXML file name(s), and the {self.headers.SEQNAME} to %s."
                        ),
                    )
                )
            else:
                self.aggregated_errors_object.buffer_error(
                    RecordDoesNotExist(
                        Sample,
                        {"name": sample_name},
                        file=self.file,
                        sheet=self.sheet,
                        column=self.headers.SAMPLENAME,
                        rownum=self.rownum,
                    )
                )
        return rec

    @transaction.atomic
    def get_or_create_msrun_sample_from_mzxml(
        self,
        sample,
        msrun_sequence,
        mzxml_metadata,
    ):
        """Takes a sample record, msrun_sequence record, and metadata parsed from the mzxml (including ArchiveFile
        records created from the file) and gets or creates MSRunSample records.

        Note, this will not update PeakGroup records.  Updating PeakGroup records is necessary to keep things in synch
        if PeakGroups from this file were previously added to a placeholder record.

        See get_create_or_update_msrun_sample_from_row to add mzXML files to MSRunSample records while updating
        PeakGroup records.

        This method assumes that either no placeholder MSRunSample record exists or none of the PeakGroup records
        that link to it were confirmed to have matched this mzXML.  A PeakGroup record only matches an mzXML if the Peak
        Annotation Details sheet (i.e. the --infile) annotates the mzXML filename and/or matching peak annotation sample
        header as belonging to a specific peak annotation file (e.g. accucor file) AND the PeakGroup.med_mz falls into
        the scan range parsed from the file.  (NOTE: There is no reliable way to check the polarity associated with a
        PeakGroup record, because that is not reliably recorded in (and cannot be reliably derived from) the
        PeakAnnotation files).

        Args:
            sample (Sample)
            msrun_sequence (MSRunSequence)
            mzxml_metadata (dict): This dict contains metadata parsed from the mzXML file.  Example structure:
                {
                    "polarity": "positive",
                    "mz_min": 1.0,
                    "mz_max": 100.0,
                    "raw_file_name": "sample1.raw",
                    "raw_file_sha1": "KJCWVQUWEKENF",
                    "mzaf_record": mzxml_rec,
                    "rawaf_record": raw_rec,
                    "mzxml_dir": "some/path/to/file",
                    "mzxml_filename": "sample1.mzXML",
                    "added": False,  # This is assumed to be False in this method
                }
        Exceptions:
            Raises:
                None (that are created here)
            Buffers:
                RecordDoesNotExist
        Returns:
            rec (MSRunSample)
            created (boolean)
        """
        if mzxml_metadata["added"] is True or msrun_sequence is None or sample is None:
            self.skipped(MSRunSample.__name__)
            return None, False

        try:
            msrs_rec_dict = {
                "msrun_sequence": msrun_sequence,
                "sample": sample,
                "polarity": mzxml_metadata["polarity"],
                "mz_min": mzxml_metadata["mz_min"],
                "mz_max": mzxml_metadata["mz_max"],
                "ms_raw_file": mzxml_metadata["rawaf_record"],
                "ms_data_file": mzxml_metadata["mzaf_record"],
            }

            rec, created = MSRunSample.objects.get_or_create(**msrs_rec_dict)

            # Update the fact this one has been handled
            mzxml_metadata["added"] = True

            if created:
                self.created(MSRunSample.__name__)
            else:
                self.existed(MSRunSample.__name__)

        except Exception as e:
            self.handle_load_db_errors(e, MSRunSample, msrs_rec_dict)
            self.errored(MSRunSample.__name__)
            # Trigger a rollback of this record
            raise e

        return rec, created

    def get_msrun_sequence(self, name: Optional[str] = None) -> Optional[MSRunSequence]:
        """Retrieves an MSRunSequence record using either the value in the supplied SEQNAME column or via defaults for
        the Sequences sheet.

        The SEQMANE column is a comma-delimited string, which has the following values in this order:
        - Operator
        - LC Protocol Name
        - Instrument
        - Date (in the format YYYY-MM-DD)

        Args:
            name (string): Optional comma-delimited string from the SEQNAME column (if an infile/df was supplied.  If
                None, the default behavior is to use the Defaults file/sheet.
        Exceptions:
            Raises:
                None
            Buffers:
                RequiredColumnValue
                InfileError
        Returns:
            msrseq (Optional[MSRunSequence])
        """
        try:
            if name is not None:
                # If we have a name, that means that the value is from the data sheet (not the defaults file/sheet)
                # Record where any possible errors will come from for the catch below
                file = self.file
                sheet = self.sheet
                column = self.DefaultsHeaders.DEFAULT_VALUE
                rownum = self.rownum

                operator, lcprotname, instrument, date_str = re.split(r",\s*", name)

                date = string_to_datetime(
                    date_str,
                    # The following arguments are for error reporting
                    file=file,
                    sheet=sheet,
                    column=column,
                    rownum=rownum,
                )
            else:
                # Error-reporting info
                if self.df is None:
                    file = (
                        self.defaults_file
                        if self.defaults_file is not None
                        else "the defaults file"
                    )
                    sheet = None
                else:
                    file = self.file
                    sheet = (
                        self.defaults_sheet
                        if self.defaults_file is not None
                        else "the defaults sheet"
                    )
                column = self.DefaultsHeaders.DEFAULT_VALUE
                rownum = None

                if (
                    len(
                        # If all the defaults are set for the sequences loader
                        [
                            v
                            for v in [
                                self.operator_default,
                                self.date_default,
                                self.lc_protocol_name_default,
                                self.instrument_default,
                            ]
                            if v is not None
                        ]
                    )
                    == 4
                ):
                    operator = self.operator_default
                    lcprotname = self.lc_protocol_name_default
                    instrument = self.instrument_default
                    date_str = self.date_default
                    date = string_to_datetime(
                        date_str,
                        # The following arguments are for error reporting
                        file=file,
                        sheet=sheet,
                        column=column,
                        # We didn't save this when reading the defaults sheet, so we're going to name the row by the
                        # sequences loader's date column header
                        rownum=SequencesLoader.DataHeaders.DATE,
                    )
                else:
                    # We didn't save this when reading the defaults sheet, so we're going to name the rows by the
                    # sequences loader's corresponding column headers
                    missing_defaults_header_rows = []
                    if self.operator_default is None:
                        missing_defaults_header_rows.append(
                            SequencesLoader.DataHeaders.OPERATOR
                        )
                    if self.lc_protocol_name_default is None:
                        missing_defaults_header_rows.append(
                            SequencesLoader.DataHeaders.LCNAME
                        )
                    if self.instrument_default is None:
                        missing_defaults_header_rows.append(
                            SequencesLoader.DataHeaders.INSTRUMENT
                        )
                    if self.date_default is None:
                        missing_defaults_header_rows.append(
                            SequencesLoader.DataHeaders.DATE
                        )
                    rownum = ", ".join(
                        [
                            f"{self.DefaultsSheetName} {rowname}"
                            for rowname in missing_defaults_header_rows
                        ]
                    )

                    raise RequiredColumnValues(
                        [
                            RequiredColumnValue(
                                file=file,
                                sheet=sheet,
                                column=column,
                                rownum=f"{self.DefaultsSheetName} {rowname}",
                            )
                            for rowname in missing_defaults_header_rows
                        ]
                    )

            msrseq_query_dict = {
                "researcher": operator,
                "date": date,
                "lc_method__name": lcprotname,
                "instrument": instrument,
            }

            msrseq = MSRunSequence.objects.get(**msrseq_query_dict)

        except Exception as e:
            if isinstance(e, InfileError):
                self.aggregated_errors_object.buffer_error(e)
            elif isinstance(e, MSRunSequence.DoesNotExist):
                seqsource = self.file if self.file is not None else self.defaults_file
                if seqsource is None:
                    seqsource = "the default arguments: [operator, date, instrument, and lc_protocol_name]"
                self.aggregated_errors_object.buffer_error(
                    RecordDoesNotExist(
                        MSRunSequence,
                        msrseq_query_dict,
                        file=seqsource,
                        sheet=self.sheet,
                    )
                )
            else:
                self.aggregated_errors_object.buffer_error(
                    InfileError(
                        f"{type(e).__name__}: {e}",
                        file=file,
                        sheet=sheet,
                        column=column,
                        rownum=rownum,
                    ),
                    orig_exception=e,
                )
            msrseq = None

        return msrseq

    def get_matching_mzxml_metadata(
        self,
        sample_name: str,
        sample_header: Optional[str],
        mzxml_path: Optional[str],
    ) -> Optional[dict]:
        """Identifies and retrieves the mzXML file (and metadata) that matches the row of the Peak Annotation Details
        sheet so that it is associated with the MSRunSample record belonging to the correct MSRunSequence.  To do this,
        it tries looking up the metadata by (in the following order of precedence): mzXML name, sample header, or sample
        name to match a set of files (each with that name).  It then optionally (if there is more than 1 file with the
        same name) matches the path of the mzXML file from the mzXML name column with the actual directory path of the
        actual files.

        Note, the user may not have added a path to the mzXML column, and we don't want to have to force them to fill
        that out, since most times, the name is sufficient.  If no match is found, an error will be printed with the
        available (matching) paths (this will be all files with the same name if no path was supplied).  The user is
        then instructed to edit the mzXML name on the indicated row to include one of the displayed file paths.

        Uses self.mzxml_dict, which contains data parsed from mzXML files indexes by mzXML basename and directory.

        Args:
            sample_name (str): Name of a sample in the database.
            sample_header (Optional[str]): Value header string from a peak annotation file.
            mzxml_path (Optional[str]): Path and/or name of an mzXML file.
        Exceptions:
            None
        Returns:
            Optional[dict]: A single dict of mzXML metadata from self.mzxml_dict[mzxml basename][mzxml dir]
        """
        mzxml_string_dir = ""
        mzxml_name = None
        multiple_mzxml_dict = None

        # If we have an mzXML filename, that trumps any mzxml we might match using the sample header
        if mzxml_path is not None:
            mzxml_string_dir, mzxml_name = os.path.split(mzxml_path)
            mzxml_basename, _ = os.path.splitext(mzxml_name)
            multiple_mzxml_dict = self.mzxml_dict.get(mzxml_basename)

        # If we have a sample_header, that trumps any mzxml we might match using the sample name
        if multiple_mzxml_dict is None:
            multiple_mzxml_dict = self.mzxml_dict.get(sample_header)
            mzxml_basename = str(sample_header)

        # As a last resort, we use the sample name itself
        if multiple_mzxml_dict is None:
            multiple_mzxml_dict = self.mzxml_dict.get(sample_name)
            mzxml_basename = str(sample_name)

        # If not found
        if multiple_mzxml_dict is None or len(multiple_mzxml_dict.keys()) == 0:
            return {
                "polarity": None,
                "mz_min": None,
                "mz_max": None,
                "raw_file_name": None,
                "raw_file_sha1": None,
                "mzaf_record": None,
                "rawaf_record": None,
                "mzxml_dir": None,
                "mzxml_filename": None,
                "added": False,
            }

        # Now we have a multiple_mzxml_dict with potentially multiple mzXML files' metadata
        # If there's only 1, return it:
        if len(multiple_mzxml_dict.keys()) == 1:  # The keys are directories
            single_dir_key = list(multiple_mzxml_dict.keys())[0]
            single_dir_list_of_dicts = multiple_mzxml_dict[single_dir_key]
            if len(single_dir_list_of_dicts) == 1:
                return single_dir_list_of_dicts[0]

        # Otherwise, we need to try and match it using the directory portion of the mzXML file reported in the infile.
        # We need to do this in order to match the sequence.  There may be multiple sequences in the infile.
        matches = []
        # For every directory key of metadata for files with the current (base)name
        for real_dir in multiple_mzxml_dict.keys():
            # If the directory parsed from the mzXML in the infile is a portion of the path of the real file
            if mzxml_string_dir in real_dir:
                # The number of matching metadata dicts is the number of ArchiveFile primary keys (with the same
                # name and directory) (this is mainly for the case that a directory path wasn't provided in the
                # mzXML name column in the infile)
                matches.extend(multiple_mzxml_dict[real_dir])

        if len(matches) > 1:
            match_files = "\n\t".join(
                [
                    # Not using dr["mzaf_record"].file_location because Django appends a randomized hash string
                    os.path.join(dr["mzxml_dir"], dr["mzxml_filename"])
                    for dr in matches
                ]
            )
            self.aggregated_errors_object.buffer_warning(
                InfileError(
                    (
                        f"Multiple mzXML files with the same basename [{mzxml_basename}] match row {self.rownum}:\n"
                        f"\t{match_files}\n"
                        "The default MSRunSequence will be used if provided.  If this is followed by an error "
                        "requiring defaults to be supplied, add one of the paths of the above files to the "
                        f"{self.defaults.MZXMLNAME} column in %s."
                    ),
                    file=self.file,
                    sheet=self.sheet,
                    rownum=self.rownum,
                )
            )
            return None

        return matches[0]

    def separate_placeholder_peak_groups(
        self,
        rec_dict,
        annot_name,
        rec,
    ):
        """This method retrieves peakgroups from the provided MSRunSample record and separates them based on the peak
        annotation file name (annot_name) the dict is associated with compared with the one in the PeakGroup record.

        Note that technically (though not in practice), multiple different mzXML files for the same sample/sequence can
        be used in a single peak annotation file.  One peak group could originate from one such mzXML file and the other
        from another mzXML file, so mz_min and mz_max are used as an additional check, however, since polarity isn't
        saved in the peak data from the peak annotation file data, this is not perfect.  Thus, this method could
        associate some peak groups with the wrong mzXML file.  It's not perfect.

        It error-checks that the msrun_sequence and sample in both the rec_dict and the rec are the same.

        The basic idea is that the rec_dict represents a proposed/potential MSRunSample record that was found in a row
        of the Peak Annotation Details sheet and that the supplied rec is an MSRunSample record that is a placeholder
        that has various PeakGroup records linked to it, that may have originated from the mzXML file that is being
        added in the rec_dict.  This method compares the metadata in the PeakGroup's data with the annot_name and the
        metadata extracted from the mzXML file that populated the rec_dict.  While this metadata contains polarity and
        the polarity was available in the peak annotation file (inferred from the adductName of the PARENT), those
        values are/were not saved in the peak data.  So if the annot_name is the same and the medMZ is between mz_min
        and mz_max, then the peak group matches the new file.  The peak group records linked to the placeholder record
        are thus returned in 2 groups: matching and unmatching.

        Note that since scan ranges can overlap, this could also work with a non-placeholder record to shift peak groups
        from one to the other MSRunSample record..

        The record represented by rec_dict is assumed to be different from the supplied record.

        Args:
            rec_dict (dict of MSRunSample field values): Field value pairs of an MSRunSample record that may or may not
                exist in the database, WITH values for polarity, mz_min, and mz_max.
            rec (MSRunSample): An MSRunSample record.  This is intended to be a placeholder record without an
                ms_data_file value, but either way, the metadata (ms_data_file, polarity, mz_min, and mz_max) in the
                record is ignored.
        Exceptions:
            None
        Returns:
            matching (QuerySet of PeakGroup records)
            unmatching (QuerySet of PeakGroup records)
        """
        # Do not associate any peak group records with the proposed record in rec_dict, if there is no associated
        # annot_name.
        if annot_name is None:
            return PeakGroup.objects.none(), rec.peak_groups.all()

        # Make sure that the sequence and sample are the same
        if rec_dict["sample"] != rec.sample:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    "separate_placeholder_peak_groups called with MSRunSample record data not from the same sample: "
                    f"{rec_dict['sample']} (ID: {rec_dict['sample'].id}) != {rec.sample} (ID: {rec.sample.id})"
                )
            )
            # All peak groups are unmatching
            return PeakGroup.objects.none(), rec.peak_groups.all()
        elif rec_dict["msrun_sequence"] != rec.msrun_sequence:
            self.aggregated_errors_object.buffer_error(
                ValueError(
                    "separate_placeholder_peak_groups called with MSRunSample record data not from the same "
                    f"MSRunSequence: {rec_dict['msrun_sequence']} (ID: {rec_dict['msrun_sequence'].id}) != "
                    f"{rec.msrun_sequence} (ID: {rec.msrun_sequence.id})"
                )
            )
            # All peak groups are unmatching
            return PeakGroup.objects.none(), rec.peak_groups.all()

        # Annotate the peak groups with the min and max med_mz of its peak_data
        qs = PeakGroup.objects.annotate(
            min_med_mz=Min("peak_data__med_mz", default=None),
            max_med_mz=Max("peak_data__med_mz", default=None),
        )

        # Filtering criteria
        matching_q_exp = (
            Q(peak_annotation_file__filename=annot_name)
            & Q(Q(min_med_mz__isnull=True) | Q(min_med_mz__gte=rec_dict["mz_min"]))
            & Q(Q(max_med_mz__isnull=True) | Q(max_med_mz__lte=rec_dict["mz_max"]))
        )

        # Generate the matching and unmatching PeakGroup QuerySets
        matching_qs = qs.filter(matching_q_exp)
        unmatching_qs = qs.filter(~matching_q_exp)

        # Clear out the annotations so that .update can be called on the querysets
        matching_qs.query.annotations.clear()
        unmatching_qs.query.annotations.clear()

        return matching_qs, unmatching_qs

    @classmethod
    def parse_mzxml(cls, mzxml_path, full_dict=False):
        """Creates a dict of select data parsed from an mzXML file

        This extracts the raw file name, raw file's sha1, and the polarity from an mzxml file and returns a condensed
        dictionary of only those values (for simplicity).  The construction of the condensed dict will perform
        validation and conversion of the desired values, which will not occur when the full_dict is requested.  If
        full_dict is True, it will return the uncondensed version.

        If not all polarities of all the scans are the same, an error will be buffered.

        Args:
            mzxml_path (str or Path): mzXML file path
            full_dict (boolean): Whether to return the raw/full dict of the mzXML file

        Exceptions:
            Raises:
                None
            Buffers:
                ValueError

        Returns:
            If mzxml_path is not a real existing file:
                None
            If full_dict=False:
                {
                    "raw_file_name": <raw file base name parsed from mzXML file>,
                    "raw_file_sha1": <sha1 string parsed from mzXML file>,
                    "polarity": "positive" or "negative" (based on first polarity parsed from mzXML file),
                    "mz_min": <float parsed from lowMz from the mzXML file>,
                    "mz_max": <float parsed from highMz from the mzXML file>,
                }
            If full_dict=True:
                xmltodict.parse(xml_content)
        """
        # In order to use this as a class method, we will buffer the errors in a one-off AggregatedErrors object
        errs_buffer = AggregatedErrors()

        # Assume Path object
        mzxml_path_obj = mzxml_path
        if isinstance(mzxml_path, str):
            mzxml_path_obj = Path(mzxml_path)

        if not mzxml_path_obj.is_file():
            # mzXML files are optional, but the file names are supplied in a file, in which case, we may have a name,
            # but not the file, so just return None if what we have isn't a real file.
            return None

        # Parse the xml content
        with mzxml_path_obj.open(mode="r") as f:
            xml_content = f.read()
        mzxml_dict = xmltodict.parse(xml_content)

        if full_dict:
            return mzxml_dict

        try:
            raw_file_type = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileType"]
            raw_file_name = Path(
                mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileName"]
            ).name
            raw_file_sha1 = mzxml_dict["mzXML"]["msRun"]["parentFile"]["@fileSha1"]
            if raw_file_type != "RAWData":
                errs_buffer.buffer_error(
                    ValueError(
                        f"Unsupported file type [{raw_file_type}] encountered in mzXML file [{str(mzxml_path_obj)}].  "
                        "Expected: [RAWData]."
                    )
                )
                raw_file_name = None
                raw_file_sha1 = None

            polarity = MSRunSample.POLARITY_DEFAULT
            mz_min = None
            mz_max = None
            symbol_polarity = ""
            mixed_polarities = {}
            for entry_dict in mzxml_dict["mzXML"]["msRun"]["scan"]:
                # Parse the mz_min
                tmp_mz_min = float(entry_dict["@lowMz"])
                # Get the min of the mins
                if mz_min is None or tmp_mz_min < mz_min:
                    mz_min = tmp_mz_min
                # Parse the mz_max
                tmp_mz_max = float(entry_dict["@highMz"])
                # Get the max of the maxes
                if mz_max is None or tmp_mz_max > mz_max:
                    mz_max = tmp_mz_max
                # Parse the polarity
                # If we haven't run into a polarity conflict (yet)
                if str(mzxml_path_obj) not in mixed_polarities.keys():
                    if symbol_polarity == "":
                        symbol_polarity = entry_dict["@polarity"]
                    elif symbol_polarity != entry_dict["@polarity"]:
                        mixed_polarities[str(mzxml_path_obj)] = {
                            "first": symbol_polarity,
                            "different": entry_dict["@polarity"],
                            "scan": entry_dict["@num"],
                        }
            if len(mixed_polarities.keys()) > 0:
                errs_buffer.buffer_exception(
                    MixedPolarityErrors(mixed_polarities),
                )
        except KeyError as ke:
            errs_buffer.buffer_error(MzxmlParseError(str(ke)))
        if symbol_polarity == "+":
            polarity = MSRunSample.POSITIVE_POLARITY
        elif symbol_polarity == "-":
            polarity = MSRunSample.NEGATIVE_POLARITY
        elif symbol_polarity != "":
            errs_buffer.buffer_error(
                ValueError(
                    f"Unsupported polarity value [{symbol_polarity}] encountered in mzXML file "
                    f"[{str(mzxml_path_obj)}]."
                )
            )

        return {
            "raw_file_name": raw_file_name,
            "raw_file_sha1": raw_file_sha1,
            "polarity": polarity,
            "mz_min": mz_min,
            "mz_max": mz_max,
        }, errs_buffer

    def leftover_mzxml_files_exist(self):
        """Traverse self.mzxml_dict and return True if any mzXML files have not yet been added to an MSRunSample record
        (meaning, it was not listed in the Peak Annotation Details sheet/file).

        This method exists in order to avoid errors when trying to retrieve default values, if they are not needed, e.g.
        when the infile is complete and all mzXMLs were included in it.

        Args:
            None
        Exceptions:
            None
        Returns
            boolean
        """
        for mzxml_basename in self.mzxml_dict.keys():
            for mzxml_dir in self.mzxml_dict[mzxml_basename].keys():
                for mzxml_metadata in self.mzxml_dict[mzxml_basename][mzxml_dir]:
                    if mzxml_metadata["added"] is False:
                        return True
        return False

    @classmethod
    def get_suffix_pattern(cls, suffix_patterns=None, add_patterns=True):
        """Create a regular expression that can be used to strip chained suffixes from a sample header.
        Args:
            suffix_patterns (list of regular expression strings)
            add_patterns (boolean): Whether to add the supplied patterns to the defaults or replace them.
        Exceptions:
            None
        Returns:
            pattern (compiled re)
        """
        suffixes = cls.DEFAULT_SAMPLE_HEADER_SUFFIXES
        if suffix_patterns is not None:
            if add_patterns:
                suffixes.extend(suffix_patterns)
            else:
                suffixes = suffix_patterns
        return re.compile(r"(" + "|".join(suffixes) + r")+$")

    @classmethod
    def guess_sample_name(cls, mzxml_bamename, suffix_patterns=None, add_patterns=True):
        """Strips suffixes from an accucor/isocorr sample header (or mzXML file basename) using
        self.DEFAULT_SAMPLE_HEADER_SUFFIXES and/or the supplied suffixes.  The result is usually the name of the sample
        as it appears in the database.

        Use caution.  This doesn't guarantee the resulting sample name is accurate.  If the resulting sample name is not
        unique, you may end up with conflict errors at some later point in the processing of a study submission.  To
        resolve this, you must add the header/mzXML to the input file associated with the correct database sample name
        (containing any required prefix).

        Args:
            mzxml_bamename (string): The basename of an mzXML file (or accucor/isocorr sample header.
            suffix_patterns (list of regular expression strings)
            add_patterns (boolean): Whether to add the supplied patterns to the defaults or replace them.
        Exceptions:
            None
        Returns:
            guessed_sample_name (string)
        """
        pattern = cls.get_suffix_pattern(
            suffix_patterns=suffix_patterns, add_patterns=add_patterns
        )
        return re.sub(pattern, "", mzxml_bamename)
