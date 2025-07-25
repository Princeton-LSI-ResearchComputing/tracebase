from collections import defaultdict, namedtuple
from typing import Dict, Optional

from django.db import transaction
from django.db.utils import IntegrityError

from DataRepo.loaders.base.table_column import ColumnReference, TableColumn
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.models import Compound, CompoundSynonym
from DataRepo.utils.exceptions import (
    CompoundExistsAsMismatchedSynonym,
    DuplicateValues,
    ProhibitedCompoundName,
    ProhibitedStringValue,
    RollbackException,
    SynonymExistsAsMismatchedCompound,
)


class CompoundsLoader(TableLoader):
    """
    Load the Compound and CompoundSynonym tables
    """

    # Header keys (for convenience use only).  Note, they cannot be used in the namedtuple() call.  Literal required.
    NAME_KEY = "NAME"
    HMDBID_KEY = "HMDB_ID"
    FORMULA_KEY = "FORMULA"
    SYNONYMS_KEY = "SYNONYMS"

    SYNONYMS_DELIMITER = Compound.delimiter

    DataSheetName = "Compounds"

    # The tuple used to store different kinds of data per column at the class level
    DataTableHeaders = namedtuple(
        "DataTableHeaders",
        [
            "NAME",
            "HMDB_ID",
            "FORMULA",
            "SYNONYMS",
        ],
    )

    # The default header names (which can be customized via yaml file via the corresponding load script)
    DataHeaders = DataTableHeaders(
        NAME="Compound",
        HMDB_ID="HMDB ID",
        FORMULA="Formula",
        SYNONYMS="Synonyms",
    )

    # List of required header keys
    DataRequiredHeaders = [NAME_KEY, HMDBID_KEY, FORMULA_KEY]

    # List of header keys for columns that require a value
    DataRequiredValues = DataRequiredHeaders

    # No DataDefaultValues needed

    DataColumnTypes: Dict[str, type] = {
        NAME_KEY: str,
        HMDBID_KEY: str,
        FORMULA_KEY: str,
        SYNONYMS_KEY: str,
    }

    # Combinations of columns whose values must be unique in the file
    DataUniqueColumnConstraints = [[NAME_KEY], [HMDBID_KEY]]

    # A mapping of database field to column.  Only set when the mapping is 1:1.  Omit others.
    FieldToDataHeaderKey = {
        Compound.__name__: {
            "name": NAME_KEY,
            "hmdb_id": HMDBID_KEY,
            "formula": FORMULA_KEY,
            "synonyms": SYNONYMS_KEY,
        },
        CompoundSynonym.__name__: {
            "name": SYNONYMS_KEY,
            "compound": NAME_KEY,
        },
    }

    # No FieldToDataValueConverter needed

    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(
            field=Compound.name,
            name=DataHeaders.NAME,
            # TODO: Replace sheet/header strings with references once the circular import issue is figured out
            reference=ColumnReference(
                sheet="Tracers",
                header="Compound",
            ),
        ),
        HMDB_ID=TableColumn.init_flat(field=Compound.hmdb_id, name=DataHeaders.HMDB_ID),
        FORMULA=TableColumn.init_flat(field=Compound.formula, name=DataHeaders.FORMULA),
        SYNONYMS=TableColumn.init_flat(
            field=CompoundSynonym.name,
            name=DataHeaders.SYNONYMS,
            header_required=True,
            value_required=False,
            format=f"List of compound synonyms delimited by '{SYNONYMS_DELIMITER}'.",
        ),
    )

    # List of model classes that the loader enters records into.  Used for summarized results & some exception handling
    Models = [Compound, CompoundSynonym]

    name_fix_suggestion = (
        f"You may choose to manually edit the automatically fixed compound name in the '{DataHeaders.NAME}' and/or "
        f"'{DataHeaders.SYNONYMS}' columns of the '{DataSheetName}' sheet, but be sure to also fix any occurrences in "
        "any peak annotation files as well."
    )

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
                synonyms_delimiter (Optional[str]) [;]: Synonym string delimiter.
        Exceptions:
            None
        Returns:
            None
        """
        self.synonyms_delimiter = kwargs.pop(
            "synonyms_delimiter", self.SYNONYMS_DELIMITER
        )
        super().__init__(*args, **kwargs)

    def load_data(self):
        """Loads the tissue table from the dataframe.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        # TableLoader doesn't handle parsing column values like the delimited synonyms column, so we need to check
        # it explicitly in this derived class.
        self.check_for_cross_column_name_duplicates()

        for _, row in self.df.iterrows():
            # check_for_cross_column_name_duplicates can add to the skip row indexes
            # We didn't call get_row_val, so in order to know to skip, we must supply the row index (in row.name)
            if self.is_skip_row(row.name):
                self.errored(Compound.__name__)
                # The synonym errored count will be inaccurate.  If there was an error reading or parsing, we don't
                # know how many there are
                self.skipped(CompoundSynonym.__name__)
                continue

            try:
                cmpd_rec = self.get_or_create_compound(row)
            except RollbackException:
                # Exception handling was handled in get_or_create_*
                # Continue processing rows to find more errors
                cmpd_rec = None

            synonyms = self.parse_synonyms(self.get_row_val(row, self.headers.SYNONYMS))

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row() or cmpd_rec is None:
                # The count will be inaccurate.  If there was an error reading or parsing, we don't know how many
                # there are
                self.errored(CompoundSynonym.__name__)
                continue

            for synonym in synonyms:
                try:
                    self.get_or_create_synonym(synonym, cmpd_rec)
                except RollbackException:
                    # Exception handling was handled in get_or_create_*
                    # Continue processing rows to find more errors
                    pass

    @transaction.atomic
    def get_or_create_compound(self, row):
        """Get or create a compound record.
        Args:
            row (pandas dataframe row)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            rec (Optional[Compound])
        """
        rec_dict = None
        rec = None

        try:
            name = self.get_row_val(row, self.headers.NAME)
            formula = self.get_row_val(row, self.headers.FORMULA)
            hmdb_id = self.get_row_val(row, self.headers.HMDB_ID)

            try:
                Compound.validate_compound_name(name)
            except ProhibitedStringValue as pc:
                name = Compound.validate_compound_name(name, fix=True)
                self.aggregated_errors_object.buffer_warning(
                    ProhibitedCompoundName(
                        pc.found,
                        value=pc.value,
                        file=self.friendly_file,
                        sheet=self.sheet,
                        column=self.headers.NAME,
                        rownum=self.rownum,
                        suggestion=self.name_fix_suggestion,
                    ),
                    is_fatal=self.validate,
                    orig_exception=pc,
                )

            rec_dict = {
                "name": name,
                "formula": formula,
                "hmdb_id": hmdb_id,
            }

            # get_row_val can add to skip_row_indexes when there is a missing required value
            if self.is_skip_row():
                self.errored(Compound.__name__)
                return rec

            rec, created = Compound.objects.get_or_create(**rec_dict)

            if created:
                rec.full_clean()
                self.created(Compound.__name__)
            else:
                self.existed(Compound.__name__)

        except Exception as e:
            if isinstance(e, IntegrityError) and "DataRepo_compoundsynonym_pkey" in str(
                e
            ):
                # This is caused by trying to create a synonym that is already associated with a different compound
                # We want a better error to describe this situation than we would get from handle_load_db_errors
                try:
                    self.aggregated_errors_object.buffer_error(
                        CompoundExistsAsMismatchedSynonym(
                            rec_dict["name"],
                            rec_dict,
                            CompoundSynonym.objects.get(name__exact=rec_dict["name"]),
                        ),
                        orig_exception=e,
                    )
                except Exception:
                    self.handle_load_db_errors(e, Compound, rec_dict)
            else:
                self.handle_load_db_errors(e, Compound, rec_dict)
            self.errored(Compound.__name__)
            raise RollbackException()

        return rec

    @transaction.atomic
    def get_or_create_synonym(self, synonym, cmpd_rec):
        """Get or create a compound synonym record.
        Args:
            synonym (string)
            cmpd_rec (Compound)
        Exceptions:
            Raises:
                RollbackException
            Buffers:
                None
        Returns:
            None
        """
        rec_dict = None
        try:
            rec_dict = {
                "name": synonym,
                "compound": cmpd_rec,
            }

            syn_rec, created = CompoundSynonym.objects.get_or_create(**rec_dict)

            if created:
                syn_rec.full_clean()
                self.created(CompoundSynonym.__name__)
            else:
                self.existed(CompoundSynonym.__name__)

        except SynonymExistsAsMismatchedCompound as seamc:
            self.aggregated_errors_object.buffer_error(seamc)
            self.errored(CompoundSynonym.__name__)
            raise RollbackException()
        except Exception as e:
            self.handle_load_db_errors(e, CompoundSynonym, rec_dict)
            self.errored(CompoundSynonym.__name__)
            raise RollbackException()

    def parse_synonyms(self, synonyms_string: Optional[str]) -> list:
        """Parse the synonyms column value using the self.synonyms_delimiter.

        Args:
            synonyms_string (Optional[str]): String of delimited synonyms
        Exceptions:
            None
        Returns:
            synonyms (list of strings)
        """
        if synonyms_string is None:
            return []
        synonyms = []
        if synonyms_string:
            for synonym in synonyms_string.split(self.synonyms_delimiter):
                try:
                    Compound.validate_compound_name(synonym)
                except ProhibitedStringValue as pc:
                    synonym = Compound.validate_compound_name(synonym, fix=True)
                    self.aggregated_errors_object.buffer_warning(
                        ProhibitedCompoundName(
                            pc.found,
                            value=pc.value,
                            file=self.friendly_file,
                            sheet=self.sheet,
                            column=self.headers.SYNONYMS,
                            rownum=self.rownum,
                            suggestion=self.name_fix_suggestion,
                        ),
                        is_fatal=self.validate,
                        orig_exception=pc,
                    )
                synonym = synonym.strip()
                if synonym != "":
                    synonyms.append(synonym)

        return synonyms

    def check_for_cross_column_name_duplicates(self):
        """Look for duplicates between compound name and synonym on different rows.

        Args:
            None
        Exceptions:
            Raises:
                None
            Buffered:
                DuplicateValues
        Returns:
            None
        """
        # Create a dict to track what names/synonyms occur on which rows
        namesyn_dict = defaultdict(lambda: defaultdict(list))
        syn_dict = defaultdict(list)
        for index, row in self.df.iterrows():
            # Explicitly not skipping rows with duplicates
            name = self.get_row_val(row, self.headers.NAME)
            namesyn_dict[name]["name"].append(index)

            synonyms = self.parse_synonyms(self.get_row_val(row, self.headers.SYNONYMS))
            for synonym in synonyms:
                # A synonym that is exactly the same as its compound name is skipped in the load
                if synonym != name:
                    namesyn_dict[synonym]["synonym"].append(index)
                syn_dict[synonym].append(index)

        # We need to check the synonyms column individually, because the standard unique constraints check of
        # TableLoader only looks at the entire column value, not the delimited/parsed values
        syn_dupe_dict = defaultdict(list)
        for syn in syn_dict.keys():
            if len(syn_dict[syn]) > 1:
                syn_dupe_dict[syn] = syn_dict[syn]
                self.add_skip_row_index(index_list=syn_dict[syn])
        if len(syn_dupe_dict.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                DuplicateValues(
                    syn_dupe_dict,
                    [self.headers.SYNONYMS],
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
            )

        # Build a dupe dict for only cross-column duplicates
        cross_dupe_dict = defaultdict(list)
        for namesyn in namesyn_dict.keys():
            if len(namesyn_dict[namesyn].keys()) > 1:
                idxs = []
                for idx in namesyn_dict[namesyn]["name"]:
                    if idx not in idxs:
                        idxs.append(idx)
                for idx in namesyn_dict[namesyn]["synonym"]:
                    if idx not in idxs:
                        idxs.append(idx)
                cross_dupe_dict[namesyn].extend(idxs)
                self.add_skip_row_index(index_list=namesyn_dict[namesyn])
        if len(cross_dupe_dict.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                DuplicateValues(
                    cross_dupe_dict,
                    [f"{self.headers.NAME} and {self.headers.SYNONYMS}"],
                    addendum=(
                        f"Note, no 2 rows are allowed to have a {self.headers.NAME} or {self.headers.SYNONYMS} in "
                        "common."
                    ),
                    sheet=self.sheet,
                    file=self.friendly_file,
                )
            )
