from collections import defaultdict

from django.db.utils import IntegrityError

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.utils.exceptions import (
    CompoundExistsAsMismatchedSynonym,
    DuplicateValues,
    SynonymExistsAsMismatchedCompound,
)
from DataRepo.utils.loader import TraceBaseLoader


class CompoundsLoader(TraceBaseLoader):
    """
    Load the Compound and CompoundSynonym tables
    """

    # Define the dataframe key names and requirements
    NAME_HEADER = "Compound"
    HMDB_ID_HEADER = "HMDB ID"
    FORMULA_HEADER = "Formula"
    SYNONYMS_HEADER = "Synonyms"
    REQUIRED_HEADERS = [NAME_HEADER, FORMULA_HEADER, HMDB_ID_HEADER]
    REQUIRED_VALUES = REQUIRED_HEADERS
    ALL_HEADERS = [NAME_HEADER, FORMULA_HEADER, HMDB_ID_HEADER, SYNONYMS_HEADER]
    UNIQUE_COLUMN_CONSTRAINTS = [[NAME_HEADER], [HMDB_ID_HEADER]]
    COMPOUND_FLD_TO_COL = {
        "name": NAME_HEADER,
        "hmdb_id": HMDB_ID_HEADER,
        "formula": FORMULA_HEADER,
        "synonyms": SYNONYMS_HEADER,
    }
    SYNONYM_FLD_TO_COL = {
        "name": SYNONYMS_HEADER,
        "compound": NAME_HEADER,
    }

    def __init__(
        self,
        compounds_df,
        synonym_separator=";",
        dry_run=False,
        defer_rollback=False,  # DO NOT USE MANUALLY - THIS WILL NOT ROLL BACK (handle in atomic transact in caller)
        sheet=None,
        file=None,
    ):
        # Data
        self.compounds_df = compounds_df
        self.synonym_separator = synonym_separator

        super().__init__(
            compounds_df,
            all_headers=self.ALL_HEADERS,
            reqd_headers=self.REQUIRED_HEADERS,
            reqd_values=self.REQUIRED_VALUES,
            unique_constraints=self.UNIQUE_COLUMN_CONSTRAINTS,
            dry_run=dry_run,
            defer_rollback=defer_rollback,
            sheet=sheet,
            file=file,
            models=[Compound, CompoundSynonym],
        )

    def parse_synonyms(self, synonyms_string: str) -> list:
        synonyms = []
        if synonyms_string:
            synonyms = [
                synonym.strip()
                for synonym in synonyms_string.split(self.synonym_separator)
                if synonym.strip() != ""
            ]
        return synonyms

    def check_for_cross_column_name_duplicates(self):
        """
        This method looks for duplicates between compound name and synonym on different rows.
        """
        # Create a dict to track what names/synonyms occur on which rows
        namesyn_dict = defaultdict(lambda: defaultdict(list))
        syn_dict = defaultdict(list)
        for index, row in self.compounds_df.iterrows():
            # Explicitly not skipping rows with duplicates
            name = self.getRowVal(row, self.NAME_HEADER)
            namesyn_dict[name]["name"].append(index)

            synonyms = self.parse_synonyms(self.getRowVal(row, self.SYNONYMS_HEADER))
            for synonym in synonyms:
                # A synonym that is exactly the same as its compound name is skipped in the load
                if synonym != name:
                    namesyn_dict[synonym]["synonym"].append(index)
                syn_dict[synonym].append(index)

        # We need to check the synonyms column individually, because the standard unique constraints check of
        # TraceBaseLoader only looks at the entire column value, not the delimited/parsed values
        syn_dupe_dict = defaultdict(list)
        for syn in syn_dict.keys():
            if len(syn_dict[syn]) > 1:
                syn_dupe_dict[syn] = syn_dict[syn]
        if len(syn_dupe_dict.keys()) > 0:
            self.aggregated_errors_object.buffer_error(
                DuplicateValues(
                    syn_dupe_dict,
                    [self.SYNONYMS_HEADER],
                    sheet=self.sheet,
                    file=self.file,
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
                    [f"{self.NAME_HEADER} and {self.SYNONYMS_HEADER}"],
                    addendum=(
                        f"Note, no 2 rows are allowed to have a {self.NAME_HEADER} or {self.SYNONYMS_HEADER} in "
                        "common."
                    ),
                    sheet=self.sheet,
                    file=self.file,
                )
            )

    @TraceBaseLoader.loader
    def load_compound_data(self):
        self.check_for_cross_column_name_duplicates()

        for index, row in self.compounds_df.iterrows():
            # Don't attempt load of rows where there are cross-references between compound names and synonyms or
            # missing required values
            if index in self.get_skip_row_indexes():
                continue

            # Index starts at 0, headers are on row 1
            rownum = index + 2

            name = self.getRowVal(row, self.NAME_HEADER)
            formula = self.getRowVal(row, self.FORMULA_HEADER)
            hmdb_id = self.getRowVal(row, self.HMDB_ID_HEADER)

            try:
                cmpd_recdict = {
                    "name": name,
                    "formula": formula,
                    "hmdb_id": hmdb_id,
                }

                cmpd_rec, cmpd_created = Compound.objects.get_or_create(**cmpd_recdict)

                if cmpd_created:
                    cmpd_rec.full_clean()
                    self.created(Compound.__name__)
                else:
                    self.existed(Compound.__name__)
            except Exception as e:
                if isinstance(
                    e, IntegrityError
                ) and "DataRepo_compoundsynonym_pkey" in str(e):
                    # This is caused by trying to create a synonym that is already associated with a different compound
                    # We want a better error to describe this situation than we would get from handle_load_db_errors
                    self.aggregated_errors_object.buffer_error(
                        CompoundExistsAsMismatchedSynonym(
                            name,
                            cmpd_recdict,
                            CompoundSynonym.objects.get(name__exact=name),
                        )
                    )
                else:
                    self.handle_load_db_errors(
                        e,
                        model=Compound,
                        rec_dict=cmpd_recdict,
                        rownum=rownum,
                        fld_to_col=self.COMPOUND_FLD_TO_COL,
                    )
                self.errored(Compound.__name__)

            synonyms = self.parse_synonyms(self.getRowVal(row, self.SYNONYMS_HEADER))

            for synonym in synonyms:
                try:
                    syn_recdict = {
                        "name": synonym,
                        "compound": cmpd_rec,
                    }

                    syn_rec, syn_created = CompoundSynonym.objects.get_or_create(
                        **syn_recdict
                    )

                    if syn_created:
                        syn_rec.full_clean()
                        self.created(CompoundSynonym.__name__)
                    else:
                        self.existed(CompoundSynonym.__name__)
                except SynonymExistsAsMismatchedCompound as seamc:
                    self.aggregated_errors_object.buffer_error(seamc)
                except Exception as e:
                    self.handle_load_db_errors(
                        e,
                        model=CompoundSynonym,
                        rec_dict=syn_recdict,
                        rownum=rownum,
                        fld_to_col=self.SYNONYM_FLD_TO_COL,
                    )
                    self.errored(CompoundSynonym.__name__)
