from collections import defaultdict

from django.db import transaction
from django.db.utils import IntegrityError

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.models.compound import (
    CompoundExistsAsMismatchedSynonym,
    SynonymExistsAsMismatchedCompound,
)
from DataRepo.utils.exceptions import (
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    DuplicateValues,
    RequiredHeadersError,
    RequiredValuesError,
    UnknownHeadersError,
)


class CompoundsLoader:
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

    def __init__(
        self,
        compounds_df,
        synonym_separator=";",
        validate=False,
        dry_run=False,
    ):
        self.compounds_df = compounds_df
        self.synonym_separator = synonym_separator
        self.bad_row_indexes = []
        self.num_inserted_compounds = 0
        self.num_existing_compounds = 0
        self.num_erroneous_compounds = 0
        self.num_inserted_synonyms = 0
        self.num_existing_synonyms = 0
        self.num_erroneous_synonyms = 0
        self.dry_run = dry_run
        self.validate = validate

        self.aggregated_errors_obj = AggregatedErrors()

    def check_headers(self):
        unknown_headers = []
        for file_header in self.compounds_df.columns:
            if file_header not in self.ALL_HEADERS:
                unknown_headers.append(file_header)
        if len(unknown_headers) > 0:
            self.aggregated_errors_obj.buffer_error(
                UnknownHeadersError(unknown_headers)
            )

        missing_headers = []
        for rqd_header in self.REQUIRED_HEADERS:
            if rqd_header not in self.compounds_df.columns:
                missing_headers.append(rqd_header)
        if len(missing_headers) > 0:
            # Cannot proceed, so not buffering
            raise RequiredHeadersError(missing_headers)

    def parse_synonyms(self, synonyms_string: str) -> list:
        synonyms = []
        if synonyms_string:
            synonyms = [
                synonym.strip()
                for synonym in synonyms_string.split(self.synonym_separator)
                if synonym.strip() != ""
            ]
        return synonyms

    def load_compounds(self):
        with transaction.atomic():
            try:
                self.validate_infile_data()
                self._load_data()
            except Exception as e:
                self.aggregated_errors_obj.buffer_error(e)

            # If there are buffered errors
            if self.aggregated_errors_obj.should_raise():
                raise self.aggregated_errors_obj

            if self.dry_run:
                # Roll back everything
                raise DryRun()

    def validate_infile_data(self):
        # Check the headers
        self.check_headers()
        self.check_required_values()
        # Check for in-file name/synonym duplicates
        self.check_for_cross_column_name_duplicates()
        # Required values are checked during the load loop

    def check_required_values(self):
        # Checking for required values in the context of the file data instead of upon insert allows us to report the
        # rows where values are missing and allows us to avoid indirect errors about unique constrain violations, etc.
        missing_rqd_values = defaultdict(list)
        for index, row in self.compounds_df.iterrows():
            for header in self.REQUIRED_VALUES:
                val = self.getRowVal(row, header)
                if val is None and header in self.REQUIRED_VALUES:
                    missing_rqd_values[header].append(index)
                    if index not in self.bad_row_indexes:
                        self.bad_row_indexes.append(index)
        if len(missing_rqd_values.keys()) > 0:
            self.aggregated_errors_obj.buffer_error(
                RequiredValuesError(missing_rqd_values)
            )

    def check_for_cross_column_name_duplicates(self):
        """
        This method looks for duplicates between compound name and synonym on different rows.
        """
        # Create a dict to track what names/synonyms occur on which rows
        namesyn_dict = defaultdict(list)
        for index, row in self.compounds_df.iterrows():
            # Explicitly not skipping rows with duplicates
            name = self.getRowVal(row, self.NAME_HEADER)
            namesyn_dict[name].append(index)

            synonyms = self.parse_synonyms(self.getRowVal(row, self.SYNONYMS_HEADER))
            for synonym in synonyms:
                if synonym != name:
                    namesyn_dict[synonym].append(index)
        # Build a dupe dict to supply to the DuplicateValues exception
        dupe_dict = defaultdict(list)
        for namesyn in namesyn_dict.keys():
            if len(namesyn_dict[namesyn]) > 1:
                dupe_dict[namesyn] = namesyn_dict[namesyn]
                for idx in namesyn_dict[namesyn]:
                    if idx not in self.bad_row_indexes:
                        self.bad_row_indexes.append(idx)
        # If there are duplicates, report them
        if len(dupe_dict.keys()) > 0:
            self.aggregated_errors_obj.buffer_error(
                DuplicateValues(
                    dupe_dict,
                    [self.NAME_HEADER, self.SYNONYMS_HEADER],
                    addendum=(
                        f"Note, no 2 rows are allowed to have a {self.NAME_HEADER} or {self.SYNONYMS_HEADER} in "
                        "common."
                    ),
                )
            )

    def getRowVal(self, row, header):
        val = None

        if header in row:
            val = row[header]
            # This will make later checks of values easier
            if val == "":
                val = None

        return val

    def _load_data(self):
        for index, row in self.compounds_df.iterrows():
            if index in self.bad_row_indexes:
                # Don't attempt load of rows where there are cross-references between compound names and synonyms or
                # missing required values
                continue

            name = self.getRowVal(row, self.NAME_HEADER)
            formula = self.getRowVal(row, self.FORMULA_HEADER)
            hmdb_id = self.getRowVal(row, self.HMDB_ID_HEADER)
            synonyms = self.parse_synonyms(self.getRowVal(row, self.SYNONYMS_HEADER))

            try:
                compound_recdict = {
                    "name": name,
                    "formula": formula,
                    "hmdb_id": hmdb_id,
                }
                compound_rec, compound_created = Compound.objects.get_or_create(
                    **compound_recdict
                )
                # get_or_create does not perform a full clean
                if compound_created:
                    compound_rec.full_clean()
                    self.num_inserted_compounds += 1
                else:
                    self.num_existing_compounds += 1
            except IntegrityError as ie:
                iestr = str(ie)
                if "DataRepo_compoundsynonym_pkey" in iestr:
                    # This was generated by the override of Compound.save trying to create a synonym that already
                    # exists associated with a different compound
                    self.aggregated_errors_obj.buffer_error(
                        CompoundExistsAsMismatchedSynonym(
                            name,
                            compound_recdict,
                            CompoundSynonym.objects.get(name__exact=name),
                        )
                    )
                else:
                    self.aggregated_errors_obj.buffer_error(ie)
                self.num_erroneous_compounds += 1
            except Exception as e:
                self.aggregated_errors_obj.buffer_error(e)
                self.num_erroneous_compounds += 1

            for synonym in synonyms:
                try:
                    synonym_recdict = {
                        "name": synonym,
                        "compound": compound_rec,
                    }
                    (
                        synonym_rec,
                        synonym_created,
                    ) = CompoundSynonym.objects.get_or_create(**synonym_recdict)
                    # get_or_create does not perform a full clean
                    if synonym_created:
                        synonym_rec.full_clean()
                        self.num_inserted_synonyms += 1
                    else:
                        self.num_existing_synonyms += 1
                except SynonymExistsAsMismatchedCompound as seamc:
                    self.aggregated_errors_obj.buffer_error(seamc)
                except IntegrityError as ie:
                    iestr = str(ie)
                    if "duplicate key value violates unique constraint" in iestr:
                        # This means that the record exists (the synonym record exists, but to a different compound -
                        # so something in the compound data in the load doesn't completely match)
                        try:
                            existing_synonym = CompoundSynonym.objects.get(
                                name__exact=synonym_recdict["name"]
                            )
                            mismatches = self.check_for_inconsistencies(
                                existing_synonym, synonym_recdict, index
                            )
                            if mismatches == 0:
                                self.aggregated_errors_obj.buffer_error(ie)
                        except Exception:
                            self.aggregated_errors_obj.buffer_error(ie)
                    else:
                        self.aggregated_errors_obj.buffer_warning(
                            CompoundSynonymExists(name)
                        )
                    self.num_erroneous_synonyms += 1
                    # synonym_skips += 1
                except Exception as e:
                    self.aggregated_errors_obj.buffer_error(e)
                    self.num_erroneous_synonyms += 1

    def check_for_inconsistencies(self, rec, value_dict, index=None):
        mismatches = 0
        for field, new_value in value_dict.items():
            orig_value = getattr(rec, field)
            if orig_value != new_value:
                mismatches += 1
                self.aggregated_errors_obj.buffer_error(
                    ConflictingValueError(
                        rec,
                        field,
                        orig_value,
                        new_value,
                        index + 1,  # row number (starting from 1)
                    )
                )
        return mismatches


class CompoundExists(Exception):
    def __init__(self, cpd):
        message = f"Compound [{cpd}] already exists."
        super().__init__(message)
        self.cpd = cpd


class CompoundSynonymExists(Exception):
    def __init__(self, syn):
        message = f"CompoundSynonym [{syn}] already exists."
        super().__init__(message)
        self.syn = syn
