from collections import defaultdict

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.utils import IntegrityError

from DataRepo.models import Compound, CompoundSynonym
from DataRepo.models.compound import (
    CompoundExistsAsMismatchedSynonym,
    SynonymExistsAsMismatchedCompound,
)
from DataRepo.utils.exceptions import (  # AmbiguousCompoundDefinitionError,
    AggregatedErrors,
    ConflictingValueError,
    DryRun,
    DuplicateValues,
    RequiredHeadersError,
    RequiredValuesError,
    UnknownHeadersError,
    ValidationDatabaseSetupError,
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
        database=None,
        validate=False,
        dry_run=False,
    ):
        self.compounds_df = compounds_df
        self.synonym_separator = synonym_separator
        self.validation_debug_messages = []
        self.validation_warning_messages = []
        self.validation_error_messages = []
        self.summary_messages = []
        self.validated_new_compounds_for_insertion = {
            settings.TRACEBASE_DB: [],
            settings.VALIDATION_DB: [],
        }
        # self.missing_rqd_values = defaultdict(list)
        self.bad_row_indexes = []
        self.num_inserted_compounds = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.num_existing_compounds = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.num_erroneous_compounds = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.num_inserted_synonyms = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.num_existing_synonyms = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.num_erroneous_synonyms = {
            settings.TRACEBASE_DB: 0,
            settings.VALIDATION_DB: 0,
        }
        self.dry_run = dry_run
        self.db = settings.TRACEBASE_DB
        self.loading_mode = "both"
        # If a database was explicitly supplied
        if database is not None:
            self.validate = False
            self.db = database
            self.loading_mode = "one"
        else:
            self.validate = validate
            if validate:
                if settings.VALIDATION_ENABLED:
                    self.db = settings.VALIDATION_DB
                else:
                    raise ValidationDatabaseSetupError()
                self.loading_mode = "one"
            else:
                self.loading_mode = "both"

        self.aggregated_errors_obj = AggregatedErrors()

    # def validate_data(self):
    #     if self.loading_mode == "both":
    #         try:
    #             self.validate_data_per_db(settings.TRACEBASE_DB)
    #             if settings.VALIDATION_ENABLED:
    #                 self.validate_data_per_db(settings.VALIDATION_DB)
    #         except (CompoundExists, CompoundNotFound) as ce:
    #             ce.message += (
    #                 f"  Try loading databases {settings.TRACEBASE_DB} and {settings.VALIDATION_DB} one by "
    #                 "one."
    #             )
    #             self.aggregated_errors_obj.buffer_error(ce)
    #     elif self.loading_mode == "one":
    #         try:
    #             self.validate_data_per_db(self.db)
    #         except Exception as e:
    #             self.aggregated_errors_obj.buffer_error(e)
    #     else:
    #         # Cannot proceed
    #         raise ValueError(
    #             f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
    #         )

    # def validate_data_per_db(self, db=settings.TRACEBASE_DB):
    #     # validate the compounds dataframe
    #     self.check_for_infile_duplicates(self.NAME_HEADER)
    #     self.check_for_infile_duplicates(self.HMDB_ID_HEADER)

    #     if self.compounds_df is not None:
    #         count = 0
    #         existing_skips = 0
    #         error_skips = 0
    #         for index, row in self.compounds_df.iterrows():
    #             if index in self.bad_row_indexes:
    #                 continue
    #             # capture compound attributes and synonyms
    #             compound, valid = self.find_compound_for_row(row, db)
    #             if compound is None and valid:
    #                 # data does not exist in database; record for future insertion
    #                 new_compound = Compound(
    #                     name=row[self.NAME_HEADER],
    #                     formula=row[self.FORMULA_HEADER],
    #                     hmdb_id=row[self.HMDB_ID_HEADER],
    #                 )
    #                 # full_clean cannot validate (e.g. uniqueness) using a non-default database
    #                 if db == settings.DEFAULT_DB:
    #                     try:
    #                         new_compound.full_clean()
    #                         self.validated_new_compounds_for_insertion[db].append(
    #                             new_compound
    #                         )
    #                         count += 1
    #                     except IntegrityError:
    #                         self.aggregated_errors_obj.buffer_warning(
    #                             CompoundExists(row[self.NAME_HEADER], db)
    #                         )
    #                         existing_skips += 1
    #                     except Exception as e:
    #                         self.aggregated_errors_obj.buffer_error(e)
    #                         error_skips += 1
    #                 elif db == settings.VALIDATION_DB:
    #                     self.validated_new_compounds_for_insertion[db].append(
    #                         new_compound
    #                     )
    #                     count += 1
    #             elif not valid:
    #                 error_skips += 1
    #         self.summary_messages.append(
    #             f"{count} compound(s) will be inserted, with default synonyms, into the {db} database.  "
    #             f"{existing_skips} already existed in the database and will be skipped.  {error_skips} compounds had "
    #             "errors that need to be fixed before they can be loaded."
    #         )
    #     else:
    #         # Nothing else to do, so raise
    #         raise ValueError("No compound data was supplied.")

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

    # def find_compound_for_row(self, row, db=settings.TRACEBASE_DB):
    #     """
    #     Find single Compound record matching data from the input row.

    #     Searches compound records using HMDB ID and name. Appends a warning to
    #     `validation_warning_messages` if HMDB ID is not found.  Searches
    #     compound records using all synonyms.  If the queries return multiple
    #     distinct scompounds, an `AmbiguousCompoundDefinitionError` is raised.

    #     Args:
    #         row (Series): Pandas Series representing a potential Compound
    #             record

    #     Returns:
    #         compound: A single compound record matching the HMDB, name, and
    #             synonym records in the input row
    #         valid: Whether there was a valid match (i.e. not an ambiguous match)
    #     Raises:
    #         no exceptions, but buffers errors/warnings
    #     """
    #     found_compound = None
    #     hmdb_compound = None
    #     named_compound = None
    #     all_found_compounds = []
    #     # start with the HMDB_ID
    #     hmdb_id = row[self.HMDB_ID_HEADER]
    #     name = row[self.NAME_HEADER]
    #     synonyms_string = row[self.SYNONYMS_HEADER]
    #     try:
    #         hmdb_compound = Compound.objects.using(db).get(hmdb_id=hmdb_id)
    #         # preferred method of "finding because it is not a potential synonym
    #         found_compound = hmdb_compound
    #         self.validation_debug_messages.append(
    #             f"Found {found_compound.name} using HMDB ID {hmdb_id}"
    #         )
    #         all_found_compounds.append(hmdb_compound)
    #     except Compound.DoesNotExist:
    #         # must be a new compound, or a data inconsistency?
    #         msg = f"Database lacks HMBD ID {hmdb_id}"
    #         self.validation_warning_messages.append(msg)

    #     try:
    #         named_compound = Compound.objects.using(db).get(name=name)
    #         if hmdb_compound is None:
    #             found_compound = named_compound
    #             self.validation_debug_messages.append(
    #                 f"Found {found_compound.name} using Compound name {name}"
    #             )
    #             all_found_compounds.append(named_compound)
    #     except Compound.DoesNotExist:
    #         # must be a new compound, or a data inconsistency?
    #         msg = f"Database lacks named compound {name}"
    #         self.validation_warning_messages.append(msg)

    #     # if we have any inconsistency between these two queries above, either the
    #     # file or the database is "wrong"
    #     if hmdb_compound != named_compound:
    #         if hmdb_compound:
    #             cve = ConflictingValueError(
    #                 hmdb_compound,
    #                 "name / hmdb_id",
    #                 f"{hmdb_compound.name} / {hmdb_compound.hmdb_id}",
    #                 f"{name} / {hmdb_id}",
    #             )
    #             self.aggregated_errors_obj.buffer_error(cve)
    #             self.validation_error_messages.append(str(cve))

    #         if named_compound:
    #             # 2 possible concurrent errors because 2 matching database records
    #             cve = ConflictingValueError(
    #                 named_compound,
    #                 "name / hmdb_id",
    #                 f"{named_compound.name} / {named_compound.hmdb_id}",
    #                 f"{name} / {hmdb_id}",
    #             )
    #             self.aggregated_errors_obj.buffer_error(cve)
    #             self.validation_error_messages.append(str(cve))

    #     if hmdb_compound is None and named_compound is None:
    #         self.validation_debug_messages.append(f"Could not find {hmdb_id}")
    #         # attempt a query by either name or synonym(s)
    #         names = [name]
    #         if synonyms_string is not None and synonyms_string != "":
    #             synonyms = self.parse_synonyms(synonyms_string)
    #             names.extend(synonyms)
    #         for name in names:
    #             alt_name_compound = Compound.compound_matching_name_or_synonym(
    #                 name, database=db
    #             )
    #             if alt_name_compound is not None:
    #                 self.validation_debug_messages.append(
    #                     f"Found {alt_name_compound.name} using {name}"
    #                 )
    #                 found_compound = alt_name_compound
    #                 if found_compound not in all_found_compounds:
    #                     all_found_compounds.append(alt_name_compound)
    #             else:
    #                 self.validation_debug_messages.append(
    #                     f"Could not find {name} in names or synonyms"
    #                 )

    #     if len(all_found_compounds) > 1:
    #         err_msg = f"Retrieved multiple ({len(all_found_compounds)}) "
    #         err_msg += f"distinct compounds using names {names}"
    #         self.aggregated_errors_obj.buffer_error(
    #             AmbiguousCompoundDefinitionError(err_msg)
    #         )
    #         return None, False

    #     return found_compound, True

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
                # self._load_data()
                self._better_load_data()
            except Exception as e:
                self.aggregated_errors_obj.buffer_error(e)

            # If there are buffered errors
            if self.aggregated_errors_obj.should_raise():
                raise self.aggregated_errors_obj

            if self.dry_run:
                # Roll back everything
                raise DryRun()

    # def _load_data(self):
    #     self.validate_data()
    #     self.load_validated_compounds()
    #     self.load_synonyms()

    def _better_load_data(self):
        self.validate_infile_data()
        if self.loading_mode == "both":
            self.better_load_compounds_per_db(settings.TRACEBASE_DB)
            if settings.VALIDATION_ENABLED:
                self.better_load_compounds_per_db(settings.VALIDATION_DB)
        elif self.loading_mode == "one":
            try:
                self.better_load_compounds_per_db(self.db)
            except Exception as e:
                self.aggregated_errors_obj.buffer_error(e)
        else:
            # Should not proceed
            raise ValueError(
                f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
            )

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

    def better_load_compounds_per_db(self, db=settings.TRACEBASE_DB):
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
                compound_rec, compound_created = Compound.objects.using(
                    db
                ).get_or_create(**compound_recdict)
                # get_or_create does not perform a full clean
                if compound_created:
                    try:
                        compound_rec.full_clean()
                    except ValidationError as ve:
                        if db == settings.VALIDATION_DB:
                            # get_or_create saves before full_clean which cases a unique constraint issue when full
                            # clean is called on a record added to a non-default database, so pass.
                            pass
                        else:
                            raise ve
                    self.num_inserted_compounds[db] += 1
                else:
                    self.num_existing_compounds[db] += 1
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
                self.num_erroneous_compounds[db] += 1
            except Exception as e:
                self.aggregated_errors_obj.buffer_error(e)
                self.num_erroneous_compounds[db] += 1

            for synonym in synonyms:
                try:
                    synonym_recdict = {
                        "name": synonym,
                        "compound": compound_rec,
                    }
                    synonym_rec, synonym_created = CompoundSynonym.objects.using(
                        db
                    ).get_or_create(**synonym_recdict)
                    # get_or_create does not perform a full clean
                    if synonym_created:
                        synonym_rec.full_clean()
                        self.num_inserted_synonyms[db] += 1
                    else:
                        self.num_existing_synonyms[db] += 1
                except SynonymExistsAsMismatchedCompound as seamc:
                    self.aggregated_errors_obj.buffer_error(seamc)
                except IntegrityError as ie:
                    iestr = str(ie)
                    if "duplicate key value violates unique constraint" in iestr:
                        # This means that the record exists (the synonym record exists, but to a different compound -
                        # so something in the compound data in the load doesn't completely match)
                        try:
                            existing_synonym = CompoundSynonym.objects.using(db).get(
                                name__exact=synonym_recdict["name"]
                            )
                            mismatches = self.check_for_inconsistencies(
                                existing_synonym, synonym_recdict, index, db
                            )
                            if mismatches == 0:
                                self.aggregated_errors_obj.buffer_error(ie)
                        except Exception:
                            self.aggregated_errors_obj.buffer_error(ie)
                    else:
                        self.aggregated_errors_obj.buffer_warning(
                            CompoundSynonymExists(name, db)
                        )
                    self.num_erroneous_synonyms[db] += 1
                    # synonym_skips += 1
                except Exception as e:
                    self.aggregated_errors_obj.buffer_error(e)
                    self.num_erroneous_synonyms[db] += 1

    def check_for_inconsistencies(self, rec, value_dict, index=None, db=None):
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
                        db,
                    )
                )
        return mismatches

    # def load_validated_compounds(self):
    #     # "both" is the normal loading mode - always loads both the validation and tracebase databases, unless the
    #     # database is explicitly supplied or --validate is supplied
    #     if self.loading_mode == "both":
    #         try:
    #             self.load_validated_compounds_per_db(settings.TRACEBASE_DB)
    #             if settings.VALIDATION_ENABLED:
    #                 self.load_validated_compounds_per_db(settings.VALIDATION_DB)
    #         except (CompoundExists, CompoundNotFound) as ce:
    #             ce.message += (
    #                 f"  Try loading databases {settings.TRACEBASE_DB} and {settings.VALIDATION_DB} one by "
    #                 "one."
    #             )
    #             self.aggregated_errors_obj.buffer_error(ce)
    #     elif self.loading_mode == "one":
    #         try:
    #             self.load_validated_compounds_per_db(self.db)
    #         except Exception as e:
    #             self.aggregated_errors_obj.buffer_error(e)
    #     else:
    #         # Should not proceed
    #         raise ValueError(
    #             f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
    #         )

    # def load_validated_compounds_per_db(self, db=settings.TRACEBASE_DB):
    #     count = 0
    #     existing_skips = 0
    #     for compound in self.validated_new_compounds_for_insertion[db]:
    #         try:
    #             compound.save(using=db)
    #             count += 1
    #         except IntegrityError:
    #             self.aggregated_errors_obj.buffer_warning(
    #                 CompoundExists(compound.name, db)
    #             )
    #             existing_skips += 1
    #             continue
    #     self.summary_messages.append(
    #         f"{count} compound(s) inserted, with default synonyms, into the {db} database.  "
    #         f"{existing_skips} were already in the database."
    #     )

    # def load_synonyms(self):
    #     # "both" is the normal loading mode - always loads both the validation and tracebase databases, unless the
    #     # database is explicitly supplied or --validate is supplied
    #     if self.loading_mode == "both":
    #         self.load_synonyms_per_db(settings.TRACEBASE_DB)
    #         if settings.VALIDATION_ENABLED:
    #             self.load_synonyms_per_db(settings.VALIDATION_DB)
    #     elif self.loading_mode == "one":
    #         self.load_synonyms_per_db(self.db)
    #     else:
    #         # Cannot proceed
    #         raise ValueError(
    #             f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
    #         )

    # def load_synonyms_per_db(self, db=settings.TRACEBASE_DB):
    #     # if we are here, every line should either have pre-existed, or have
    #     # been newly inserted.
    #     count = 0
    #     skips = 0
    #     for index, row in self.compounds_df.iterrows():
    #         if index in self.bad_row_indexes:
    #             continue
    #         # we will use the HMDB ID to retrieve
    #         hmdb_id = row[self.HMDB_ID_HEADER]
    #         # this name might always be a synonym
    #         compound_name_from_file = row[self.NAME_HEADER]
    #         try:
    #             hmdb_compound = Compound.objects.using(db).get(hmdb_id=hmdb_id)
    #             synonyms_string = row[self.SYNONYMS_HEADER]
    #             synonyms = self.parse_synonyms(synonyms_string)
    #             if hmdb_compound.name != compound_name_from_file:
    #                 synonyms.append(compound_name_from_file)
    #             for synonym in synonyms:
    #                 (compound_synonym, created) = hmdb_compound.get_or_create_synonym(
    #                     synonym, database=db
    #                 )
    #                 if created:
    #                     count += 1
    #         except Compound.DoesNotExist:
    #             skips += 1
    #             self.aggregated_errors_obj.buffer_error(
    #                 CompoundNotFound(compound_name_from_file, db, hmdb_id)
    #             )
    #     self.summary_messages.append(
    #         f"{count} additional synonym(s) inserted into the {db} database.  {skips} skipped."
    #     )


class CompoundExists(Exception):
    def __init__(self, cpd, db):
        message = f"Compound [{cpd}] already exists in the {db} database."
        super().__init__(message)
        self.cpd = cpd
        self.db = db


class CompoundSynonymExists(Exception):
    def __init__(self, syn, db):
        message = f"CompoundSynonym [{syn}] already exists in the {db} database."
        super().__init__(message)
        self.syn = syn
        self.db = db


# TODO: Delete this exception class (or move to exceptions.py). I don't think I need it.
class CompoundNotFound(Exception):
    def __init__(self, cpd, db, hmdb_id):
        message = f"Cound not find compound [{cpd}] in database [{db}] by searching for its HMDB ID: [{hmdb_id}]."
        super().__init__(message)
        self.cpd = cpd
        self.db = db
        self.hmdb_id = hmdb_id
