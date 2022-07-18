from django.conf import settings
from django.db.utils import IntegrityError

from DataRepo.models import Compound
from DataRepo.utils.exceptions import (
    AmbiguousCompoundDefinitionError,
    HeaderError,
    ValidationDatabaseSetupError,
)


class CompoundsLoader:
    """
    Load the Compound and CompoundSynonym tables
    """

    # Define the dataframe key names and requirements
    KEY_COMPOUND_NAME = "Compound"
    KEY_HMDB = "HMDB ID"
    KEY_FORMULA = "Formula"
    KEY_SYNONYMS = "Synonyms"
    REQUIRED_KEYS = [KEY_COMPOUND_NAME, KEY_FORMULA, KEY_HMDB, KEY_SYNONYMS]

    def __init__(
        self, compounds_df, synonym_separator=";", database=None, validate=False
    ):
        self.compounds_df = compounds_df
        self.synonym_separator = synonym_separator
        self.validation_debug_messages = []
        self.validation_warning_messages = []
        self.validation_error_messages = []
        self.summary_messages = []
        self.validated_new_compounds_for_insertion = []
        self.missing_headers = []
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

        """
        strip any leading and trailing spaces from the headers and some
        columns, just to normalize
        """
        if self.compounds_df is not None:
            self.compounds_df.rename(columns=lambda x: x.strip())
            self.check_required_headers()
            for col in (
                self.KEY_COMPOUND_NAME,
                self.KEY_FORMULA,
                self.KEY_HMDB,
                self.KEY_SYNONYMS,
            ):
                self.compounds_df[col] = self.compounds_df[col].str.strip()

    def validate_data(self):
        # validate the compounds dataframe
        self.check_for_duplicates(self.KEY_COMPOUND_NAME)
        self.check_for_duplicates(self.KEY_HMDB)

        if self.compounds_df is not None:
            for index, row in self.compounds_df.iterrows():
                # capture compound attributes and synonyms
                compound = self.find_compound_for_row(row)
                if compound is None:
                    # data does not exist in database; record for future insertion
                    new_compound = Compound(
                        name=row[self.KEY_COMPOUND_NAME],
                        formula=row[self.KEY_FORMULA],
                        hmdb_id=row[self.KEY_HMDB],
                    )
                    # full_clean cannot validate (e.g. uniqueness) using a non-default database
                    if self.db == settings.DEFAULT_DB:
                        new_compound.full_clean()
                    self.validated_new_compounds_for_insertion.append(new_compound)

    def check_required_headers(self):
        for header in self.REQUIRED_KEYS:
            if header not in self.compounds_df.columns:
                self.missing_headers.append(header)
                err_msg = f"Could not find the required header '{header}."
                self.validation_error_messages.append(err_msg)
        if len(self.missing_headers) > 0:
            raise (
                HeaderError(
                    f"The following column headers were missing: {', '.join(self.missing_headers)}",
                    self.missing_headers,
                )
            )

    def check_for_duplicates(self, column_header):

        dupe_dict = {}
        for index, row in self.compounds_df[
            self.compounds_df.duplicated(subset=[column_header], keep=False)
        ].iterrows():
            dupe_key = row[column_header]
            if dupe_key not in dupe_dict:
                dupe_dict[dupe_key] = str(index + 1)
            else:
                dupe_dict[dupe_key] += "," + str(index + 1)

        if len(dupe_dict.keys()) != 0:
            err_msg = (
                f"The following duplicate {column_header} were found in the original data: ["
                f"{'; '.join(list(map(lambda c: c + ' on rows: ' + dupe_dict[c], dupe_dict.keys())))}]"
            )

            self.validation_error_messages.append(err_msg)

    def find_compound_for_row(self, row):
        """
        Find single Compound record matching data from the input row.

        Searches compound records using HMDB ID and name. Appends a warning to
        `validation_warning_messages` if HMDB ID is not found.  Searches
        compound records using all synonyms.  If the queries return multiple
        distinct scompounds, an `AmbiguousCompoundDefinitionError` is raised.

        Args:
            row (Series): Pandas Series representing a potential Compound
                record

        Returns:
            compound: A single compound record matching the HMDB, name, and
                synonym records in the input row

        Raises:
            AmbiguousCompoundDefinitionError: Multiple compounds were found
        """
        found_compound = None
        hmdb_compound = None
        named_compound = None
        all_found_compounds = []
        # start with the HMDB_ID
        hmdb_id = row[self.KEY_HMDB]
        name = row[self.KEY_COMPOUND_NAME]
        synonyms_string = row[self.KEY_SYNONYMS]
        try:
            hmdb_compound = Compound.objects.using(self.db).get(hmdb_id=hmdb_id)
            # preferred method of "finding because it is not a potential synonym
            found_compound = hmdb_compound
            self.validation_debug_messages.append(
                f"Found {found_compound.name} using HMDB ID {hmdb_id}"
            )
            all_found_compounds.append(hmdb_compound)
        except Compound.DoesNotExist:
            # must be a new compound, or a data inconsistency?
            msg = f"Database lacks HMBD ID {hmdb_id}"
            self.validation_warning_messages.append(msg)

        try:
            named_compound = Compound.objects.using(self.db).get(name=name)
            if hmdb_compound is None:
                found_compound = named_compound
                self.validation_debug_messages.append(
                    f"Found {found_compound.name} using Compound name {name}"
                )
                all_found_compounds.append(named_compound)
        except Compound.DoesNotExist:
            # must be a new compound, or a data inconsistency?
            msg = f"Database lacks named compound {name}"
            self.validation_warning_messages.append(msg)

        # if we have any inconsistency between these two queries above, either the
        # file or the database is "wrong"
        if hmdb_compound != named_compound:
            err_msg = f"ERROR: Data inconsistency. File input Compound={name} HMDB ID={hmdb_id} "
            if hmdb_compound is None:
                err_msg += "did not match a database record using the file's HMDB ID, "
            else:
                err_msg += f"matches a database compound (by file's HMDB ID) of Compound={hmdb_compound.name} "
                err_msg += f"HMDB ID={hmdb_compound.hmdb_id}, "

            if named_compound is None:
                err_msg += "but did not match a named database record using the file's Compound "
            else:
                err_msg += f"but matches a database compound (by file's Compound) of Compound={named_compound.name} "
                err_msg += f"HMDB ID={named_compound.hmdb_id}"

            self.validation_error_messages.append(err_msg)

        if hmdb_compound is None and named_compound is None:
            self.validation_debug_messages.append(f"Could not find {hmdb_id}")
            # attempt a query by either name or synonym(s)
            names = [name]
            if synonyms_string is not None and synonyms_string != "":
                synonyms = self.parse_synonyms(synonyms_string)
                names.extend(synonyms)
            for name in names:
                alt_name_compound = Compound.compound_matching_name_or_synonym(
                    name, database=self.db
                )
                if alt_name_compound is not None:
                    self.validation_debug_messages.append(
                        f"Found {alt_name_compound.name} using {name}"
                    )
                    found_compound = alt_name_compound
                    if found_compound not in all_found_compounds:
                        all_found_compounds.append(alt_name_compound)
                else:
                    self.validation_debug_messages.append(
                        f"Could not find {name} in names or synonyms"
                    )

        if len(all_found_compounds) > 1:
            err_msg = f"Retrieved multiple ({len(all_found_compounds)}) "
            err_msg += f"distinct compounds using names {names}"
            raise AmbiguousCompoundDefinitionError(err_msg)

        return found_compound

    def parse_synonyms(self, synonyms_string: str) -> list:
        synonyms = [
            synonym.strip() for synonym in synonyms_string.split(self.synonym_separator)
        ]
        return synonyms

    def load_validated_compounds(self):
        # "both" is the normal loading mode - always loads both the validation and tracebase databases, unless the
        # database is explicitly supplied or --validate is supplied
        if self.loading_mode == "both":
            try:
                self.load_validated_compounds_per_db(settings.TRACEBASE_DB)
                if settings.VALIDATION_ENABLED:
                    self.load_validated_compounds_per_db(settings.VALIDATION_DB)
            except (CompoundExists, CompoundNotFound) as ce:
                ce.message += (
                    f"  Try loading databases {settings.TRACEBASE_DB} and {settings.VALIDATION_DB} one by "
                    "one."
                )
                raise ce
        elif self.loading_mode == "one":
            self.load_validated_compounds_per_db(self.db)
        else:
            raise Exception(
                f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
            )

    def load_validated_compounds_per_db(self, db=settings.TRACEBASE_DB):
        count = 0
        for compound in self.validated_new_compounds_for_insertion:
            try:
                compound.save(using=db)
            except IntegrityError:
                raise CompoundExists(compound.name, db)
            count += 1
        self.summary_messages.append(
            f"{count} compound(s) inserted, with default synonyms, into the {db} database."
        )

    def load_synonyms(self):
        # "both" is the normal loading mode - always loads both the validation and tracebase databases, unless the
        # database is explicitly supplied or --validate is supplied
        if self.loading_mode == "both":
            self.load_synonyms_per_db(settings.TRACEBASE_DB)
            if settings.VALIDATION_ENABLED:
                self.load_synonyms_per_db(settings.VALIDATION_DB)
        elif self.loading_mode == "one":
            self.load_synonyms_per_db(self.db)
        else:
            raise Exception(
                f"Internal error: Invalid loading_mode: [{self.loading_mode}]"
            )

    def load_synonyms_per_db(self, db=settings.TRACEBASE_DB):
        # if we are here, every line should either have pre-existed, or have
        # been newly inserted.
        count = 0
        for index, row in self.compounds_df.iterrows():
            # we will use the HMDB ID to retrieve
            hmdb_id = row[self.KEY_HMDB]
            # this name might always be a synonym
            compound_name_from_file = row[self.KEY_COMPOUND_NAME]
            try:
                hmdb_compound = Compound.objects.using(db).get(hmdb_id=hmdb_id)
            except Compound.DoesNotExist:
                raise CompoundNotFound(compound_name_from_file, db, hmdb_id)
            synonyms_string = row[self.KEY_SYNONYMS]
            synonyms = self.parse_synonyms(synonyms_string)
            if hmdb_compound.name != compound_name_from_file:
                synonyms.append(compound_name_from_file)
            for synonym in synonyms:
                (compound_synonym, created) = hmdb_compound.get_or_create_synonym(
                    synonym, database=db
                )
                if created:
                    count += 1
        self.summary_messages.append(
            f"{count} additional synonym(s) inserted into the {db} database."
        )


class CompoundExists(Exception):
    message = ""

    def __init__(self, cpd, db):
        self.message = f"Compound {cpd} already exists in the {db} database."
        super().__init__(self.message)
        self.cpd = cpd
        self.db = db


class CompoundNotFound(Exception):
    message = ""

    def __init__(self, cpd, db, hmdb_id):
        self.message = f"Cound not find compound [{cpd}] in database [{db}] by searching for its HMDB ID: [{hmdb_id}]."
        super().__init__(self.message)
        self.cpd = cpd
        self.db = db
        self.hmdb_id = hmdb_id
