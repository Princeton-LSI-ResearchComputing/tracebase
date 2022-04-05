from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q

from .utilities import atom_count_in_formula


class Compound(models.Model):
    # Class variables
    HMDB_CPD_URL = "https://hmdb.ca/metabolites"

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="The compound name that is commonly used in the laboratory "
        '(e.g. "glucose", "C16:0", etc.).',
    )
    formula = models.CharField(
        max_length=256,
        help_text="The molecular formula of the compound "
        '(e.g. "C6H12O6", "C16H32O2", etc.).',
    )

    # ID to serve as an external link to record using HMDB_CPD_URL

    hmdb_id = models.CharField(
        max_length=11,
        unique=True,
        verbose_name="HMDB ID",
        help_text=f"A unique identifier for this compound in the Human Metabolome Database ({HMDB_CPD_URL}).",
    )

    @property  # type: ignore
    def hmdb_url(self):
        "Returns the url to the compound's hmdb record"
        return f"{self.HMDB_CPD_URL}/{self.hmdb_id}"

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    def get_or_create_synonym(self, synonym_name=None, database=settings.TRACEBASE_DB):
        if not synonym_name:
            synonym_name = self.name
        (compound_synonym, created) = CompoundSynonym.objects.using(
            database
        ).get_or_create(name=synonym_name, compound_id=self.id)
        return (compound_synonym, created)

    def save(self, *args, **kwargs):
        """
        Call the "real" save() method first, to generate the compound_id,
        because the compound_id is intrinsic to the compound_synonym(s) which we
        are auto-creating afterwards
        """
        super().save(*args, **kwargs)
        (_primary_synonym, created) = self.get_or_create_synonym()
        ucfirst_synonym = self.name[0].upper() + self.name[1:]
        (_secondary_synonym, created) = self.get_or_create_synonym(ucfirst_synonym)

    @classmethod
    def compound_matching_name_or_synonym(cls, name, database=settings.TRACEBASE_DB):
        """
        compound_matching_name_or_synonym is a class method that takes a string (name or
        synonym) and retrieves a distinct compound that matches it
        (case-insensitive), if any. Because we must enforce unique
        names, synonyms, and compound linkages, if more than 1 compound is found
        matching the query, an error is thrown.
        """

        # find the distinct union of these queries
        matching_compounds = (
            cls.objects.using(database)
            .filter(Q(name__iexact=name) | Q(synonyms__name__iexact=name))
            .distinct()
        )
        if matching_compounds.count() > 1:
            raise ValidationError(
                "compound_matching_name_or_synonym retrieved multiple "
                f"distinct compounds matching {name} from the database"
            )
        return matching_compounds.first()

    class Meta:
        verbose_name = "compound"
        verbose_name_plural = "compounds"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class CompoundSynonym(models.Model):

    # Instance / model fields
    name = models.CharField(
        primary_key=True,
        max_length=256,
        unique=True,
        help_text="A synonymous name for a compound that is commonly used within the laboratory. "
        '(e.g. "palmitic acid", "hexadecanoic acid", "C16", and "palmitate" '
        'might also be synonyms for "C16:0").',
    )
    compound = models.ForeignKey(
        Compound, related_name="synonyms", on_delete=models.CASCADE
    )

    class Meta:
        verbose_name = "synonym"
        verbose_name_plural = "synonyms"
        ordering = ["compound", "name"]

    def __str__(self):
        return str(self.name)
