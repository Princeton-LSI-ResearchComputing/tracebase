from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q
from django.forms.models import model_to_dict

from DataRepo.models.utilities import atom_count_in_formula


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
        """
        Takes element symbol (e.g. "C") or element name (e.g. "Carbon") and returns the count of that element in the
        compound
        """
        return atom_count_in_formula(self.formula, atom)

    def get_or_create_synonym(self, synonym_name=None):
        if not synonym_name:
            synonym_name = self.name
        (compound_synonym, created) = CompoundSynonym.objects.get_or_create(
            name=synonym_name, compound_id=self.id
        )
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

    def clean(self, *args, **kwargs):
        """
        super.clean will raise an error about existing compounds, if this entire record already exists.

        But we also need to ensure that the compound name doesn't already exist as a synonym of a different compound.

        Note, calling super.clean first will give us access to self.id.
        """
        from DataRepo.utils.exceptions import CompoundExistsAsMismatchedSynonym

        super().clean(*args, **kwargs)
        sqs = CompoundSynonym.objects.filter(name__exact=self.name).exclude(
            compound__id__exact=self.id
        )
        # Don't report the ID - it is arbitrary, so remove it from the record dicts
        compound_dict = {k: v for k, v in model_to_dict(self).items() if k != "id"}
        if sqs.count() > 0:
            raise CompoundExistsAsMismatchedSynonym(
                self.name, compound_dict, sqs.first()
            )

    @classmethod
    def compound_matching_name_or_synonym(cls, name):
        """
        compound_matching_name_or_synonym is a class method that takes a string (name or
        synonym) and retrieves a distinct compound that matches it
        (case-insensitive), if any. Because we must enforce unique
        names, synonyms, and compound linkages, if more than 1 compound is found
        matching the query, an error is thrown.
        """

        # find the distinct union of these queries
        matching_compounds = cls.objects.filter(
            cls.get_name_query_expression(name)
        ).distinct()
        if matching_compounds.count() > 1:
            raise ValidationError(
                "compound_matching_name_or_synonym retrieved multiple "
                f"distinct compounds matching {name} from the database"
            )
        elif matching_compounds.count() == 0:
            raise cls.DoesNotExist(f"Compound [{name}] not found.")
        return matching_compounds.get()

    @classmethod
    def get_name_query_expression(cls, name):
        return Q(name__iexact=name) | Q(synonyms__name__iexact=name)

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

    def clean(self, *args, **kwargs):
        """
        super.clean will raise an error about existing synonyms.

        But we also need to ensure that this synonym doesn't already exist as a compound name for a different compound.

        Note, calling super.clean first will give us access to self.id.
        """
        from DataRepo.utils.exceptions import SynonymExistsAsMismatchedCompound

        super().clean(*args, **kwargs)
        cqs = Compound.objects.filter(name__exact=self.name).exclude(
            id__exact=self.compound.id
        )
        if cqs.count() > 0:
            raise SynonymExistsAsMismatchedCompound(
                self.name, self.compound, cqs.first()
            )
