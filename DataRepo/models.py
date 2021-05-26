import warnings
from datetime import date, timedelta

from chempy import Substance
from chempy.util.periodic import atomic_number
from django.core.exceptions import ValidationError
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import Sum
from django.utils.functional import cached_property


def value_from_choices_label(label, choices):
    """
    Return the choices value for a given label
    """
    dictionary = {}
    for choices_value, choices_label in choices:
        dictionary[choices_label] = choices_value
    result = None
    result = dictionary.get(label)
    return result


# abstract class for models which are "labeled" with tracers; because we have
# not normalized column names, this is not a Django Abstract base class
# (https://docs.djangoproject.com/en/3.1/topics/db/models/#abstract-base-classes).
# This simply shares some configured variables/values.
class TracerLabeledClass:
    # choice specifications
    CARBON = "C"
    NITROGEN = "N"
    HYDROGEN = "H"
    OXYGEN = "O"
    SULFUR = "S"
    TRACER_LABELED_ELEMENT_CHOICES = [
        (CARBON, "Carbon"),
        (NITROGEN, "Nitrogen"),
        (HYDROGEN, "Hydrogen"),
        (OXYGEN, "Oxygen"),
        (SULFUR, "Sulfur"),
    ]

    MAX_LABELED_ATOMS = 20

    @classmethod
    def tracer_labeled_elements_list(cls):
        tracer_element_list = []
        for idx in cls.TRACER_LABELED_ELEMENT_CHOICES:
            tracer_element_list.append(idx[0])
        return tracer_element_list


def atom_count_in_formula(formula, atom):
    """
    Return the number of specified atom in the compound.
    Returns None if atom is not a recognized symbol
    Returns 0 if the atom is recognized, but not found in the compound
    """
    substance = Substance.from_formula(formula)
    try:
        count = substance.composition.get(atomic_number(atom))
    except ValueError:
        warnings.warn(f"{atom} not found in list of elements")
        count = None
    else:
        if count is None:
            # Valid atom, but not in formula
            count = 0
    return count


class Protocol(models.Model):

    MSRUN_PROTOCOL = "msrun_protocol"
    ANIMAL_TREATMENT = "animal_treatment"
    CATEGORY_CHOICES = [
        (MSRUN_PROTOCOL, "LC-MS Run Protocol"),
        (ANIMAL_TREATMENT, "Animal Treatment"),
    ]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(
        blank=True, help_text="Full text of the protocol's methods"
    )
    category = models.CharField(
        max_length=256,
        choices=CATEGORY_CHOICES,
        default=MSRUN_PROTOCOL,
        help_text="Classification of the protocol, "
        "e.g. an animal treatment or MSRun procedure.",
    )

    def __str__(self):
        return str(self.name)


class Compound(models.Model):
    # Class variables
    HMDB_CPD_URL = "https://hmdb.ca/metabolites"

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    formula = models.CharField(max_length=256)

    # ID to serve as an external link to record using HMDB_CPD_URL
    hmdb_id = models.CharField(max_length=11, unique=True)

    @property
    def hmdb_url(self):
        "Returns the url to the compound's hmdb record"
        return f"{self.HMDB_CPD_URL}/{self.hmdb_id}"

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)


class Study(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(blank=True)

    def __str__(self):
        return str(self.name)


class Animal(models.Model, TracerLabeledClass):

    FEMALE = "F"
    MALE = "M"
    SEX_CHOICES = [(FEMALE, "female"), (MALE, "male")]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    state = models.CharField(max_length=256, null=True, blank=True)
    tracer_compound = models.ForeignKey(Compound, on_delete=models.RESTRICT, null=True)
    # NOTE: encoding labeled atom as the atom's symbol, NOT the full element
    # name, as I have seen in some example files
    tracer_labeled_atom = models.CharField(
        max_length=1,
        null=True,
        choices=TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
        default=TracerLabeledClass.CARBON,
        blank=True,
    )
    # NOTE: encoding atom count as an integer, NOT a float, as I have seen in
    # some example files
    tracer_labeled_count = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(1),
            MaxValueValidator(TracerLabeledClass.MAX_LABELED_ATOMS),
        ],
        help_text="The number of labeled atoms (M+) in the tracer compound "
        "supplied to this animal.",
    )
    tracer_infusion_rate = models.FloatField(
        null=True, blank=True, validators=[MinValueValidator(0)]
    )
    tracer_infusion_concentration = models.FloatField(
        null=True, blank=True, validators=[MinValueValidator(0)]
    )
    genotype = models.CharField(max_length=256)
    body_weight = models.FloatField(
        null=True, blank=True, validators=[MinValueValidator(0)]
    )
    age = models.FloatField(null=True, blank=True, validators=[MinValueValidator(0)])
    sex = models.CharField(max_length=1, null=True, blank=True, choices=SEX_CHOICES)
    diet = models.CharField(max_length=256, null=True, blank=True)
    feeding_status = models.CharField(max_length=256, null=True, blank=True)
    studies = models.ManyToManyField(Study, related_name="animals")
    treatment = models.ForeignKey(
        Protocol,
        on_delete=models.RESTRICT,
        null=True,
        blank=True,
        related_name="animals",
        help_text="Lab controlled label of the actions taken on an animal.",
        limit_choices_to={"category": Protocol.ANIMAL_TREATMENT},
    )

    def clean(self):
        super().clean()

        if self.treatment is not None:
            if self.treatment.category != Protocol.ANIMAL_TREATMENT:
                raise ValidationError(
                    "Protocol category for an Animal must be of type "
                    f"{Protocol.ANIMAL_TREATMENT}"
                )

    def __str__(self):
        return str(self.name)


class Tissue(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)

    SERUM_TISSUE_NAME = "Serum"

    def __str__(self):
        return str(self.name)


class Sample(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    date = models.DateField(default=date.today)
    researcher = models.CharField(max_length=256)
    animal = models.ForeignKey(
        Animal, on_delete=models.CASCADE, null=False, related_name="samples"
    )
    tissue = models.ForeignKey(Tissue, on_delete=models.RESTRICT, null=False)
    """
    researchers have advised that samples might have a time_collected up to a
    day prior-to and a week after infusion
    """
    MINIMUM_VALID_TIME_COLLECTED = timedelta(days=-1)
    MAXIMUM_VALID_TIME_COLLECTED = timedelta(weeks=1)
    time_collected = models.DurationField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(MINIMUM_VALID_TIME_COLLECTED),
            MaxValueValidator(MAXIMUM_VALID_TIME_COLLECTED),
        ],
        help_text="The time, in minutes relative to an infusion timepoint, "
        "that a sample was extracted from a animal",
    )

    def __str__(self):
        return str(self.name)


class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    researcher = models.CharField(max_length=256)
    date = models.DateField()
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol = models.ForeignKey(
        Protocol,
        on_delete=models.RESTRICT,
        limit_choices_to={"category": Protocol.MSRUN_PROTOCOL},
    )
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample = models.ForeignKey(Sample, on_delete=models.RESTRICT)

    # Two runs that share researcher, date, protocol, and sample would be
    # indistinguishable, thus we restrict the database to ensure that
    # combination is unique. Constraint below assumes a researcher runs a
    # sample/protocol combo only once a day.
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["researcher", "date", "protocol", "sample"],
                name="unique_msrun",
            )
        ]

    def clean(self):
        super().clean()

        if self.protocol.category != Protocol.MSRUN_PROTOCOL:
            raise ValidationError(
                "Protocol category for an MSRun must be of type "
                f"{Protocol.MSRUN_PROTOCOL}"
            )


class PeakGroupSet(models.Model):
    id = models.AutoField(primary_key=True)
    filename = models.CharField(
        max_length=256,
        unique=True,
        help_text="Unique name of the source-file or dataset containing "
        "a researcher-defined set of peak groups and their associated data",
    )
    imported_timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return str(f"{self.filename} at {self.imported_timestamp}")


class PeakGroup(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        help_text="Compound or isomer group name [e.g. citrate/isocitrate]",
    )
    formula = models.CharField(
        max_length=256, help_text="molecular formula of the compound [e.g. C6H12O6]"
    )
    ms_run = models.ForeignKey(
        MSRun,
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="database identifier of the MS run this PeakGroup was derived from",
    )
    compounds = models.ManyToManyField(
        Compound,
        related_name="peak_groups",
        help_text="database identifier(s) for the TraceBase compound(s) that this PeakGroup describes",
    )
    peak_group_set = models.ForeignKey(
        PeakGroupSet,
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="source file or dataset this PeakGroup was derived from",
    )

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    @cached_property
    def total_abundance(self):
        """
        Total ion counts for this compound. Accucor provides this in the tab
        "pool size". Calculated by summing the corrected_abundance of all
        Measurements for this compound.
        """
        return self.peak_data.all().aggregate(
            corrected_abundance=Sum("corrected_abundance")
        )["corrected_abundance"]

    @cached_property
    def enrichment_fraction(self):
        """
        enrichment fraction - for this PeakGroup in this sample, weighted
        average of the fraction of labeled atoms. "What fraction of carbons are
        labeled in this compound" = sum of all (PeakData.fraction *
        PeakData.labeled_count) /
        PeakGroup.Compound.num_atoms(PeakData.labeled_element).). Calculated
        without using labeled_count = 0.
        """
        enrichment_sum = 0.0
        for peak_data in self.peak_data.all():
            enrichment_sum = enrichment_sum + (
                peak_data.fraction * peak_data.labeled_count
            )
        compound = self.compounds.first()
        return enrichment_sum / compound.atom_count(peak_data.labeled_element)

    @cached_property
    def enrichment_abundance(self):
        """
        enrichment abundance - abundance of labeled atoms in this compound =
        PeakGroup.total_abundance * PeakGroup:enrichment_fraction
        """
        return self.total_abundance * self.enrichment_fraction

    @cached_property
    def normalized_labeling(self):
        """
        normalized labeling - enrichment in this compound normalized to the
        enrichment in the tracer compound from the final serum timepoint. =
        PeakGroup.enrichment_fraction / PeakGroup.enrichment_fraction = this
        compound / tracer compound in serum
        """

        try:
            final_serum_sample = (
                Sample.objects.filter(animal_id=self.ms_run.sample.animal.id)
                .filter(tissue__name=Tissue.SERUM_TISSUE_NAME)
                .latest("time_collected")
            )
            serum_peak_group = (
                PeakGroup.objects.filter(ms_run__sample_id=final_serum_sample.id)
                .filter(compounds__id=self.ms_run.sample.animal.tracer_compound.id)
                .get()
            )
            normalized_labeling = (
                self.enrichment_fraction / serum_peak_group.enrichment_fraction
            )

        except Sample.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labeling for "
                f"{self.ms_run.sample}:{self}, "
                "associated 'Serum' sample not found."
            )
            normalized_labeling = None

        except PeakGroup.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labeling for "
                f"{self.ms_run.sample}:{self}, "
                "PeakGroup for associated 'Serum' sample not found."
            )
            normalized_labeling = None

        return normalized_labeling

    class Meta:
        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["name", "ms_run"],
                name="unique_peakgroup",
            ),
        ]

    def __str__(self):
        return str(self.name)


class PeakData(models.Model, TracerLabeledClass):
    """
    PeakData is a single observation (at the most atomic level) of a MS-detected molecule.
    For example, this could describe the data for M+2 in glucose from mouse 345 brain tissue.
    """

    id = models.AutoField(primary_key=True)
    peak_group = models.ForeignKey(
        PeakGroup, on_delete=models.CASCADE, null=False, related_name="peak_data"
    )
    labeled_element = models.CharField(
        max_length=1,
        null=True,
        choices=TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
        default=TracerLabeledClass.CARBON,
        blank=True,
        help_text="the type of element that is labeled in this observation (e.g. C, H, O)",
    )
    labeled_count = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(0),
            MaxValueValidator(TracerLabeledClass.MAX_LABELED_ATOMS),
        ],
        help_text="The number of labeled atoms (M+) observed relative to the "
        "presumed compound referred to in the peak group.",
    )
    raw_abundance = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="ion counts or raw abundance of this observation",
    )
    corrected_abundance = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="ion counts corrected for natural abundance of isotopomers",
    )
    med_mz = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="median mass/charge value of this measurement",
    )
    med_rt = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="median retention time value of this measurement",
    )

    @cached_property
    def fraction(self):
        """
        fraction - the corrected abundance of this labeled form as a fraction
        of the total abundance for all corrected forms in this PeakGroup.
        Accucor calculates this as "Normalized", but here renaming from
        "normalized_abundance" to avoid confusion with other variables like
        "normalized labeling"
        """
        return self.corrected_abundance / self.peak_group.total_abundance

    class Meta:
        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["peak_group", "labeled_element", "labeled_count"],
                name="unique_peakdata",
            )
        ]
