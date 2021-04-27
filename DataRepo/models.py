import datetime

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models


def value_from_choices_label(label, choices):
    """
    Return the choices value for a given label
    """
    dictionary = {}
    for value, label in choices:
        dictionary[label] = value
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
    
    class Meta:
        ordering = ["-name"]
    
    def __str__(self):
        return self.name


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

    def __str__(self):
        return str(self.name)


class Tissue(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)

    def __str__(self):
        return str(self.name)


class Sample(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    date = models.DateField(default=datetime.date.today)
    researcher = models.CharField(max_length=256)
    animal = models.ForeignKey(
        Animal, on_delete=models.CASCADE, null=False, related_name="samples"
    )
    tissue = models.ForeignKey(Tissue, on_delete=models.RESTRICT, null=False)

    def __str__(self):
        return str(self.name)


class Protocol(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(blank=True)


class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    date = models.DateTimeField(auto_now=False, auto_now_add=True, editable=True)
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol = models.ForeignKey(Protocol, on_delete=models.RESTRICT)
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample = models.ForeignKey(Sample, on_delete=models.RESTRICT)


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

    class Meta:
        # composite key
        unique_together = ("name", "ms_run")

    def __str__(self):
        return str(self.name)


# PeakData is a single observation (at the most atomic level) of a MS-detected molecule.
# For example, this could describe the data for M+2 in glucose from mouse 345 brain tissue.


class PeakData(models.Model, TracerLabeledClass):
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
            MinValueValidator(1),
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

    class Meta:
        # composite key
        unique_together = ("peak_group", "labeled_element", "labeled_count")
