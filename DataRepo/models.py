import datetime

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.urls import reverse


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
    
    def get_absolute_url(self):
        return reverse('compound_detail', args=[str(self.id)])


class Study(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(blank=True)

    def __str__(self):
        return str(self.name)

    def get_absolute_url(self):
        return reverse('study_detail', args=[str(self.id)])

class Animal(models.Model):

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

    FEMALE = "F"
    MALE = "M"
    SEX_CHOICES = [(FEMALE, "female"), (MALE, "male")]

    MAX_LABELED_COUNT = 20

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
        choices=TRACER_LABELED_ELEMENT_CHOICES,
        default=CARBON,
        blank=True,
    )
    # NOTE: encoding atom count as an integer, NOT a float, as I have seen in
    # some example files
    tracer_labeled_count = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
        validators=[MinValueValidator(1), MaxValueValidator(MAX_LABELED_COUNT)],
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

    def get_absolute_url(self):
        return reverse('animal_detail', args=[str(self.id)])

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
 #       return str(self.name)
         return '{0} ({1})'.format(self.id, self.animal.name)
         
    def get_absolute_url(self):
        return reverse('sample_detail', args=[str(self.id)])

class Protocol(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(blank=True)

    def __str__(self):
        return str(self.name)

    def get_absolute_url(self):
        return reverse('protocol_detail', args=[str(self.id)])

class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    date = models.DateTimeField(auto_now=False, auto_now_add=True, editable=True)
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol = models.ForeignKey(Protocol, on_delete=models.RESTRICT)
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample = models.ForeignKey(Sample, on_delete=models.RESTRICT)
