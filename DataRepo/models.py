import warnings
from datetime import date, timedelta

import pandas as pd
from chempy import Substance
from chempy.util.periodic import atomic_number
from django.apps import apps
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
    except (ValueError, AttributeError):
        warnings.warn(f"{atom} not found in list of elements")
        count = None
    else:
        if count is None:
            # Valid atom, but not in formula
            count = 0
    return count


def get_all_models():
    """
    Retrieves all models from DataRepo and returns them as a list
    """
    return list(apps.all_models["DataRepo"].values())


def get_model_fields(model):
    """
    Retrieves all non-auto- and non-relation- fields from the supplied model and returns as a list
    """
    return list(
        filter(
            lambda x: x.get_internal_type() != "AutoField"
            and not getattr(x, "is_relation"),
            model._meta.get_fields(),
        )
    )


def get_all_fields_named(target_field):
    """
    Dynamically retrieves all fields from any model with a specific name
    """
    models = get_all_models()
    found_fields = []
    for model in models:
        fields = list(get_model_fields(model))
        for field in fields:
            if field.name == target_field:
                found_fields.append([model, field])
    return found_fields


def get_researchers():
    """
    Get a list of distinct researcher names that is the union of values in researcher fields from any model
    """
    target_field = "researcher"
    researchers = []
    # Get researcher names from any model containing a "researcher" field
    fields = get_all_fields_named(target_field)
    for field_info in fields:
        model = field_info[0]
        researchers += list(
            map(
                lambda x: x[target_field], model.objects.values(target_field).distinct()
            )
        )
    unique_researchers = list(pd.unique(researchers))
    return unique_researchers


class Protocol(models.Model):

    MSRUN_PROTOCOL = "msrun_protocol"
    ANIMAL_TREATMENT = "animal_treatment"
    CATEGORY_CHOICES = [
        (MSRUN_PROTOCOL, "LC-MS Run Protocol"),
        (ANIMAL_TREATMENT, "Animal Treatment"),
    ]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="Unique name of the protocol.",
    )
    description = models.TextField(
        blank=True,
        help_text="Full text of the protocol's methods.",
    )
    category = models.CharField(
        max_length=256,
        choices=CATEGORY_CHOICES,
        help_text="Classification of the protocol, "
        "e.g. an animal treatment or MSRun procedure.",
    )

    @classmethod
    def retrieve_or_create_protocol(
        cls, protocol_input, category=None, provisional_description=None
    ):
        """
        retrieve or create a protocol, based on input.
        protocol_input can either be a name or an integer (protocol_id)
        """

        created = False

        try:
            protocol = Protocol.objects.get(id=protocol_input)
        except ValueError:
            # protocol_input must not be an integer; try the name
            try:
                protocol, created = Protocol.objects.get_or_create(
                    name=protocol_input,
                    category=category,
                )
                if created:
                    # add the provisional description
                    if provisional_description is not None:
                        protocol.description = provisional_description
                        protocol.full_clean()
                        protocol.save()

            except Protocol.DoesNotExist as e:
                raise Protocol.DoesNotExist(
                    f"Protocol ID {protocol_input} does not exist."
                ) from e

        except Protocol.DoesNotExist as e:
            # protocol_input was an integer, but was not found
            print(f"Protocol ID {protocol_input} does not exist.")
            raise e
        return (protocol, created)

    class Meta:
        verbose_name = "protocol"
        verbose_name_plural = "protocols"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


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

    @property
    def hmdb_url(self):
        "Returns the url to the compound's hmdb record"
        return f"{self.HMDB_CPD_URL}/{self.hmdb_id}"

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    class Meta:
        verbose_name = "compound"
        verbose_name_plural = "compounds"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class Study(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A succinct name for the study, which is a collection of "
        "one or more series of animals and their associated data.",
    )
    description = models.TextField(
        blank=True,
        help_text="A long form description for the study which may include "
        "the experimental design process, citations, and other relevant details.",
    )

    class Meta:
        verbose_name = "study"
        verbose_name_plural = "studies"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class Animal(models.Model, TracerLabeledClass):

    FEMALE = "F"
    MALE = "M"
    SEX_CHOICES = [(FEMALE, "female"), (MALE, "male")]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="A unique name or lab identifier of the source animal for a series of studied samples.",
    )
    tracer_compound = models.ForeignKey(
        Compound,
        on_delete=models.RESTRICT,
        null=True,
        help_text="The compound which was used as the tracer (i.e. infusate). "
        "The tracer is the labeled compound that is infused into the animal.",
    )
    # NOTE: encoding labeled atom as the atom's symbol, NOT the full element
    # name, as I have seen in some example files
    tracer_labeled_atom = models.CharField(
        max_length=1,
        null=True,
        choices=TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
        default=TracerLabeledClass.CARBON,
        blank=True,
        help_text="The type of atom that is labeled in the tracer compound "
        '(e.g. "C", "H", "O").',
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
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The rate of tracer infusion in microliters/min/gram of body weight of the animal (ul/min/g).",
    )
    tracer_infusion_concentration = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The millimolar concentration of the tracer in the solution that was infused (mM).",
    )
    genotype = models.CharField(
        max_length=256, help_text="The laboratory standardized genotype of the animal."
    )
    body_weight = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The weight (in grams) of the animal at the time of sample collection.",
    )
    age = models.DurationField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The age of the animal at the time of sample collection.",
    )
    sex = models.CharField(
        max_length=1,
        null=True,
        blank=True,
        choices=SEX_CHOICES,
        help_text='The sex of the animal ("male" or "female").',
    )
    diet = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text='The feeding descriptor for the animal [e.g. "LabDiet Rodent 5001"].',
    )
    feeding_status = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="The laboratory coded dietary state for the animal, "
        'also referred to as "Animal State" (e.g. "fasted").',
    )
    studies = models.ManyToManyField(
        Study,
        related_name="animals",
        help_text="The experimental study(ies) the the animal is associated with.",
    )
    treatment = models.ForeignKey(
        Protocol,
        on_delete=models.RESTRICT,
        null=True,
        blank=True,
        related_name="animals",
        limit_choices_to={"category": Protocol.ANIMAL_TREATMENT},
        help_text="The laboratory controlled label of the actions taken on an animal.",
    )

    @cached_property
    def all_serum_samples(self):
        """
        all_serum_samples() in an instance method that returns all the serum
        samples removed from the calling animal object, ordered by the time they
        were collected from the animal, which is recorded as the time
        elapsed/duration from the initiation of infusion or treatment,
        typically.
        """
        return (
            self.samples.filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .all()
        )

    @cached_property
    def final_serum_sample(self):
        """
        final_serum_sample in an instance method that returns the last single
        serum sample removed from the animal, based on the time elapsed/duration
        from the initiation of infusion or treatment, typically.  If the animal
        has no serum samples or if the retrieved serum sample has no annotated
        time_collected, a warning will be issued.
        """

        final_serum_sample = (
            self.samples.filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .last()
        )

        if final_serum_sample is None:
            warnings.warn(f"Animal {self.name} has no 'serum' samples.")

        if final_serum_sample and not final_serum_sample.time_collected:
            warnings.warn(
                f"The Final serum sample {final_serum_sample.name} for"
                f"Animal {self.name} is missing a time_collected value."
            )

        return final_serum_sample

    @cached_property
    def final_serum_sample_id(self):
        """
        final_serum_sample_time_collected in an instance method that returns the time_collected of the last single
        serum sample removed from the animal, based on the time elapsed/duration from the initiation of infusion or
        treatment.  If the animal has no serum samples or if the retrieved serum sample has no annotated
        time_collected, a warning will be issued.
        """

        fss = self.final_serum_sample()
        id = None
        if final_serum_sample and final_serum_sample.time_collected:
            id = final_serum_sample.id

        return id

    @cached_property
    def all_serum_samples_tracer_peak_groups(self):
        """
        Instance method that returns a list of all peak groups assayed from all
        serum samples on an animal
        """
        all_serum_samples_tracer_peak_groups = []
        for serum_sample in self.all_serum_samples.all():
            # Add the animal's serum samples' peak groups to all_serum_samples_tracer_peak_groups
            all_serum_samples_tracer_peak_groups.extend(
                list(serum_sample.peak_groups(self.tracer_compound))
            )
        return all_serum_samples_tracer_peak_groups

    @cached_property
    def final_serum_sample_tracer_peak_group(self):
        """
        final_serum_sample_tracer_peak_group is an instance method that returns
        the very last recorded PeakGroup obtained from the Animal's final serum
        sample from the last date it was measured/assayed
        """
        if not self.final_serum_sample:
            warnings.warn(f"Animal {self.name} has no final serum sample.")
            return None
        return (
            self.final_serum_sample.peak_groups(self.tracer_compound)
            .order_by("msrun__date")
            .last()
        )

    @cached_property
    def final_serum_sample_tracer_peak_data(self):
        """
        final_serum_sample_tracer_peak_data is an instance method that returns
        all the PeakData from the very last recorded PeakGroup obtained from the
        Animal's final serum sample from the last date it was measured/assayed
        """
        final_peak_group = self.final_serum_sample_tracer_peak_group
        if not final_peak_group:
            return None
        return final_peak_group.peak_data

    @cached_property
    def intact_tracer_peak_data(self):
        """
        intact_tracer_peak_data is an instance method that returns the peak data
        matching the intact tracer (i.e. the labeled_count matches the tracer_labeled_count)
        """
        if not self.tracer_labeled_count:
            warnings.warn(f"Animal {self.name} has no annotated tracer_labeled_count")
            return None
        final_peak_data = self.final_serum_sample_tracer_peak_data
        if not final_peak_data:
            return None
        return final_peak_data.filter(labeled_count=self.tracer_labeled_count).get()

    @cached_property
    def final_serum_tracer_rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_disappearance_intact_per_gram
        )

    @cached_property
    def final_serum_tracer_rate_appearance_intact_per_gram(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact_g, or
        sometimes Fcirc_intact. This is calculated on the Animal's
        final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return self.final_serum_sample_tracer_peak_group.rate_appearance_intact_per_gram

    @cached_property
    def final_serum_tracer_rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_disappearance_intact_per_animal
        )

    @cached_property
    def final_serum_tracer_rate_appearance_intact_per_animal(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact, or sometimes
        Fcirc_intact_per_mouse. This is calculated on the Animal's final serum
        sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_appearance_intact_per_animal
        )

    @cached_property
    def final_serum_tracer_rate_disappearance_average_per_gram(self):
        """
        Also referred to as Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction' in
        nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_disappearance_average_per_gram
        )

    @cached_property
    def final_serum_tracer_rate_appearance_average_per_gram(self):
        """
        Also referred to as Ra_avg_g, and sometimes referred to as Fcirc_avg.
        Equivalent to Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_appearance_average_per_gram
        )

    @cached_property
    def final_serum_tracer_rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg), also referred to as Rd_avg
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_disappearance_average_per_animal
        )

    @cached_property
    def final_serum_tracer_rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg), also referred to as Ra_avg or sometimes
        Fcirc_avg_per_mouse. Ra_avg = Ra_avg_g * 'Body Weight'' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_appearance_average_per_animal
        )

    @cached_property
    def final_serum_tracer_rate_appearance_average_atom_turnover(self):
        """
        also referred to as Fcirc_avg_atom.  Originally defined as
        Fcirc_avg * PeakData:label_count in nmol atom / min / gram
        turnover of atoms in this compound, e.g. "nmol carbon / min / g"
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        return (
            self.final_serum_sample_tracer_peak_group.rate_appearance_average_atom_turnover
        )

    class Meta:
        verbose_name = "animal"
        verbose_name_plural = "animals"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    def clean(self):
        super().clean()

        if self.treatment is not None:
            if self.treatment.category != Protocol.ANIMAL_TREATMENT:
                raise ValidationError(
                    "Protocol category for an Animal must be of type "
                    f"{Protocol.ANIMAL_TREATMENT}"
                )


class Tissue(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text='The laboratory standardized name for this tissue type (e.g. "serum", "brain", "liver").',
    )
    description = models.TextField(
        blank=True,
        help_text="Description of this tissue type.",
    )

    class Meta:
        verbose_name = "tissue"
        verbose_name_plural = "tissues"
        ordering = ["name"]

    SERUM_TISSUE_PREFIX = "serum_plasma"

    def __str__(self):
        return str(self.name)


class Sample(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="The unique name of the biological sample.",
    )
    date = models.DateField(
        default=date.today, help_text="The date the sample was collected."
    )
    researcher = models.CharField(
        max_length=256,
        help_text='The name of the researcher who prepared the sample (e.g. "Alex Medina").',
    )
    animal = models.ForeignKey(
        Animal,
        on_delete=models.CASCADE,
        null=False,
        related_name="samples",
        help_text="The source animal from which the sample was extracted.",
    )
    tissue = models.ForeignKey(
        Tissue,
        on_delete=models.RESTRICT,
        null=False,
        help_text="The tissue type this sample was taken from.",
    )

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
        help_text="The time, relative to an infusion timepoint, "
        "that a sample was extracted from an animal.",
    )

    def peak_groups(self, compound=None):
        """
        Retrieve a list of PeakGroup objects for a sample instance.  If an optional compound is passed (e.g.
        animal.tracer_compound), then is it used to filter the PeakGroup queryset to a specific compound's peakgroup(s)
        [if multiple PeakGroupSets exist].
        """

        peak_groups = PeakGroup.objects.filter(msrun__sample_id=self.id)
        if compound:
            peak_groups = peak_groups.filter(compounds__id=compound.id)
        return peak_groups.all()

    def peak_data(self, compound=None):
        """
        Retrieve a list of PeakData objects for a sample instance.  If an optional compound is passed (e.g.
        animal.tracer_compound), then is it used to filter the PeakData queryset to a specific peakgroup.
        """

        peakdata = PeakData.objects.filter(peak_group__msrun__sample_id=self.id)

        if compound:
            peakdata = peakdata.filter(peak_group__compounds__id=compound.id)

        return peakdata.all()

    @cached_property
    def is_serum_sample(self):
        """returns True if the sample is flagged as a "serum" sample"""

        # NOTE: this logic may have to change in the future
        if self.tissue in Tissue.objects.filter(
            name__startswith=Tissue.SERUM_TISSUE_PREFIX
        ):
            return True
        else:
            return False

    class Meta:
        verbose_name = "sample"
        verbose_name_plural = "samples"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    researcher = models.CharField(
        max_length=256,
        help_text="The name of the researcher who ran the mass spectrometer.",
    )
    date = models.DateField(
        help_text="The date that the mass spectrometer was run.",
    )
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol = models.ForeignKey(
        Protocol,
        on_delete=models.RESTRICT,
        limit_choices_to={"category": Protocol.MSRUN_PROTOCOL},
        help_text="The protocol that was used for this mass spectrometer run.",
    )
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample = models.ForeignKey(
        Sample,
        on_delete=models.RESTRICT,
        related_name="msruns",
        help_text="The sample that was run on the mass spectrometer.",
    )

    class Meta:
        verbose_name = "mass spectrometry run"
        verbose_name_plural = "mass spectrometry runs"
        ordering = ["date", "researcher", "sample__name", "protocol__name"]

        """
        MS runs that share researcher, date, protocol, and sample would be
        indistinguishable, thus we restrict the database to ensure that
        combination is unique. Constraint below assumes a researcher runs a
        sample/protocol combo only once a day.
        """
        constraints = [
            models.UniqueConstraint(
                fields=["researcher", "date", "protocol", "sample"],
                name="unique_msrun",
            )
        ]

    def __str__(self):
        return str(
            f"MS run of sample {self.sample.name} with {self.protocol.name} by {self.researcher} on {self.date}"
        )

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
        help_text="The unique name of the source-file or dataset containing "
        "a researcher-defined set of peak groups and their associated data",
    )
    imported_timestamp = models.DateTimeField(
        auto_now_add=True,
        help_text="The timestamp for when the source datafile was imported.",
    )

    class Meta:
        verbose_name = "peak group set"
        verbose_name_plural = "peak group sets"
        ordering = ["filename"]

    def __str__(self):
        return str(f"{self.filename} at {self.imported_timestamp}")


class PeakGroup(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        help_text='The compound or isomer group name (e.g. "citrate/isocitrate", "glucose").',
    )
    formula = models.CharField(
        max_length=256,
        help_text='The molecular formula of the compound (e.g. "C6H12O6").',
    )
    msrun = models.ForeignKey(
        MSRun,
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The MS Run this PeakGroup belongs to.",
    )
    compounds = models.ManyToManyField(
        Compound,
        related_name="peak_groups",
        help_text="The compound(s) that this PeakGroup is presumed to represent.",
    )
    peak_group_set = models.ForeignKey(
        PeakGroupSet,
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The source file this PeakGroup came from.",
    )

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    @cached_property
    def total_abundance(self):
        """
        Total ion counts for this compound.

        Accucor provides this in the tab "pool size".

        Sum of the corrected_abundance of all PeakData for this PeakGroup.

        """
        return self.peak_data.all().aggregate(
            total_abundance=Sum("corrected_abundance")
        )["total_abundance"]

    @cached_property
    def enrichment_fraction(self):
        """
        A weighted average of the fraction of labeled atoms for this PeakGroup
        in this sample.

        i.e. The fraction of carbons that are labeled in this PeakGroup compound

        Sum of all (PeakData.fraction * PeakData.labeled_count) /
            PeakGroup.Compound.num_atoms(PeakData.labeled_element)
        """
        enrichment_fraction = None

        try:
            enrichment_sum = 0.0
            for peak_data in self.peak_data.all():
                enrichment_sum = enrichment_sum + (
                    peak_data.fraction * peak_data.labeled_count
                )

            compound = self.compounds.first()
            atom_count = compound.atom_count(peak_data.labeled_element)

            enrichment_fraction = enrichment_sum / atom_count

        except (AttributeError, TypeError):
            if compound is not None:
                msg = "no compounds were associated with PeakGroup"
            elif peak_data.labeled_count is None:
                msg = "labeled_count missing from PeakData"
            elif peak_data.labeled_element is None:
                msg = "labeled_element missing from PeakData"
            else:
                msg = "unknown error occurred"
            warnings.warn(
                "Unable to compute enrichment_fraction for "
                f"{self.msrun.sample}:{self}, {msg}."
            )

        return enrichment_fraction

    @cached_property
    def enrichment_abundance(self):
        """
        This abundance of labeled atoms in this compound.

        PeakGroup.total_abundance * PeakGroup.enrichment_fraction
        """
        return self.total_abundance * self.enrichment_fraction

    @cached_property
    def normalized_labeling(self):
        """
        The enrichment in this compound normalized to the enrichment in the
        tracer compound from the final serum timepoint.

        ThisPeakGroup.enrichment_fraction / SerumTracerPeakGroup.enrichment_fraction
        """

        try:
            final_serum_sample = (
                Sample.objects.filter(animal_id=self.msrun.sample.animal.id)
                .filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
                .latest("time_collected")
            )
            serum_peak_group = (
                PeakGroup.objects.filter(msrun__sample_id=final_serum_sample.id)
                .filter(compounds__id=self.msrun.sample.animal.tracer_compound.id)
                .get()
            )
            normalized_labeling = (
                self.enrichment_fraction / serum_peak_group.enrichment_fraction
            )

        except Sample.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labeling for "
                f"{self.msrun.sample}:{self}, "
                "associated 'serum' sample not found."
            )
            normalized_labeling = None

        except PeakGroup.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labeling for "
                f"{self.msrun.sample}:{self}, "
                "PeakGroup for associated 'serum' sample not found."
            )
            normalized_labeling = None

        return normalized_labeling

    @cached_property
    def animal(self):
        """Convenient instance method to cache the animal this PeakGroup came from"""
        return self.msrun.sample.animal

    @cached_property
    def is_tracer_compound_group(self):
        """
        Instance method which returns True if a compound it is associated with
        is also the tracer compound for the animal it came from.  This is
        primarily a check to prevent tracer appearance/disappearance
        calculations from returning values from non-tracer compounds. Uncertain
        whether this is a true concern.
        """
        if self.animal.tracer_compound in self.compounds.all():
            return True
        else:
            warnings.warn(
                f"{self.name} is not the designated tracer for Animal {self.animal.name}."
            )
            return False

    @cached_property
    def from_serum_sample(self):
        """
        Instance method which returns True if a peakgroup was obtained from a
        msrun of a serum sample. Uncertain whether this is a true concern.
        """
        if self.msrun.sample.is_serum_sample:
            return True
        else:
            warnings.warn(f"{self.name} is not from a serum sample msrun.")
            return False

    @cached_property
    def can_compute_tracer_rates(self):
        """
        Instance method which returns True if a peak_group can (feasibly)
        calculate rates of appearance and dissapearance of a tracer group
        """
        if not self.is_tracer_compound_group:
            warnings.warn(
                f"{self.name} is not the designated tracer for Animal {self.animal.name}."
            )
            return False
        if not self.from_serum_sample:
            warnings.warn(f"{self.name} is not from a serum sample msrun.")
            return False
        if not self.animal.tracer_infusion_concentration:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_concentration."
            )
            return False
        if not self.animal.tracer_infusion_rate:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_infusion_rate."
            )
            return False
        return True

    @cached_property
    def can_compute_body_weight_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can utilize
        the associated animal.body_weight
        """
        if not self.animal.body_weight:
            warnings.warn(f"Animal {self.animal.name} has no annotated body_weight.")
            return False
        return True

    @cached_property
    def can_compute_intact_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using fully-labeled/intact measurements of a tracer's
        peakdata.  Returns the peakdata.fraction, if it exists and is greater
        than zero.
        """
        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False

        try:
            intact_peakdata = self.peak_data.filter(
                labeled_count=self.animal.tracer_labeled_count
            ).get()
        except PeakData.DoesNotExist:
            warnings.warn(
                f"PeakGroup {self.name} has no fully labeled/intact peakdata."
            )
            return False

        if (
            intact_peakdata
            and intact_peakdata.fraction
            and intact_peakdata.fraction > 0
        ):
            return True
        else:
            warnings.warn(
                f"PeakGroup {self.name} has no fully labeled/intact peakdata."
            )
            return False

    @cached_property
    def can_compute_average_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using averaged enrichment measurements of a tracer's
        peakdata.
        """
        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False

        if self.enrichment_fraction and self.enrichment_fraction > 0:
            return True
        else:
            warnings.warn(f"PeakGroup {self.name} has no enrichment_fraction.")
            return False

    @cached_property
    def rate_disappearance_intact_per_gram(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None

        fraction = (
            self.peak_data.filter(labeled_count=self.animal.tracer_labeled_count)
            .get()
            .fraction
        )

        return (
            self.animal.tracer_infusion_rate
            * self.animal.tracer_infusion_concentration
            / fraction
        )

    @cached_property
    def rate_appearance_intact_per_gram(self):
        """Rate of Appearance (intact)"""

        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None

        return (
            self.rate_disappearance_intact_per_gram
            - self.animal.tracer_infusion_rate
            * self.animal.tracer_infusion_concentration
        )

    @cached_property
    def rate_disappearance_intact_per_animal(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        if not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        return self.rate_disappearance_intact_per_gram * self.animal.body_weight

    @cached_property
    def rate_appearance_intact_per_animal(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        if not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        return self.rate_appearance_intact_per_gram * self.animal.body_weight

    @cached_property
    def rate_disappearance_average_per_gram(self):
        """
        Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction'
        in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        return (
            self.animal.tracer_infusion_concentration
            * self.animal.tracer_infusion_rate
            / self.enrichment_fraction
        )

    @cached_property
    def rate_appearance_average_per_gram(self):
        """
        Ra_avg_g = Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        return (
            self.rate_disappearance_average_per_gram
            - self.animal.tracer_infusion_concentration
            * self.animal.tracer_infusion_rate
        )

    @cached_property
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg)
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        if not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None

        return self.rate_disappearance_average_per_gram * self.animal.body_weight

    @cached_property
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg)
        Ra_avg = Ra_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        if not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None

        return self.rate_appearance_average_per_gram * self.animal.body_weight

    @cached_property
    def rate_appearance_average_atom_turnover(self):
        """
        turnover of atoms in this compound in nmol atom / min / gram
        """
        if (
            not self.can_compute_average_tracer_rates
            or not self.animal.tracer_labeled_count
        ):
            warnings.warn(
                f"{self.name} cannot compute average tracer turnover of atoms."
            )
            return None
        return self.rate_appearance_average_per_gram * self.animal.tracer_labeled_count

    class Meta:
        verbose_name = "peak group"
        verbose_name_plural = "peak groups"
        ordering = ["name"]

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["name", "msrun"],
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
        help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").',
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
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The ion count of this observation.",
    )
    corrected_abundance = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="The ion counts corrected for natural abundance of isotopomers.",
    )
    med_mz = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The median mass/charge value of this measurement.",
    )
    med_rt = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The median retention time value of this measurement.",
    )

    @cached_property
    def fraction(self):
        """
        The corrected abundance of the labeled element in this PeakData as a
        fraction of the total abundance of the labeled element in this
        PeakGroup.

        Accucor calculates this as "Normalized", but TraceBase renames it to
        "fraction" to avoid confusion with other variables like "normalized
        labeling".

        """
        return self.corrected_abundance / self.peak_group.total_abundance

    class Meta:
        verbose_name = "peak data"
        verbose_name_plural = "peak data"
        ordering = ["peak_group", "labeled_count"]

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["peak_group", "labeled_element", "labeled_count"],
                name="unique_peakdata",
            )
        ]
