from datetime import date, timedelta
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.functional import cached_property

from DataRepo.hier_cached_model import HierCachedModel, cached_function

from .compound import Compound
from .protocol import Protocol
from .study import Study
from .tracerlabeledclass import TracerLabeledClass

class Animal(HierCachedModel, TracerLabeledClass):
    # No parent_related_key_name, because this is a root
    child_related_key_names = ["samples"]

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
        blank=True,
        null=True,
        related_name="animals",
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
        validators=[MinValueValidator(timedelta(seconds=0))],
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

    # @cached_function is *slower* than uncached
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

    @property  # type: ignore
    @cached_function
    def final_serum_sample(self):
        """
        final_serum_sample in an instance method that returns the last single
        serum sample removed from the animal, based on the time elapsed/duration
        from the initiation of infusion or treatment, typically.  If the animal
        has no serum samples or if the retrieved serum sample has no annotated
        time_collected, a warning will be issued.
        """
        final_serum_sample = (
            self.samples.filter(tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX)
            .order_by("time_collected")
            .last()
        )

        if final_serum_sample is None:
            warnings.warn(f"Animal {self.name} has no 'serum' samples.")
        elif not final_serum_sample.time_collected:
            warnings.warn(
                f"The Final serum sample {final_serum_sample.name} for "
                f"Animal {self.name} is missing a time_collected value."
            )

        return final_serum_sample

    @property  # type: ignore
    @cached_function
    def final_serum_sample_id(self):
        """
        final_serum_sample_id in an instance method that returns the id of the last single
        serum sample removed from the animal, based on the time elapsed/duration from the initiation of infusion or
        treatment.  If the animal has no serum samples, a warning will be issued.
        """
        # Note: calling self.final_serum_sample here ran into linting issues with `fss.id` not "existing". Added
        # fss\..* to this list of generated-members in the pylint config to ignore it.
        id = None
        fss = self.final_serum_sample
        if fss and fss.id:
            id = fss.id
        return id

    @property  # type: ignore
    @cached_function
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

    @property  # type: ignore
    @cached_function
    def final_serum_sample_tracer_peak_group(self):
        """
        final_serum_sample_tracer_peak_group is an instance method that returns
        the very last recorded PeakGroup obtained from the Animal's final serum
        sample from the last date it was measured/assayed
        """
        if not self.final_serum_sample:
            warnings.warn(f"Animal {self.name} has no final serum sample.")
            return None
        else:
            return (
                self.final_serum_sample.peak_groups(self.tracer_compound)
                .order_by("msrun__date")
                .last()
            )

    # @cached_function does not work with this method because non-None values are not picklable
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
        else:
            return final_peak_group.peak_data

    @property  # type: ignore
    @cached_function
    def intact_tracer_peak_data(self):
        """
        intact_tracer_peak_data is an instance method that returns the peak data
        matching the intact tracer (i.e. the labeled_count matches the tracer_labeled_count)
        """
        if not self.tracer_labeled_count:
            warnings.warn(f"Animal {self.name} has no annotated tracer_labeled_count")
            return None
        else:
            final_peak_data = self.final_serum_sample_tracer_peak_data
            if not final_peak_data:
                return None
            else:
                return final_peak_data.filter(
                    labeled_count=self.tracer_labeled_count
                ).get()

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_disappearance_intact_per_gram
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_intact_per_gram(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact_g, or
        sometimes Fcirc_intact. This is calculated on the Animal's
        final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_appearance_intact_per_gram
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_disappearance_intact_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_intact_per_animal(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact, or sometimes
        Fcirc_intact_per_mouse. This is calculated on the Animal's final serum
        sample tracer's PeakGroup.
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_appearance_intact_per_animal
            )

    @property  # type: ignore
    @cached_function
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
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_disappearance_average_per_gram
            )

    @property  # type: ignore
    @cached_function
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
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_appearance_average_per_gram
            )

    @property  # type: ignore
    @cached_function
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
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_disappearance_average_per_animal
            )

    @property  # type: ignore
    @cached_function
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
        else:
            return (
                self.final_serum_sample_tracer_peak_group.rate_appearance_average_per_animal
            )

    @property  # type: ignore
    @cached_function
    def final_serum_tracer_rate_appearance_average_atom_turnover(self):
        """
        also referred to as Fcirc_avg_atom.  Originally defined as
        Fcirc_avg * PeakData:label_count in nmol atom / min / gram
        turnover of atoms in this compound, e.g. "nmol carbon / min / g"
        """
        if not self.final_serum_sample_tracer_peak_group:
            warnings.warn(f"Animal {self.name} has no final serum sample peak group.")
            return None
        else:
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

