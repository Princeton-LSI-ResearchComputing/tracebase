import warnings
from datetime import timedelta

from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function

from .element_label import ElementLabel
from .protocol import Protocol
from .tissue import Tissue


class Animal(HierCachedModel, ElementLabel):
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
    infusate = models.ForeignKey(
        to="DataRepo.Infusate",
        on_delete=models.RESTRICT,
        null=True,
        blank=True,
        related_name="animal",
        help_text="The solution infused into the animal containing 1 or more tracer compounds at specific "
        "concentrations.",
    )
    infusion_rate = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The rate of infusion of the tracer solution in microliters/min/gram of body weight of the animal "
        "(ul/min/g).",
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
        to="DataRepo.Study",
        related_name="animals",
        help_text="The experimental study(ies) the the animal is associated with.",
    )
    treatment = models.ForeignKey(
        to="DataRepo.Protocol",
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

    @property  # type: ignore
    @cached_function
    def tracer_labeled_elements(self):
        """
        This method returns a unique list of the labeled elements that exist among the tracers as if they were parent
        observations (i.e. count=0 and parent=True).  This is so that Isocorr data canrecord 0 observations for parent
        records.  Accucor data does present data for counts of 0 already.
        """
        # Assuming all samples come from 1 animal, so we're only looking at 1 (any) sample
        tracer_labeled_elements = []
        for tracer in self.infusate.tracers.all():
            for label in tracer.labels.all():
                if label.element not in tracer_labeled_elements:
                    tracer_labeled_elements.append(label.element)
        return tracer_labeled_elements

    @property  # type: ignore
    @cached_function
    def serum_tracers_enrichment_fractions(self):
        """
        A weighted average of the fraction of labeled atoms for this PeakGroup
        in this sample.
        i.e. The fraction of carbons that are labeled in this PeakGroup compound
        Sum of all (PeakData.fraction * PeakData.labeled_count) /
            PeakGroup.Compound.num_atoms(PeakData.labeled_element)
        """
        from DataRepo.models.peak_group import PeakGroup
        from DataRepo.models.sample import Sample
        from DataRepo.models.tissue import Tissue

        tracers_enrichment_fractions = {}
        tracer_compounds = None
        error = False
        msg = ""

        try:
            tracer_compounds = []
            tracer_compound_ids = []
            for tracer in self.infusate.tracers.all():
                tracer_compounds.append(tracer.compound)
                tracer_compound_ids.append(tracer.compound.id)
            if len(tracer_compound_ids) == 0:
                raise NoTracerCompounds(self)
            tracer_labeled_elements = self.tracer_labeled_elements

            # Get the peak group of each tracer compound from the final serum sample
            final_serum_sample = (
                Sample.objects.filter(animal_id=self.id)
                .filter(tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX)
                .latest("time_collected")
            )
            final_serum_tracer_peak_groups = PeakGroup.objects.filter(
                msrun__sample_id=final_serum_sample.id
            ).filter(compounds__id__in=tracer_compound_ids)
            if final_serum_tracer_peak_groups.count() != len(tracer_compounds):
                raise MissingSerumTracerPeakGroups(
                    self,
                    final_serum_sample,
                    final_serum_tracer_peak_groups,
                    tracer_compounds,
                )

            # Count the total numnber of each element among all the tracer compounds
            total_atom_counts = {}
            for tracer_labeled_element in tracer_labeled_elements:
                total_atom_counts[tracer_labeled_element] = 0
            for tracer_compound in tracer_compounds:
                for tracer_labeled_element in tracer_labeled_elements:
                    total_atom_counts[
                        tracer_labeled_element
                    ] += tracer_compound.atom_count(tracer_labeled_element)

            final_serum_tracers_enrichment_sums = {}
            # Sum the element enrichments across all tracer compounds
            for tracer_labeled_element in tracer_labeled_elements:
                final_serum_tracers_enrichment_sums[tracer_labeled_element] = 0.0
                for final_serum_tracer_peak_group in final_serum_tracer_peak_groups:
                    label_pd_recs = final_serum_tracer_peak_group.peak_data.filter(
                        labels__element__exact=tracer_labeled_element
                    )
                    # This assumes that if there are any label_pd_recs for this measured elem, the calculation is valid
                    if label_pd_recs.count() == 0:
                        raise MissingPeakData(
                            final_serum_tracer_peak_group, tracer_labeled_element
                        )
                    for label_pd_rec in label_pd_recs:
                        # This assumes the PeakDataLabel unique constraint: peak_data, element
                        label_rec = label_pd_rec.labels.get(
                            element__exact=tracer_labeled_element
                        )
                        # And this assumes that label_rec must exist because of the filter above the loop
                        final_serum_tracers_enrichment_sums[tracer_labeled_element] += (
                            label_pd_rec.fraction * label_rec.count
                        )

            for tracer_labeled_element in tracer_labeled_elements:
                tracers_enrichment_fractions[tracer_labeled_element] = (
                    final_serum_tracers_enrichment_sums[tracer_labeled_element]
                    / total_atom_counts[tracer_labeled_element]
                )
        except NoTracerCompounds as ntc:
            error = True
            msg = str(ntc)
        except MissingSerumTracerPeakGroups as mstpg:
            error = True
            msg = str(mstpg)
        except MissingPeakData as mpd:
            error = True
            msg = str(mpd)
        finally:
            if error:
                warnings.warn(
                    f"Unable to compute serum_tracers_enrichment_fractions for {final_serum_sample}:{self}, {msg}."
                )
                return None

        return tracers_enrichment_fractions

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


class MissingSerumTracerPeakGroups(Exception):
    def __init__(
        self,
        animal,
        final_serum_sample,
        final_serum_tracer_peak_groups,
        tracer_compounds,
    ):
        msg = (
            f"There is not a peak group [{', '.join(final_serum_tracer_peak_groups.values_list('name', flat=True))}] "
            f"in the final serum sample [{final_serum_sample}] for animal [{animal}] for every tracer compound "
            f"[{', '.join(list(map(lambda x: x.name, tracer_compounds)))}]"
        )
        super().__init__(msg)
        self.animal = animal
        self.final_serum_sample = final_serum_sample
        self.final_serum_tracer_peak_groups = final_serum_tracer_peak_groups
        self.tracer_compounds = tracer_compounds


class MissingPeakData(Exception):
    def __init__(self, final_serum_tracer_peak_group, tracer_labeled_element):
        msg = (
            f"PeakData record missing for element {tracer_labeled_element} in final serum peak group "
            f"{final_serum_tracer_peak_group}.  There should exist a PeakData record for every tracer labeled "
            "element, even if the abundance is 0."
        )
        super().__init__(msg)
        self.final_serum_tracer_peak_group = final_serum_tracer_peak_group
        self.tracer_labeled_element = tracer_labeled_element


class NoTracerCompounds(Exception):
    def __init__(self, animal):
        msg = f"Animal [{animal}] has no tracers."
        super().__init__(msg)
        self.animal = animal
