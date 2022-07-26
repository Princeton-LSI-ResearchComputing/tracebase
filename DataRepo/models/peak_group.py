import warnings

from django.db import models
from django.db.models import Sum
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.utilities import atom_count_in_formula


class PeakGroup(HierCachedModel):

    parent_related_key_name = "msrun"
    # Leaf

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
        to="DataRepo.MSRun",
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The MS Run this PeakGroup belongs to.",
    )
    compounds = models.ManyToManyField(
        to="DataRepo.Compound",
        related_name="peak_groups",
        help_text="The compound(s) that this PeakGroup is presumed to represent.",
    )
    peak_group_set = models.ForeignKey(
        to="DataRepo.PeakGroupSet",
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_groups",
        help_text="The source file this PeakGroup came from.",
    )

    def atom_count(self, atom):
        return atom_count_in_formula(self.formula, atom)

    # @cached_function is *slower* than uncached
    @cached_property
    def total_abundance(self):
        """
        Total ion counts for this compound.
        Accucor provides this in the tab "pool size".
        Sum of the corrected_abundance of all PeakData for this PeakGroup.
        """
        # Note: If the measured compound does not contain one of the labeled atoms from the tracer compounds, including
        # counts from peakdata records linked to a label of an atom that is not in the measured compound would be
        # invalid.  However, such records should not exist, and if they do, their abundance would be 0, so this code
        # assumes that to be the case.
        return self.peak_data.all().aggregate(
            total_abundance=Sum("corrected_abundance", default=0)
        )["total_abundance"]

    @property  # type: ignore
    @cached_function
    def peak_labeled_elements(self):
        """
        Gets labels present among any of the tracers in the infusate IF the elements are present in the supplied
        (measured) compounds.  Basically, if the supplied compound contains an element that is a labeled element in any
        of the tracers, included in the returned list.
        """
        peak_labeled_elements = []
        compound_recs = self.compounds.all()
        for compound_rec in compound_recs:
            for (
                tracer_labeled_element
            ) in self.msrun.sample.animal.tracer_labeled_elements:
                if (
                    compound_rec.atom_count(tracer_labeled_element) > 0
                    and tracer_labeled_element not in peak_labeled_elements
                ):
                    peak_labeled_elements.append(tracer_labeled_element)
        return peak_labeled_elements

    @property  # type: ignore
    @cached_function
    def enrichment_fractions(self):
        """
        A weighted average of the fraction of labeled atoms for this PeakGroup
        in this sample.
        i.e. The fraction of carbons that are labeled in this PeakGroup compound
        Sum of all (PeakData.fraction * PeakData.labeled_count) /
            PeakGroup.Compound.num_atoms(PeakData.labeled_element)
        """
        from DataRepo.models.peak_data import PeakData
        from DataRepo.models.peak_data_label import PeakDataLabel

        enrichment_fractions = {}
        compound = None
        error = False
        msg = ""

        try:
            compound = self.compounds.first()
            peak_labeled_elements = self.peak_labeled_elements
            if len(peak_labeled_elements) == 0:
                raise NoCommonLabels(self)
            for measured_element in peak_labeled_elements:
                # Calculate the numerator
                element_enrichment_sum = 0.0
                label_pd_recs = self.peak_data.filter(
                    labels__element__exact=measured_element
                )
                # This assumes that if there are any label_pd_recs for this measured elem, the calculation is valid
                if label_pd_recs.count() == 0:
                    raise PeakData.DoesNotExist()
                for label_pd_rec in label_pd_recs:
                    # This assumes the PeakDataLabel unique constraint: peak_data, element
                    label_rec = label_pd_rec.labels.get(element__exact=measured_element)
                    # And this assumes that label_rec must exist because of the filter above the loop
                    element_enrichment_sum = element_enrichment_sum + (
                        label_pd_rec.fraction * label_rec.count
                    )

                # Calculate the denominator
                # This assumes that multiple measured compounds for the same PeakGroup are composed of the same elements
                atom_count = compound.atom_count(measured_element)

                enrichment_fractions[measured_element] = (
                    element_enrichment_sum / atom_count
                )

        except (AttributeError, TypeError, NoCommonLabels) as e:
            error = True
            # The last 2 should not happen since the fields in PeakDataLabel are null=False, but to hard against
            # unexpected DB changes...
            if compound is None:
                msg = "No compounds were associated with PeakGroup"
            elif e.__class__.__name__ == "NoCommonLabels":
                # NoCommonLabels is meaningless if there are no linked compounds (above)
                error = False
                raise e
            elif label_rec.count is None:
                msg = "Labeled count missing from PeakDataLabel"
            elif label_rec.element is None:
                msg = "Labeled element missing from PeakDataLabel"
            else:
                raise e
        except (PeakData.DoesNotExist, PeakDataLabel.DoesNotExist):
            error = True
            msg = (
                f"PeakDataLabel record missing for element {measured_element}.  There should exist a PeakData record "
                "for every tracer labeled element common with the the measured compound, even if the abundance is 0."
            )
        except PeakDataLabel.MultipleObjectsReturned:
            error = True
            # This should not happen bec it would violate the PeakDataLabel unique constraint, but to hard against
            # unexpected DB changes...
            msg = (
                f"PeakDataLabel returned multiple records for element {measured_element} linked to the same PeakData "
                "record."
            )
        finally:
            if error:
                warnings.warn(
                    f"Unable to compute enrichment_fractions for {self.msrun.sample}:{self}, {msg}."
                )
                return None

        return enrichment_fractions

    @property  # type: ignore
    @cached_function
    def enrichment_abundances(self):
        """
        This abundance of labeled atoms in this compound.
        PeakGroup.total_abundance * PeakGroup.enrichment_fractions
        """
        try:
            enrichment_abundances = {}
            for elem in self.enrichment_fractions.keys():
                # If self.enrichment_fractions is None, it will be handled in the except
                enrichment_abundances[elem] = (
                    self.total_abundance * self.enrichment_fractions[elem]
                )
        except (AttributeError, TypeError):
            enrichment_abundances = None
        return enrichment_abundances

    @property  # type: ignore
    @cached_function
    def normalized_labelings(self):
        """
        The enrichment in this compound normalized to the enrichment in the
        tracer compound from the final serum timepoint.
        ThisPeakGroup.enrichment_fractions / SerumTracerPeakGroup.enrichment_fractions
        """
        from DataRepo.models.sample import Sample

        try:
            # An animal can have no tracer_compound (#312 & #315)
            # And without the enrichment_fractions check, deleting a tracer can result in:
            #   TypeError: unsupported operand type(s) for /: 'NoneType' and 'float'
            # in test: test_models.DataLoadingTests.test_peak_group_total_abundance_zero
            if (
                self.msrun.sample.animal.infusate.tracers.count() > 0
                and self.enrichment_fractions is not None
                and self.msrun.sample.animal.serum_tracers_enrichment_fractions
                is not None
            ):
                normalized_labelings = {}
                for elem in self.enrichment_fractions.keys():
                    normalized_labelings[elem] = (
                        self.enrichment_fractions[elem]
                        / self.msrun.sample.animal.serum_tracers_enrichment_fractions[
                            elem
                        ]
                    )
            else:
                normalized_labelings = None
        except Sample.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labelings for "
                f"{self.msrun.sample}:{self}, "
                "associated 'serum' sample not found."
            )
            normalized_labelings = None

        except PeakGroup.DoesNotExist:
            warnings.warn(
                "Unable to compute normalized_labelings for "
                f"{self.msrun.sample}:{self}, "
                "PeakGroup for associated 'serum' sample not found."
            )
            normalized_labelings = None

        return normalized_labelings

    # @cached_function is *slower* than uncached
    @cached_property
    def animal(self):
        """Convenient instance method to cache the animal this PeakGroup came from"""
        return self.msrun.sample.animal

    @property  # type: ignore
    @cached_function
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
            return False

    @property  # type: ignore
    @cached_function
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

    @property  # type: ignore
    @cached_function
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
        elif not self.from_serum_sample:
            warnings.warn(f"{self.name} is not from a serum sample msrun.")
            return False
        elif not self.animal.tracer_infusion_concentration:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_concentration."
            )
            return False
        elif not self.animal.tracer_infusion_rate:
            warnings.warn(
                f"Animal {self.animal.name} has no annotated tracer_infusion_rate."
            )
            return False
        return True

    # @cached_function is *slower* than uncached
    @cached_property
    def can_compute_body_weight_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can utilize
        the associated animal.body_weight
        """
        if not self.animal.body_weight:
            warnings.warn(f"Animal {self.animal.name} has no annotated body_weight.")
            return False
        else:
            return True

    @property  # type: ignore
    @cached_function
    def can_compute_intact_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using fully-labeled/intact measurements of a tracer's
        peakdata.  Returns the peakdata.fraction, if it exists and is greater
        than zero.
        """
        from DataRepo.models.peak_data import PeakData

        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False
        else:
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

    @property  # type: ignore
    @cached_function
    def can_compute_average_tracer_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using averaged enrichment measurements of a tracer's
        peakdata.
        """
        if not self.can_compute_tracer_rates:
            warnings.warn(f"{self.name} cannot compute tracer rates.")
            return False
        else:
            if self.enrichment_fractions and self.enrichment_fractions > 0:
                return True
            else:
                warnings.warn(f"PeakGroup {self.name} has no enrichment_fractions.")
                return False

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        else:
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

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_gram(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        else:
            return (
                self.rate_disappearance_intact_per_gram
                - self.animal.tracer_infusion_rate
                * self.animal.tracer_infusion_concentration
            )

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_disappearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_animal(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_rates:
            warnings.warn(f"{self.name} cannot compute intact tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_appearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_gram(self):
        """
        Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction'
        in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        else:
            return (
                self.animal.tracer_infusion_concentration
                * self.animal.tracer_infusion_rate
                / self.enrichment_fractions
            )

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_gram(self):
        """
        Ra_avg_g = Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        else:
            return (
                self.rate_disappearance_average_per_gram
                - self.animal.tracer_infusion_concentration
                * self.animal.tracer_infusion_rate
            )

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg)
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_disappearance_average_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg)
        Ra_avg = Ra_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_average_tracer_rates:
            warnings.warn(f"{self.name} cannot compute average tracer rate.")
            return None
        elif not self.can_compute_body_weight_tracer_rates:
            warnings.warn(
                f"{self.name} cannot compute per-animal tracer rate (missing body_weight)."
            )
            return None
        else:
            return self.rate_appearance_average_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
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
        else:
            return (
                self.rate_appearance_average_per_gram * self.animal.tracer_labeled_count
            )

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


class NoCommonLabels(Exception):
    def __init__(self, peak_group):
        msg = (
            f"PeakGroup {peak_group.name} found associated with a measured compound "
            f"{','.join(list(peak_group.compounds.all().values_list('name', flat=True)))} that contains no elements "
            "common with the labeled elements among the tracers in the infusate "
            f"[{peak_group.msrun.sample.animal.infusate.name}]."
        )
        super().__init__(msg)
        self.peak_group = peak_group
