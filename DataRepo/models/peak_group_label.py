import warnings

from django.db import models
from django.forms.models import model_to_dict
from django.utils.functional import cached_property
from pyparsing import ParseException

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.utilities import atom_count_in_formula


class PeakGroupLabel(HierCachedModel):

    parent_related_key_name = "peak_group"
    # Leaf

    id = models.AutoField(primary_key=True)
    peak_group = models.ForeignKey(
        to="DataRepo.PeakGroup",
        on_delete=models.CASCADE,
        null=False,
        blank=False,
        related_name="labels",
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").',
    )

    class Meta:
        verbose_name = "labeled element"
        verbose_name_plural = "labeled elements"
        ordering = ["peak_group", "element"]
        constraints = [
            models.UniqueConstraint(
                # We could additionally have "count" and "mass_number", but we only want 1 instance of "C" linked to 1
                # peak_data record. And while we could theoretically have both 13C and 14C labels in the same compound,
                # the tracer code doesn't currently support that, so there's no point in allowing that here.
                fields=["peak_group", "element"],
                name="unique_peakgrouplabel",
            )
        ]

    def __str__(self):
        return str(f"{self.element}")

    @property  # type: ignore
    @cached_function
    def enrichment_fraction(self):
        """
        A weighted average of the fraction of labeled atoms for this PeakGroup
        in this sample with this (labeled) element.
        i.e. The fraction of carbons that are labeled in this.PeakGroup compound
        Sum of all (fraction * count) / num_atoms(element)
        """
        from DataRepo.models.peak_data import PeakData
        from DataRepo.models.peak_data_label import PeakDataLabel

        enrichment_fraction = None
        warning = False
        msg = ""

        try:
            # This assumes that multiple measured compounds for the same PeakGroup are composed of the same elements

            # Calculate the denominator
            try:
                atom_count = self.atom_count()
            except ParseException as pe:
                # We're intentionally allowing AttributeError exceptions to pass through to the outer catch to issue a
                # warning because it means there's no formula for the parent PeakGroup, meaning a compound
                # determination could not be confidently made.  However, if there is a formula and it has a
                # PeakGroupLabel that's not in that formula, that's an error/inconsistency which should raise an
                # exception.
                raise NoCommonLabel(self) from pe
            if atom_count == 0:
                raise NoCommonLabel(self)

            # Calculate the numerator
            element_enrichment_sum = 0.0
            label_pd_recs = self.peak_group.peak_data.filter(
                labels__element__exact=self.element
            )

            # This assumes that if there are any label_pd_recs for this measured elem, the calculation is valid
            if label_pd_recs.count() == 0:
                raise PeakData.DoesNotExist()

            for label_pd_rec in label_pd_recs:
                # This assumes the PeakDataLabel unique constraint: peak_data, element
                label_rec = label_pd_rec.labels.get(element__exact=self.element)

                # This assumes that label_rec must exist because of the filter above the loop
                element_enrichment_sum = element_enrichment_sum + (
                    label_pd_rec.fraction * label_rec.count
                )

            enrichment_fraction = element_enrichment_sum / atom_count

        except (AttributeError, TypeError, NoCommonLabel) as e:
            warning = True
            # The last 2 should not happen since the fields in PeakDataLabel are null=False, but to guard against
            # unexpected DB changes...
            if (
                self.peak_group.compounds.count() == 0
                or self.peak_group.formula is None
            ):
                msg = f"No compounds or peak group formula was associated with PeakGroup {self.peak_group}"
            elif isinstance(e, NoCommonLabel):
                # NoCommonLabel is meaningless if there is no formula (above)
                warning = False
                raise e
            elif label_pd_rec.fraction is None:
                msg = (
                    f"PeakData fraction was None from record [{label_pd_rec}] likely because the PeakGroup total "
                    "abundance was 0"
                )
            elif label_rec.count is None:
                msg = f"Labeled count missing from PeakDataLabel record [{label_rec}]"
            elif label_rec.element is None:
                msg = f"Labeled element missing from PeakDataLabel record [{label_rec}]"
            else:
                raise e
        except (PeakData.DoesNotExist, PeakDataLabel.DoesNotExist):
            warning = True
            msg = (
                f"PeakDataLabel record missing for PeakGroup [{self.peak_group}]'s element {self.element}.  There "
                "should exist a PeakData record for every tracer labeled element common with the the measured "
                "compound, even if the abundance is 0."
            )
        except PeakDataLabel.MultipleObjectsReturned:
            warning = True
            # This should not happen bec it would violate the PeakDataLabel unique constraint, but to guard against
            # unexpected DB changes...
            msg = (
                f"PeakDataLabel returned multiple records for element {self.element} linked to the same PeakData "
                "record."
            )
        finally:
            if warning:
                warnings.warn(
                    f"Unable to compute enrichment_fraction for {self.peak_group.msrun.sample}:{self}, {msg}."
                )

        return enrichment_fraction

    @property  # type: ignore
    @cached_function
    def enrichment_abundance(self):
        """
        The abundance of labeled atoms in this.PeakGroup's measured compound.
        this.PeakGroup.total_abundance * this.enrichment_fraction
        """
        try:
            # If self.enrichment_fraction is None, it will be handled in the except
            enrichment_abundance = (
                self.peak_group.total_abundance * self.enrichment_fraction
            )
        except (AttributeError, TypeError):
            enrichment_abundance = None
        return enrichment_abundance

    @property  # type: ignore
    @cached_function
    def normalized_labeling(self):
        """
        The enrichment in this peak group's measured compound normalized to the enrichment in the
        tracer compound from the final serum timepoint.
        This.PeakGroup.enrichment_fraction / SerumTracerPeakGroup.enrichment_fraction
        """
        from DataRepo.models.sample import Sample

        try:
            serum_tracers_enrichment_fraction = self.animal.labels.get(
                element__exact=self.element
            ).serum_tracers_enrichment_fraction

            if (
                self.animal.infusate.tracers.count() > 0
                and self.enrichment_fraction is not None
                and serum_tracers_enrichment_fraction is not None
            ):
                normalized_labeling = (
                    self.enrichment_fraction / serum_tracers_enrichment_fraction
                )
            else:
                normalized_labeling = None

        except Sample.DoesNotExist:
            warnings.warn(
                f"Unable to compute normalized_labelings for {self.peak_group.msrun.sample}:{self}, associated "
                "'serum' sample not found."
            )
            normalized_labeling = None

        return normalized_labeling

    # @cached_function is *slower* than uncached
    @cached_property
    def animal(self):
        """Convenient instance method to cache the animal this PeakGroup came from"""
        return self.peak_group.msrun.sample.animal

    @property  # type: ignore
    @cached_function
    def tracer(self):
        """
        If this peakgroup's compounds contains a compound that is among the tracers for this animal, it returns the
        tracer record, otherwidse None
        """
        from DataRepo.models.tracer import Tracer

        try:
            # This gets the tracer for this peakgroup (based on the compounds)
            this_tracer = self.animal.infusate.tracers.get(
                compound__id__in=list(
                    self.peak_group.compounds.values_list("id", flat=True)
                ),
            )
        except Tracer.DoesNotExist:
            this_tracer = None

        return this_tracer

    @property  # type: ignore
    @cached_function
    def tracer_label_count(self):
        from DataRepo.models.tracer_label import TracerLabel

        try:
            # This gets the supplied tracer's tracer_label with this element, and returns its count
            this_tracer_label_count = self.tracer.labels.get(
                element__exact=self.element
            ).count
        except (AttributeError, TracerLabel.DoesNotExist):
            # We will get an AttributeError if the tracer passed in is None
            this_tracer_label_count = None

        return this_tracer_label_count

    @property  # type: ignore
    @cached_function
    def tracer_concentration(self):
        # This gets the supplied tracer's tracer_label with this element, and returns its count
        return self.animal.infusate.tracer_links.get(
            tracer__exact=self.tracer
        ).concentration

    @property  # type: ignore
    @cached_function
    def get_peak_group_label_tracer_info(self):
        this_tracer = self.tracer

        if this_tracer:
            this_tracer_label_count = self.tracer_label_count
            this_tracer_concentration = self.tracer_concentration
        else:
            return None

        return {
            "tracer": this_tracer,
            "count": this_tracer_label_count,
            "concentration": this_tracer_concentration,
        }

    @property  # type: ignore
    @cached_function
    def is_tracer_label_compound_group(self):
        """
        Instance method which returns True if the compound it is associated with is also a tracer compound (with this
        labeled element) for the animal it came from.  Use this to prevent tracer appearance/disappearance
        calculations from calculating these values for non-tracer compounds or for labels not in the tracer.
        """
        tracer = self.tracer
        count = self.tracer_label_count
        return True if tracer and count and count > 0 else False

    @property  # type: ignore
    @cached_function
    def from_serum_sample(self):
        """
        Instance method which returns True if a peakgroup was obtained from a serum sample.
        """
        if self.peak_group.msrun.sample.is_serum_sample is None:
            warnings.warn(
                f"Sample {self.peak_group.msrun.sample.name}'s is_serum_sample field hasn't been set."
            )
            fss = self.peak_group.msrun.sample._is_serum_sample()
        else:
            fss = self.peak_group.msrun.sample.is_serum_sample

        if fss:
            return True

        warnings.warn(f"{self.peak_group.name} is not from a serum sample.")

        return False

    @property  # type: ignore
    @cached_function
    def can_compute_tracer_label_rates(self):
        """
        Instance method which returns True if a peak_group_label can calculate rates of appearance and dissapearance of
        a tracer label group
        """
        if not self.is_tracer_label_compound_group:
            warnings.warn(
                f"Peak Group [{self.peak_group.name}] is not a valid tracer label peak group for Animal "
                f"{self.animal.name} and Label [{self.element}]."
            )
            return False
        elif not self.from_serum_sample:
            warnings.warn(f"{self.peak_group.name} is not from a serum sample msrun.")
            return False
        elif not self.animal.infusion_rate:
            warnings.warn(f"Animal {self.animal.name} has no annotated infusion rate.")
            return False
        else:
            tracer_info = self.get_peak_group_label_tracer_info
            if not tracer_info["concentration"]:
                warnings.warn(
                    f"Animal {self.animal.name} has no annotated tracer_concentration."
                )
                return False

        return True

    @property  # type: ignore
    @cached_function
    def can_compute_body_weight_intact_tracer_label_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can utilize
        the associated animal.body_weight
        """
        if not self.can_compute_intact_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact tracer rate for element {self.element}."
            )
            return False
        elif not self.animal.body_weight:
            warnings.warn(f"Animal {self.animal.name} has no annotated body_weight.")
            return False

        return True

    @property  # type: ignore
    @cached_function
    def can_compute_body_weight_average_tracer_label_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can utilize
        the associated animal.body_weight
        """
        if not self.can_compute_average_tracer_label_rates:
            warnings.warn(
                f"{self.peak_group.name} cannot compute average tracer rate for element {self.element}."
            )
            return False
        elif not self.animal.body_weight:
            warnings.warn(f"Animal {self.animal.name} has no annotated body_weight.")
            return False

        return True

    @property  # type: ignore
    @cached_function
    def can_compute_intact_tracer_label_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be
        calculated using fully-labeled/intact measurements of a tracer's
        peakdata.  Returns the peakdata.fraction, if it exists and is greater
        than zero.
        """

        if not self.can_compute_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact (dis/)appearance rates for element "
                f"{self.element}."
            )
            return False

        tracer_info = self.get_peak_group_label_tracer_info

        # This can return multiple records if there are multiple labeled elements.  An element with a specific count
        # can exist along side other elements with different counts
        intact_peakdata = self.peak_group.peak_data.filter(
            labels__element__exact=self.element
        ).filter(
            labels__count__exact=tracer_info["count"],
        )
        if intact_peakdata.count() == 0:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} has no fully labeled/intact peakdata for element {self.element}."
            )
            return False

        # NOTE: PR review: **IMPORTANT**: Is this the right way to handle whether we **can** compute rates?  E.g. if we
        # had C and N labels and raw and corrected abundance for C when N's label count is 1, but when N's label count
        # is 0 and C's 3-count peakdata record is: {'id': 20260, 'peak_group': 3176, 'raw_abundance': None,
        # 'corrected_abundance': 0.0, 'med_mz': None, 'med_rt': None} ... Can we still validly calculate C's rates?

        fraction_total = 0.0
        for pdrec in intact_peakdata.all():
            if pdrec.fraction:
                fraction_total += pdrec.fraction

        if fraction_total == 0:
            warnings.warn(
                f"PeakGroup {self.peak_group.name}'s peakdata records for element {self.element} at count "
                f"{tracer_info['count']} are not all fully intact: {str(model_to_dict(pdrec))}."
            )
            return False

        return True

    @property  # type: ignore
    @cached_function
    def can_compute_average_tracer_label_rates(self):
        """
        Instance method which returns True if a peak_group rate metric can be calculated using averaged enrichment
        measurements of a tracer's peakdata.
        """
        if not self.can_compute_tracer_label_rates:
            warnings.warn(
                f"{self.peak_group.name} cannot compute average (dis/)appearance rates for element {self.element}."
            )
            return False

        if self.enrichment_fraction and self.enrichment_fraction > 0:
            return True
        else:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} has no enrichment fraction for element {self.element}.",
            )
            return False

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """Rate of Disappearance (intact)"""

        if not self.can_compute_intact_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact tracer rate for element {self.element}."
            )
            return None

        tracer_info = self.get_peak_group_label_tracer_info

        # There cam be multiple peak_data records if there are multiple labeled elements.  A specific element with a
        # the same count can exist along side other elements with different counts.  The fraction is therefore the sum
        # of their corrected abundances divided by the total abundance for the group, or...
        fraction = 0.0
        for pdrec in self.peak_group.peak_data.filter(
            labels__element__exact=self.element
        ).filter(
            labels__count=tracer_info["count"],
        ):
            fraction += pdrec.fraction

        return self.animal.infusion_rate * tracer_info["concentration"] / fraction

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_gram(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_intact_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact appearance rate for element {self.element}."
            )
            return None

        tracer_info = self.get_peak_group_label_tracer_info

        return (
            self.rate_disappearance_intact_per_gram
            - self.animal.infusion_rate * tracer_info["concentration"]
        )

    def atom_count(self):
        return atom_count_in_formula(self.peak_group.formula, self.element)

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """Rate of Disappearance (intact)"""
        if not self.can_compute_body_weight_intact_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact disappearance rate for element "
                f"{self.element}."
            )
            return None
        return self.rate_disappearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_animal(self):
        """Rate of Appearance (intact)"""
        if not self.can_compute_body_weight_intact_tracer_label_rates:
            # Even though this warning would have been issued above, python mysteriously filters that warning in some
            # cases and issues no warning from the called method in the conditional above at all when it is expected,
            # but re-warning it here works.
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute intact appearance rate for element {self.element}."
            )
            return None
        return self.rate_appearance_intact_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_gram(self):
        """
        Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction'
        in nmol/min/g
        """
        if not self.can_compute_average_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute average disappearance rate for element "
                f"{self.element}."
            )
            return None

        tracer_info = self.get_peak_group_label_tracer_info

        result = None

        try:
            result = (
                tracer_info["concentration"]
                * self.animal.infusion_rate
                / self.enrichment_fraction
            )
        except Exception as e:
            raise e

        return result

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_gram(self):
        """
        Ra_avg_g = Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        """
        if not self.can_compute_average_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute average appearance rate for element "
                f"{self.element}."
            )
            return None

        tracer_info = self.get_peak_group_label_tracer_info

        result = None

        try:
            result = (
                self.rate_disappearance_average_per_gram
                - tracer_info["concentration"] * self.animal.infusion_rate
            )
        except Exception as e:
            raise e

        return result

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg)
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_body_weight_average_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute average disappearance rate for element "
                f"{self.element}."
            )
            return None

        return self.rate_disappearance_average_per_gram * self.animal.body_weight

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg)
        Ra_avg = Ra_avg_g * 'Body Weight' in nmol/min
        """
        if not self.can_compute_body_weight_average_tracer_label_rates:
            warnings.warn(
                f"PeakGroup {self.peak_group.name} - cannot compute average appearance rate for element "
                f"{self.element}."
            )
            return None

        return self.rate_appearance_average_per_gram * self.animal.body_weight


class NoCommonLabel(Exception):
    def __init__(self, peakgrouplabel):
        msg = (
            f"PeakGroup {peakgrouplabel.peak_group.name} found associated with peak group formula: "
            f"[{peakgrouplabel.peak_group.formula}] that does not contain the labeled element "
            f"{peakgrouplabel.element} that is associated via PeakGroupLabel (from the tracers in the infusate "
            f"[{peakgrouplabel.peak_group.msrun.sample.animal.infusate.name}])."
        )
        super().__init__(msg)
        self.peak_group_label = peakgrouplabel
