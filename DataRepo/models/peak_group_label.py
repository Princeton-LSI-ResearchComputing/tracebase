import warnings

from django.db import models
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
        related_name="peak_group_labels",
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
        Sum of all (PeakData.fraction * PeakData.labeled_count) /
            PeakGroup.Compound.num_atoms(PeakData.labeled_element)
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
                # And this assumes that label_rec must exist because of the filter above the loop
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
            serum_tracers_enrichment_fraction = (
                self.peak_group.msrun.sample.animal.animal_labels.get(
                    element__exact=self.element
                ).serum_tracers_enrichment_fraction
            )

            if (
                self.peak_group.msrun.sample.animal.infusate.tracers.count() > 0
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
                "Unable to compute normalized_labelings for "
                f"{self.peak_group.msrun.sample}:{self}, "
                "associated 'serum' sample not found."
            )
            normalized_labeling = None

        return normalized_labeling

    def atom_count(self):
        return atom_count_in_formula(self.peak_group.formula, self.element)


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