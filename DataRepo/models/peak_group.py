from django.db import models
from django.db.models import Max, Min, Sum
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import atom_count_in_formula


@MaintainedModel.relation(
    generation=3,
    parent_field_name="msrun_sample",
    update_label="fcirc_calcs",
)
class PeakGroup(HierCachedModel, MaintainedModel):
    parent_related_key_name = "msrun_sample"
    child_related_key_names = ["labels"]

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        help_text='The compound or isomer group name (e.g. "citrate/isocitrate", "glucose").',
    )
    formula = models.CharField(
        max_length=256,
        null=False,
        help_text='The molecular formula of the compound (e.g. "C6H12O6").',
    )
    msrun_sample = models.ForeignKey(
        to="DataRepo.MSRunSample",
        on_delete=models.CASCADE,
        null=False,
        blank=False,
        related_name="peak_groups",
        help_text="The MS Run this PeakGroup belongs to.",
    )
    compounds = models.ManyToManyField(
        to="DataRepo.Compound",
        related_name="peak_groups",
        help_text="The compound(s) that this PeakGroup is presumed to represent.",
    )
    peak_annotation_file = models.ForeignKey(
        to="DataRepo.ArchiveFile",
        on_delete=models.RESTRICT,
        null=False,
        blank=False,
        related_name="peak_groups",
        help_text="The data file from which this PeakGroup was imported.",
    )

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

    @cached_property
    def min_med_mz(self):
        """Get the minimum med_mz value from all the peak_data that links to this peak group"""
        return self.peak_data.filter(med_mz__isnull=False).aggregate(
            min_med_mz=Min("med_mz", default=None)
        )["min_med_mz"]

    @cached_property
    def max_med_mz(self):
        """Get the maximum med_mz value from all the peak_data that links to this peak group"""
        return self.peak_data.filter(med_mz__isnull=False).aggregate(
            max_med_mz=Max("med_mz", default=None)
        )["max_med_mz"]

    @property  # type: ignore
    @cached_function
    def peak_labeled_elements(self):
        """
        Gets labels present among any of the tracers in the infusate IF the elements are present in the peak group
        formula.  Basically, if the compound contains an element that is a labeled element in any of the tracers, it is
        included in the returned list.
        """
        peak_labeled_elements = []
        for atom in self.msrun_sample.sample.animal.infusate.tracer_labeled_elements():
            if atom_count_in_formula(self.formula, atom) > 0:
                peak_labeled_elements.append(atom)
        return peak_labeled_elements

    # @cached_function is *slower* than uncached
    @cached_property
    def animal(self):
        """Convenient instance method to cache the animal this PeakGroup came from"""
        return self.msrun_sample.sample.animal

    class Meta:
        verbose_name = "peak group"
        verbose_name_plural = "peak groups"
        ordering = ["name"]

        # composite key
        constraints = [
            models.UniqueConstraint(
                fields=["name", "msrun_sample"],
                name="unique_peakgroup",
            ),
        ]

    def __str__(self):
        return str(self.name)
