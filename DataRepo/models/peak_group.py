from django.db.models import AutoField, CASCADE, CharField, ForeignKey, ManyToManyField, RESTRICT, Sum, UniqueConstraint
from django.utils.functional import cached_property

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import atom_count_in_formula


@MaintainedModel.relation(
    generation=2,
    parent_field_name="sample",
    # child_field_names=[],  # No children.  Insertions/deletions to this model affect FCirc calculations, upward only.
    update_label="fcirc_calcs",
)
class PeakGroup(HierCachedModel, MaintainedModel):
    parent_related_key_name = "sample"
    child_related_key_names = ["labels"]

    id = AutoField(primary_key=True)
    name = CharField(
        max_length=256,
        help_text='The compound or isomer group name (e.g. "citrate/isocitrate", "glucose").',
    )
    formula = CharField(
        max_length=256,
        null=False,
        help_text='The molecular formula of the compound (e.g. "C6H12O6").',
    )
    sample = ForeignKey(
        to="DataRepo.Sample",
        null=False,
        blank=False,
        # If the linked Sample is deleted, delete this record
        on_delete=CASCADE,
        related_name="peak_groups",
        help_text="The sample this PeakGroup came from.",
    )
    msrun_sequence = ForeignKey(
        to="DataRepo.MSRunSequence",
        null=False,
        blank=False,
        # Block MSRunSequence deletion unless all PeakGroups linked to it are deleted via a different field's cascade
        on_delete=RESTRICT,
        related_name="peak_groups",
        help_text="The mass spec batch sequence from which this peak group was derived.",
    )
    msrun_sample = ForeignKey(
        to="DataRepo.MSRunSample",
        on_delete=CASCADE,
        null=True,
        blank=True,
        related_name="peak_groups",
        help_text="The MS Run this PeakGroup came from.",
    )
    compounds = ManyToManyField(
        to="DataRepo.Compound",
        related_name="peak_groups",
        help_text="The compound(s) that this PeakGroup is presumed to represent.",
    )
    peak_annotation_file = ForeignKey(
        to="DataRepo.ArchiveFile",
        on_delete=RESTRICT,
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
            UniqueConstraint(
                fields=["name", "msrun_sample"],
                name="unique_peakgroup",
            ),
        ]

    def __str__(self):
        return str(self.name)
