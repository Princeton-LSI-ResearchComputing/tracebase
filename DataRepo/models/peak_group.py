from __future__ import annotations

from django.db import ProgrammingError, models
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

    detail_name = "peakgroup_detail"

    NAME_DELIM = "/"  # Compound.secondary_delimiter - cannot import yet

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        null=False,
        blank=False,
        help_text=(
            f"Peak group name, composed of 1 or more compound synonyms, delimited by '{NAME_DELIM}', e.g. 'citrate"
            f"{NAME_DELIM}isocitrate'.  Note, synonyms of the same compound are considered distinct peak groups.  I.e. "
            "they may confer information about the compound that is not recorded in the compound record, such as a "
            "specific stereoisomer.  Peak group names are subject to the same character restrictions as compound "
            f"names, aside from the delimiter ({NAME_DELIM})."
        ),
    )
    formula = models.CharField(
        max_length=256,
        null=False,
        blank=False,
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

    def __init__(self, *args, **kwargs):
        from DataRepo.models.compound import Compound

        if self.NAME_DELIM != Compound.secondary_delimiter:
            raise ProgrammingError(
                f"NAME_DELIM ({self.NAME_DELIM}) must be equal to Compound.secondary_delimiter "
                f"({Compound.secondary_delimiter}).  It has to be synched because importing would be circular."
            )
        super().__init__(*args, **kwargs)

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
        for atom in self.tracer_labeled_elements:
            if atom_count_in_formula(self.formula, atom) > 0:
                peak_labeled_elements.append(atom)
        return peak_labeled_elements

    @property
    @cached_function
    def tracer_labeled_elements(self):
        """This method returns a unique list of the labeled elements that exist among the tracers.

        Args:
            None
        Exceptions:
            None
        Returns:
            tracer_labeled_elements (List[str])
        """
        if self.msrun_sample.sample.animal.infusate is None:
            return []
        return self.msrun_sample.sample.animal.infusate.tracer_labeled_elements

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

    @property
    @cached_function
    def possible_isotope_observations(self):
        """Get the possible isotope observations from a peak group, i.e. all the ObservedIsotopeData objects for
        elements from the peak group's compound that exist as labels in the tracers.

        Args:
            peak_group (PeakGroup)
        Exceptions:
            None
        Returns:
            possible_observations (List[ObservedIsotopeData])
        """
        from DataRepo.models.tracer_label import TracerLabel
        from DataRepo.utils.infusate_name_parser import ObservedIsotopeData

        possible_observations = []

        if self.msrun_sample.sample.animal.infusate is None:
            return possible_observations

        tracer_labels = (
            TracerLabel.objects.filter(
                tracer__infusates__id=self.msrun_sample.sample.animal.infusate.id
            )
            .order_by("element")
            .distinct("element")
        )

        if self.compounds.count() > 0:
            # This prevents a tracer label from being added when the element does not exist in the measured compound.
            for compound_rec in self.compounds.all():
                for tracer_label in tracer_labels:
                    if (
                        compound_rec.atom_count(tracer_label.element) > 0
                        and tracer_label not in possible_observations
                    ):
                        observation = ObservedIsotopeData(
                            element=tracer_label.element,
                            mass_number=tracer_label.mass_number,
                            count=0,
                            parent=True,
                        )
                        if observation not in possible_observations:
                            possible_observations.append(observation)
        else:
            # If no compounds have yet been linked, fall back to the peak group formula (which note, could differ from
            # the compound formula (but only due to ionization))
            for tracer_label in tracer_labels:
                if (
                    atom_count_in_formula(self.formula, tracer_label.element) > 0
                    and tracer_label not in possible_observations
                ):
                    possible_observations.append(
                        ObservedIsotopeData(
                            element=tracer_label.element,
                            mass_number=tracer_label.mass_number,
                            count=0,
                            parent=True,
                        )
                    )

        return possible_observations

    def save(self, *args, **kwargs):
        """This is an override of Model.save().  Multiple representations must be checked BEFORE saving or else they
        won't be caught and will cryptically manifest as a unique constraint-related IntegrityError about the peak
        annotation file (ArchiveFile) conflicting.  So putting this in the clean method does nothing, because it never
        executes when there ARE multiple representations."""
        self.check_for_multiple_representations()
        return super().save(*args, **kwargs)

    def check_for_multiple_representations(self):
        """This checks the PeakGroup record (self) to see if its compound was already measured for this sample from a
        different peak annotation file and raises an exception if it was.

        Args:
            None
        Exceptions:
            ComplexPeakGroupDuplicate
            DuplicatePeakGroup
            MultiplePeakGroupRepresentation
            TechnicalPeakGroupDuplicate
        Returns:
            None
        """
        from DataRepo.models.utilities import exists_in_db
        from DataRepo.utils.exceptions import (
            ComplexPeakGroupDuplicate,
            DuplicatePeakGroup,
            MultiplePeakGroupRepresentation,
            TechnicalPeakGroupDuplicate,
        )

        # Ignore if a unique constraint violation will happen due to something other than the msrun_sample or
        # peak_annotation_file differing.  E.g. only the formula differs.  Cases where the msrun_samples or
        # peak_annotation_files differ are handled below.
        if PeakGroup.objects.filter(
            name=self.name,
            msrun_sample=self.msrun_sample,
            peak_annotation_file=self.peak_annotation_file,
        ).exists():
            return

        # Look for peak groups with the same name (i.e. compound) for the same sample.
        conflicts = PeakGroup.objects.filter(
            name=self.name,
            msrun_sample__sample__pk=self.msrun_sample.sample.pk,
        )

        # If the record already exists (e.g. doing an update), exclude self.  (self.pk is None otherwise.)
        if exists_in_db(self):
            conflicts = conflicts.exclude(pk=self.pk)

        # Look for duplicates due solely to business rule changes regarding MSRunSample placeholder records that
        # changes the linked MSRunSample record
        dupes = conflicts.filter(
            formula=self.formula,
            peak_annotation_file=self.peak_annotation_file,
        ).exclude(msrun_sample=self.msrun_sample)
        if dupes.exists():
            # NOTE: This DuplicatePeakGroup exception occludes the MultiplePeakGroupRepresentation exception.  That's
            # because if it's from the same peak annotation file, it is handled differently: as a warning, and skipped.
            raise DuplicatePeakGroup(self, dupes)

        # Look for duplicates solely due to the fact that the peak annotation file was edited
        file_edit_dupes = conflicts.filter(
            msrun_sample=self.msrun_sample,
            formula=self.formula,
            peak_annotation_file__filename=self.peak_annotation_file.filename,
        ).exclude(peak_annotation_file=self.peak_annotation_file)
        if file_edit_dupes.count() > 0:
            # NOTE: This TechnicalPeakGroupDuplicate exception occludes the MultiplePeakGroupRepresentation exception.
            # It is handled as an error to highlight that a disallowed edit of the peak annotation file occurred.
            raise TechnicalPeakGroupDuplicate(self, file_edit_dupes)

        # Finally, look for complex duplicates where either edits to the file and/or business rules (about the
        # linked MSRunSample record) *changed* this PeakGroup (note: if the sample and filename are the same, the
        # only difference can be the formula)
        complex_dupes = conflicts.filter(
            name=self.name,
            msrun_sample__sample=self.msrun_sample.sample,
            peak_annotation_file__filename=self.peak_annotation_file.filename,
        ).exclude(
            formula=self.formula,
            peak_annotation_file=self.peak_annotation_file,
            msrun_sample=self.msrun_sample,
        )
        if complex_dupes.count() > 0:
            # NOTE: This ComplexPeakGroupDuplicate exception occludes the MultiplePeakGroupRepresentation exception.
            # It highlights that the PeakGroup qualitatively differs (e.g. the formula changed), and that that coincides
            # with either a placeholder rule change and/or an edited file.
            rec_dict = {
                "msrun_sample": self.msrun_sample,
                "name": self.name,
                "formula": self.formula,
                "peak_annotation_file": self.peak_annotation_file,
            }

            dupe1 = complex_dupes.first()
            differences = {}
            if (
                type(dupe1.msrun_sample) is not type(self.msrun_sample)
                or dupe1.msrun_sample != self.msrun_sample
            ):
                differences["msrun_sample"] = {
                    "orig": (
                        f"{dupe1.msrun_sample} ({'concrete' if dupe1.msrun_sample.ms_data_file else 'placeholder'})"
                    ),
                    "new": (
                        f"{self.msrun_sample} ({'concrete' if self.msrun_sample.ms_data_file else 'placeholder'})"
                    ),
                }
            if (
                type(dupe1.formula) is not type(self.formula)
                or dupe1.formula != self.formula
            ):
                differences["formula"] = {
                    "orig": str(dupe1.formula),
                    "new": str(self.formula),
                }
            if (
                type(dupe1.peak_annotation_file) is not type(self.peak_annotation_file)
                or dupe1.peak_annotation_file != self.peak_annotation_file
            ):
                differences["peak_annotation_file"] = {
                    "orig": f"{dupe1.peak_annotation_file.filename} ({dupe1.peak_annotation_file.checksum})",
                    "new": f"{self.peak_annotation_file.filename} ({self.peak_annotation_file.checksum})",
                }

            suggestion = None
            n_other_dupes = complex_dupes.count() - 1
            if n_other_dupes > 0:
                suggestion = f"Note, there are {n_other_dupes} other complex duplicates not shown."

            raise ComplexPeakGroupDuplicate(
                dupe1,
                differences,
                rec_dict=rec_dict,
                suggestion=suggestion,
            )

        # TODO: The above should fix the creation of duplicate peak groups, but those errors were formerly
        # TODO: MultiplePeakGroupRepresentation exceptions and somewhere in the loading code downstream of this, there
        # TODO: appears to be some code that ignores the error, because the loads have been succeeding despite those
        # TODO: errors having been printed.  That code should be located and deleted.

        if conflicts.count() > 0:
            raise MultiplePeakGroupRepresentation(self, conflicts)

    def clean(self, *args, **kwargs):
        """This checks to ensure that the compound(s) associated with the PeakGroup HAVE an element that is labeled
        among the tracers.

        Args:
            None
        Exceptions:
            None
        Returns:
            None
        """
        from DataRepo.utils.exceptions import NoTracerLabeledElements

        if (
            self.msrun_sample.sample.animal.infusate is not None
            and len(self.peak_labeled_elements) == 0
        ):
            raise NoTracerLabeledElements(
                self.name,
                self.tracer_labeled_elements,
            )

        return super().clean(*args, **kwargs)

    def get_or_create_compound_link(self, cmpd_rec):
        """Get or create a peakgroup_compound record (so that it can be used in record creation stats).

        Args:
            cmpd_rec (Compound)
        Exceptions:
            Buffers:
                None
            Raises:
                NoTracerLabeledElements
        Returns:
            rec (Optional[PeakGroupCompound])
            created (boolean)
        """
        from DataRepo.utils.exceptions import NoTracerLabeledElements

        PeakGroupCompound = PeakGroup.compounds.through

        # Error check the labeled elements shared between the peak group's compound(s) and the tracers before creating
        # the record
        if (
            self.msrun_sample.sample.animal.infusate is not None
            and len(self.peak_labeled_elements) == 0
        ):
            raise NoTracerLabeledElements(
                self.name,
                self.tracer_labeled_elements,
            )

        # This is the effective rec_dict
        rec_dict = {
            "peakgroup": self,
            "compound": cmpd_rec,
        }

        # Get pre- and post- counts to determine if a record was created (add does a get_or_create)
        count_before = self.compounds.count()
        self.compounds.add(cmpd_rec)
        count_after = self.compounds.count()
        created = count_after > count_before

        # Retrieve the record (created or not - .add() doesn't return a record)
        rec = PeakGroupCompound.objects.get(**rec_dict)

        return rec, created

    @classmethod
    def compound_synonyms_to_peak_group_name(cls, synonyms):
        """Takes a list of strings (assumed to be valid compound synonyms) and returns the peak group name.

        Args:
            synonyms (List[str]): List of compound synonyms
        Exceptions:
            None
        Returns:
            pgname (str)
        """
        return cls.NAME_DELIM.join(sorted(synonyms, key=str.casefold))

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
