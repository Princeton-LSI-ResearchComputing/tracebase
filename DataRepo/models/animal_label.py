import warnings

from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.utilities import atom_count_in_formula


class AnimalLabel(HierCachedModel):
    """
    This class is simply a home for the calculating methods linked to the animal.  An Element field is provided for
    convenience, but it's really a foreign key to an ElementLabel record (which doesn't exist as a model.
    """

    parent_related_key_name = "animal"
    # Leaf

    id = models.AutoField(primary_key=True)
    animal = models.ForeignKey(
        "DataRepo.Animal",
        on_delete=models.CASCADE,
        related_name="labels",
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='An element that is labeled in any of the tracers in this infusate (e.g. "C", "H", "O").',
    )

    class Meta:
        verbose_name = "animal_label"
        verbose_name_plural = "labels"
        ordering = ["animal", "element"]
        constraints = [
            models.UniqueConstraint(
                fields=["animal", "element"],
                name="unique_animal_label",
            )
        ]

    def __str__(self):
        return str(self.element)

    @property  # type: ignore
    @cached_function
    def tracers(self):
        from DataRepo.models.tracer import Tracer

        if self.animal.infusate is None:
            tracers = Tracer.objects.none()
        else:
            # Get every tracer's compound that contains this element
            tracers = self.animal.infusate.tracers.filter(
                labels__element__exact=self.element
            )
        if tracers.count() == 0:
            warnings.warn(
                f"Animal [{self.animal}] has no tracers containing labeled element [{self.element}]."
            )
        return tracers

    @property  # type: ignore
    @cached_function
    def last_serum_tracer_label_peak_groups(self):
        """
        Retrieves the last Peak Group for each tracer compound that has this.element
        """
        from DataRepo.models.peak_group import PeakGroup

        peakgroups = self.animal.last_serum_tracer_peak_groups.filter(
            labels__element__exact=self.element
        )

        if peakgroups.count() != self.tracers.count():
            warnings.warn(
                f"Animal {self.animal} is missing {self.tracers.count() - peakgroups.count()} serum peak groups of "
                f"the {self.tracers.count()} tracers containing element {self.element}."
            )
            return PeakGroup.objects.none()

        return peakgroups

    @property  # type: ignore
    @cached_function
    def serum_tracers_enrichment_fraction(self):
        """
        Computes a weighted average of the fraction of labeled atoms (among all tracers) for the final serum sample.
        i.e. The fraction of carbons that are labeled among all the final serum sample's tracer compounds.
        For each TracerLabel.element
            Sum of all (PeakData.fraction * PeakDataLabel.count) /
                Sum of all (Tracers.Compound.num_atoms(TracerLabel.element))
        """
        from DataRepo.models.peak_data_label import PeakDataLabel

        tracers_enrichment_fraction = None
        error = False
        msg = ""

        try:
            if self.tracers.count() == 0:
                raise NoTracerCompounds(self.animal, self.element)

            if self.last_serum_tracer_label_peak_groups.count() != self.tracers.count():
                raise MissingPeakGroups(
                    self.tracers,
                    self.last_serum_tracer_label_peak_groups,
                )

            # Sum the element enrichment across all tracer compound peak groups for this element
            last_serum_tracers_enrichment_sum = 0.0
            total_atom_count = 0
            for pg in self.last_serum_tracer_label_peak_groups.all():
                # Count the total number of this element among all the tracer compounds (via the single peakgroup
                # formula).   This may be called on formulas that do not have self.element, but those just return 0 and
                # that's OK.
                total_atom_count += atom_count_in_formula(pg.formula, self.element)

                label_pd_recs = pg.peak_data.filter(labels__element__exact=self.element)

                # This assumes that if there are any label_pd_recs for this measured elem, the calculation is valid
                if label_pd_recs.count() == 0:
                    raise MissingPeakData(pg, self.element)

                for label_pd_rec in label_pd_recs:
                    # This assumes the PeakDataLabel unique constraint: peak_data, element
                    label_rec = label_pd_rec.labels.get(element__exact=self.element)

                    # This assumes that label_rec must exist because of the filter above the loop
                    last_serum_tracers_enrichment_sum += (
                        label_pd_rec.fraction * label_rec.count
                    )

            if total_atom_count == 0:
                raise NoTracerCompounds(self.animal, self.element)

            tracers_enrichment_fraction = (
                last_serum_tracers_enrichment_sum / total_atom_count
            )

        except NoTracerCompounds as ntc:
            error = True
            msg = NoTracerCompounds.__name__ + " ERROR: " + str(ntc)
        except MissingPeakData as mpd:
            error = True
            msg = MissingPeakData.__name__ + " ERROR: " + str(mpd)
        except MissingPeakGroups as mpg:
            error = True
            msg = MissingPeakGroups.__name__ + " ERROR: " + str(mpg)
        except PeakDataLabel.DoesNotExist as pdldne:
            # This is not something the user can recitify via loading. This would be a bug in the loading code
            raise MissingPeakDataLabel(pg, self.element) from pdldne
        except TypeError as te:
            if label_pd_rec and label_pd_rec.fraction is None:
                error = True
                msg = f" ERROR: PeakData fraction was None.  Original Error: {TypeError.__name__}: {str(te)}"
            else:
                raise te
        finally:
            if error:
                warnings.warn(
                    "Unable to compute serum_tracers_enrichment_fraction from serum samples that are all missing peak "
                    f"groups for 1 or more tracer compounds with element {self.element} for animal {self.animal}: "
                    f"{msg}"
                )
                tracers_enrichment_fraction = None

        return tracers_enrichment_fraction


class MissingPeakData(Exception):
    def __init__(self, last_serum_tracer_peak_group, tracer_labeled_element):
        msg = (
            f"PeakData record missing for element {tracer_labeled_element} in final serum peak group "
            f"{last_serum_tracer_peak_group}.  There should exist a PeakData record for every tracer labeled "
            "element, even if the abundance is 0."
        )
        super().__init__(msg)
        self.last_serum_tracer_peak_group = last_serum_tracer_peak_group
        self.tracer_labeled_element = tracer_labeled_element


class MissingPeakGroups(Exception):
    def __init__(self, tracers, peakgroups):
        msg = (
            f"PeakGroup(s) missing for tracers in all serum samples.  There are {tracers.count()} tracers and "
            f"{peakgroups.count()} corresponding peak groups."
        )
        super().__init__(msg)
        self.tracers = tracers
        self.peakgroups = peakgroups


class NoTracerCompounds(Exception):
    def __init__(self, animal, element):
        msg = (
            f"Animal [{animal}] has no tracers containing labeled element [{element}]."
        )
        super().__init__(msg)
        self.animal = animal


class MissingPeakDataLabel(Exception):
    def __init__(self, last_serum_peak_group, element):
        msg = (
            f"ERROR: PeakDataLabel record missing for element {element} in final serum peak group "
            f"{last_serum_peak_group}.  There should exist a PeakDataLabel record for every PeakData "
            "record."
        )
        super().__init__(msg)
        self.last_serum_peak_group = last_serum_peak_group
        self.element = element
