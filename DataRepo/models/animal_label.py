import warnings

from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function


class AnimalLabel(HierCachedModel):
    parent_related_key_name = "animal"
    # Leaf

    id = models.AutoField(primary_key=True)
    animal = models.ForeignKey(
        "DataRepo.Animal",
        on_delete=models.CASCADE,
        related_name="animal_labels",
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
        verbose_name_plural = "animal_labels"
        ordering = ["animal", "element"]
        constraints = [
            models.UniqueConstraint(
                fields=["animal", "element"],
                name="unique_animal_label",
            )
        ]

    @property  # type: ignore
    @cached_function
    def serum_tracers_enrichment_fraction(self):
        """
        This generates a dict keyed on labeled element.  For each labeled element among the tracers for this animal, it
        computes a weighted average of the fraction of labeled atoms (among all tracers) for the final serum sample.
        i.e. The fraction of carbons that are labeled among all the final serum sample's tracer compounds.
        For each TracerLabel.element
            Sum of all (PeakData.fraction * PeakDataLabel.count) /
                Sum of all (Tracers.Compound.num_atoms(TracerLabel.element))
        """
        from DataRepo.models.peak_data_label import PeakDataLabel
        from DataRepo.models.peak_group import PeakGroup
        from DataRepo.models.tissue import Tissue

        tracers_enrichment_fraction = None
        tracer_compounds = None
        error = False
        msg = ""

        try:
            # Get every tracer's 'compound that contains this element
            tracer_compounds = []
            tracer_compound_ids = []
            for tracer in self.animal.infusate.tracers.filter(
                labels__element__exact=self.element
            ):
                tracer_compounds.append(tracer.compound)
                tracer_compound_ids.append(tracer.compound.id)
            if len(tracer_compound_ids) == 0:
                raise NoTracerCompounds(self.animal, self.element)

            # Get the peak group of each tracer compound from the final serum sample
            final_serum_sample = self.animal.samples.filter(
                tissue__name__startswith=Tissue.SERUM_TISSUE_PREFIX
            ).latest("time_collected")
            # Get the Peak Groups for the tracer compounds of the final serum sample that have this element
            final_serum_tracer_peak_groups = PeakGroup.objects.filter(
                msrun__sample__id__exact=final_serum_sample.id
            ).filter(compounds__id__in=tracer_compound_ids)
            if final_serum_tracer_peak_groups.count() != len(tracer_compounds):
                raise MissingSerumTracerPeakGroups(
                    self.animal,
                    final_serum_sample,
                    final_serum_tracer_peak_groups,
                    tracer_compounds,
                )
            final_serum_tracer_peak_group_elem = final_serum_tracer_peak_groups.get(
                peak_group_labels__element__exact=self.element,
            )

            # Count the total number of each element among all the tracer compounds
            # This may call tracer compoundss that do not have self.element, but they just return 0
            total_atom_count = 0
            for tracer_compound in tracer_compounds:
                total_atom_count += tracer_compound.atom_count(self.element)

            # Sum the element enrichment across all tracer compounds
            final_serum_tracers_enrichment_sum = 0.0
            label_pd_recs = final_serum_tracer_peak_group_elem.peak_data.filter(
                labels__element__exact=self.element
            )
            # This assumes that if there are any label_pd_recs for this measured elem, the calculation is valid
            if label_pd_recs.count() == 0:
                raise MissingPeakData(final_serum_tracer_peak_group_elem, self.element)
            for label_pd_rec in label_pd_recs:
                # This assumes the PeakDataLabel unique constraint: peak_data, element
                label_rec = label_pd_rec.labels.get(element__exact=self.element)
                # And this assumes that label_rec must exist because of the filter above the loop
                final_serum_tracers_enrichment_sum += (
                    label_pd_rec.fraction * label_rec.count
                )

            tracers_enrichment_fraction = (
                final_serum_tracers_enrichment_sum / total_atom_count
            )
        except NoTracerCompounds as ntc:
            error = True
            msg = NoTracerCompounds.__name__ + " ERROR: " + str(ntc)
        except MissingSerumTracerPeakGroups as mstpg:
            error = True
            msg = MissingSerumTracerPeakGroups.__name__ + " ERROR: " + str(mstpg)
        except MissingPeakData as mpd:
            error = True
            msg = MissingPeakData.__name__ + " ERROR: " + str(mpd)
        except PeakDataLabel.DoesNotExist as pdldne:
            # This is not something the user can recitify via loading. This would be a bug in the loading code
            raise MissingPeakDataLabel(
                final_serum_tracer_peak_group_elem, self.element
            ) from pdldne
        except PeakGroup.DoesNotExist as pgdne:
            # This is not something the user can recitify via loading. This would be a bug in the loading code
            raise MissingPeakGroupLabel(
                final_serum_tracer_peak_groups, self.element
            ) from pgdne
        finally:
            if error:
                warnings.warn(
                    f"Unable to compute serum_tracers_enrichment_fraction for serum sample {final_serum_sample}, "
                    f"element {self.element}, and animal {self.animal}: {msg}"
                )
                return None

        return tracers_enrichment_fraction


class MissingSerumTracerPeakGroups(Exception):
    def __init__(
        self,
        animal,
        final_serum_sample,
        final_serum_tracer_peak_groups,
        tracer_compounds,
    ):
        msg = (
            f"There are {final_serum_tracer_peak_groups.count()} peak groups: "
            f"[{', '.join(final_serum_tracer_peak_groups.values_list('name', flat=True))}] in the final serum sample "
            f"[{final_serum_sample}] matching animal [{animal}]'s {len(tracer_compounds)} tracer compounds: "
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
    def __init__(self, animal, element):
        msg = (
            f"Animal [{animal}] has no tracers containing labeled element [{element}]."
        )
        super().__init__(msg)
        self.animal = animal


class MissingPeakDataLabel(Exception):
    def __init__(self, final_serum_peak_group, element):
        msg = (
            f"ERROR: PeakDataLabel record missing for element {element} in final serum peak group "
            f"{final_serum_peak_group}.  There should exist a PeakDataLabel record for every PeakData "
            "record."
        )
        super().__init__(msg)
        self.final_serum_peak_group = final_serum_peak_group
        self.element = element


class MissingPeakGroupLabel(Exception):
    def __init__(self, final_serum_tracer_peak_groups, element):
        msg = (
            f"ERROR: PeakGroupLabel record(s) missing for element {element} in the final serum peak "
            f"group(s): [{', '.join(list(final_serum_tracer_peak_groups.values_list('name', flat=True)))}].  "
            "There should exist a PeakGroupLabel record for every tracer labeled element, including this one: ",
            f"{element}.",
        )
        super().__init__(msg)
        self.final_serum_tracer_peak_groups = final_serum_tracer_peak_groups
        self.element = element
