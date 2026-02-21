import warnings

from django.conf import settings
from django.db import models

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel


class FCirc(MaintainedModel, HierCachedModel):
    """
    This class is here to perform rate of appearance/disappearance calculations for every combination of serum sample,
    tracer, and labeled element.  The last peakgroup of the given sample is used for every calculation.
    """

    parent_related_key_name = "serum_sample"
    # Leaf

    id = models.AutoField(primary_key=True)
    serum_sample = models.ForeignKey(
        "DataRepo.Sample",
        on_delete=models.CASCADE,
        related_name="fcircs",
        null=False,
        blank=False,
    )
    tracer = models.ForeignKey(
        "DataRepo.Tracer",
        on_delete=models.CASCADE,
        related_name="fcircs",
        null=False,
        blank=False,
    )
    element = models.CharField(
        max_length=1,
        null=False,
        blank=False,
        choices=ElementLabel.LABELED_ELEMENT_CHOICES,
        default=ElementLabel.CARBON,
        help_text='An element that is labeled in any of the tracers in this infusate (e.g. "C", "H", "O").',
    )
    is_last = models.BooleanField(
        default=False,
        help_text=(
            "This field indicates whether the last peak group of this serum sample and this tracer, is the last among "
            "the serum samples/tracers for the associated animal. Maintained field. Do not edit/set."
        ),
    )

    class Meta:
        verbose_name = "fcirc"
        verbose_name_plural = "fcircs"
        ordering = ["serum_sample", "tracer", "element"]
        constraints = [
            models.UniqueConstraint(
                fields=["serum_sample", "tracer", "element"],
                name="unique_fcirc",
            )
        ]

    def __str__(self):
        return (
            f"FCirc calculations object for element '{self.element}' from tracer '{self.tracer}' in sample "
            f"'{self.serum_sample}'"
        )

    # TODO: Make this into a clean method
    def save(self, *args, **kwargs):
        """
        This checks to make sure that self.serum_sample is in fact a serum sample.
        """
        # Cannot rely on auto-updates to have populated self.serum_sample.is_serum_sample, because they could be being
        # buffered, e.g. during a mass update, so call the method here
        if not self.serum_sample._is_serum_sample:
            raise InvalidSerumSample(
                f"The linked sample [{self.serum_sample}] must be a serum sample, not a "
                f"{self.serum_sample.tissue.name}."
            )

        # Now save the updated values
        super().save(*args, **kwargs)

    # No need to propagate up to Sample.  No change here affects any change elsewhere.  However, the deprecated
    # generation argument has not been removed yet and when it is not 0, a parent field name is required.  It shouldn't
    # matter though because propagation should stop at Sample and if a trigger came in from sample, it does not
    # propagate back.
    @MaintainedModel.setter(
        generation=2,
        update_field_name="is_last",
        parent_field_name="serum_sample",  # See above comment
        update_label="fcirc_calcs",
    )
    def is_last_serum_peak_group(self):
        """
        Note, there is an FCirc record for every serum sample, tracer, and label combo.  Each such combo represents a
        single "peak group" even though there can exist multiple peak groups from different serum samples and different
        msrun_samples from the same serum sample.  However, multiple msrun_samples from the same sample are ignored -
        only the last one is represented by a record in this table.  Michael and I (Rob) discussed whether it was
        worthwhile to compute values for peak groups from this sample in prior msrun_samples and Michael said no, so:

        This method determines whether the peak group from the last msrun_sample that included this tracer and label is
        the last peakgroup when considered among multiple serum samples.  There could exist last peak groups in prior
        serum samples that would result in a false return here.  There can also be later serum samples that don't
        include a peak group for this tracer which would return false if it was among the peakgroups returned, but they
        will not be among the peakgroups represented in this table.
        """

        if self.last_peak_group_in_sample:
            return self.last_peak_group_in_sample == self.last_peak_group_in_animal
        else:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )
            return False

    @property  # type: ignore
    @cached_function
    def last_peak_group_in_animal(self):
        """
        Retrieve the last serum sample PeakGroup for this animal and tracer.
        """
        return self.serum_sample.animal.last_serum_tracer_peak_groups.filter(
            compounds__exact=self.tracer.compound
        ).get()

    @property  # type: ignore
    @cached_function
    def last_peak_group_in_sample(self):
        """
        Retrieve the last PeakGroup for this serum sample and tracer.
        """
        peakgroups = self.serum_sample.last_tracer_peak_groups.filter(
            compounds__exact=self.tracer.compound
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )
            return None

        return peakgroups.get()

    @property  # type: ignore
    @cached_function
    def peak_groups(self):
        """
        Retrieve all PeakGroups for this serum sample and tracer, regardless of msrun_sequence date.

        Currently unused - see docstring in self.is_last_serum_peak_group
        """
        from DataRepo.models.peak_group import PeakGroup

        peakgroups = (
            PeakGroup.objects.filter(msrun_sample__sample__exact=self.serum_sample)
            .filter(compounds__exact=self.tracer.compound)
            .order_by("msrun_sample__msrun_sequence__date")
        )

        if peakgroups.count() == 0:
            warnings.warn(
                f"Serum sample {self.serum_sample} has no peak group for tracer {self.tracer}."
            )

        return peakgroups.all()

    @property  # type: ignore
    @cached_function
    def serum_validity(self):
        """
        Returns a dict containing information about the validity of this animal's serum samples, such as if all serum
        samples have a time_collected, if the last serum sample has all tracer peak groups, etc.
        """
        valid = True
        messages = []
        level = "good"
        prev_or_last_str = "previous"
        srmsmpl_has_no_trcr_pgs = 0
        last_trcr_pg_but_prev_srmsmpl = 0
        last_trcr_pg_but_smpl_tmclctd_is_none_amng_many = 0
        prev_smpl_tmclctd_is_none_amng_many = 0
        sib_of_last_smpl_tmclctd_is_none = 0
        tmclctd_is_none_but_only1_smpl = 0  # If 1, status is still good
        dynamic_data_stale = 0
        overall = 1

        if self.last_peak_group_in_sample is None:
            valid = False
            level = "error"
            srmsmpl_has_no_trcr_pgs = 1
            messages.append(
                f"No serum tracer peak group found for sample {self.serum_sample} and tracer {self.tracer}."
            )
        else:
            # There do exist peak groups for this sample, so we can check more things...

            # If this record's peak group (the one associated with self.serum_sample and self.tracer) used in the
            # calculations is the animal's last such peak group
            if self.is_last_serum_peak_group():
                prev_or_last_str = "last"

                # Check the sibling serum samples to see if there is adequate info to be confident that this sample is
                # actually the last serum sample
                tc_none_samples = []
                for s in self.serum_sample.animal.samples.all():
                    # If a serum sample other than self.serum_sample (containing the last peakgroup for self.tracer)
                    # has a null time collected, we are not actually sure if this presumed "last serum serum tracer
                    # peakgroup" is in fact last.  Note, we are assuming here though that the sibling serum sample we
                    # identify actually has a peak group for self.tracer.
                    if (
                        # If this is a serum sample
                        s._is_serum_sample()
                        # and is not the serum sample in this fcirc record
                        and s.id != self.serum_sample.id
                        # and its time_collected is null
                        and s.time_collected is None
                    ):
                        tc_none_samples.append(str(s))

                if len(tc_none_samples) > 0:
                    valid = False
                    level = "warn"
                    sib_of_last_smpl_tmclctd_is_none = 1
                    messages.append(
                        f"This serum sample {self.serum_sample} is assumed to be last, but serum sample(s) "
                        f"[{', '.join(tc_none_samples)}] from animal {self.serum_sample.animal} have no recorded time "
                        "collected, so it's possible these FCirc calculations could be based on a serum sample that "
                        "may not actually be the last one."
                    )

                # If self.serum_sample is not the animal's last serum sample
                if self.serum_sample.animal.last_serum_sample is None:
                    # This is likely a stale maintained field value, and indicated that maintained fields should be
                    # rebuilt.  Probably, there has been some manual manipulation of the data.
                    valid = False
                    level = "error"
                    dynamic_data_stale = 1
                    messages.append(
                        "ERROR: There appears to be a stale last serum sample value associated with animal "
                        f"{self.serum_sample.animal}.  All calculations will be suspect until this is fixed.  "
                        "Maintained fields must be rebuilt."
                    )
                elif (
                    self.serum_sample.id
                    != self.serum_sample.animal.last_serum_sample.id
                ):
                    valid = False
                    level = "warn"
                    last_trcr_pg_but_prev_srmsmpl = 1
                    messages.append(
                        f"Animal {self.serum_sample.animal}'s last serum sample "
                        f"({self.serum_sample.animal.last_serum_sample}) is not being used for calculations for "
                        f"tracer {self.tracer}.  Sample {self.serum_sample} is being used instead.  The last serum "
                        "sample probably does not contain a peak group for this tracer compound."
                    )

            # Determine the number of serum samples, but don't rely on maintained fields (for robustness)
            num_serum_samples = 0
            for ss in self.serum_sample.animal.samples.all():
                if ss.tissue.is_serum():
                    num_serum_samples += 1

            # If time collected is none and there exist other serum samples for this animal
            # Note: this level (error) is intentionally set last so that it can overwrite a warn level
            if self.serum_sample.time_collected is None and num_serum_samples > 1:
                valid = False
                if self.is_last_serum_peak_group():
                    level = "error"
                    last_trcr_pg_but_smpl_tmclctd_is_none_amng_many = 1
                else:
                    level = "warn"
                    prev_smpl_tmclctd_is_none_amng_many = 1
                messages.append(
                    f"The sample time collected is not set for this {prev_or_last_str} serum tracer peak group for "
                    f"tracer ({self.tracer}) and sample ({self.serum_sample}).  This animal "
                    f"({self.serum_sample.animal}) has {num_serum_samples} serum samples, so it's possible the FCirc "
                    "calculations for this record should or should not be for the 'last' serum sample."
                )
            elif self.serum_sample.time_collected is None:
                # This doesn't trigger/override the valid or level settings, but it does append a message
                tmclctd_is_none_but_only1_smpl = 1
                messages.append(
                    f"The sample time collected is not set for this {prev_or_last_str} serum tracer peak group for "
                    f"tracer ({self.tracer}) and sample ({self.serum_sample}).  This animal "
                    f"({self.serum_sample.animal}) only has 1 serum sample, so it's of no real concern (yet)."
                )

        if valid:
            overall = 0
            messages.insert(
                0,
                "No significant problems found with the peak group or sample collection time.",
            )

        # Any int produced from this bit str less than 000000100 should be status "good" (i.e. code < 4)
        # Serious issues should be at the top and get less severe as you descend...
        bit_str = "".join(
            [
                str(b)
                for b in [
                    srmsmpl_has_no_trcr_pgs,
                    last_trcr_pg_but_smpl_tmclctd_is_none_amng_many,
                    last_trcr_pg_but_prev_srmsmpl,
                    sib_of_last_smpl_tmclctd_is_none,
                    prev_smpl_tmclctd_is_none_amng_many,
                    dynamic_data_stale,
                    overall,
                    tmclctd_is_none_but_only1_smpl,
                ]
            ]
        )

        code = int(bit_str, 2)

        code_str = f"Status: {level} Code: {code}."

        # If we're in debug mode, include the bit string
        if settings.DEBUG:
            code_str += f"  Bit Code: {bit_str} Bit Names: ("
            code_str += " ,".join(
                [
                    "srmsmpl_has_no_trcr_pgs",
                    "last_trcr_pg_but_smpl_tmclctd_is_none_amng_many",
                    "last_trcr_pg_but_prev_srmsmpl",
                    "sib_of_last_smpl_tmclctd_is_none",
                    "prev_smpl_tmclctd_is_none_amng_many",
                    "dynamic_data_stale",
                    "overall",
                    "tmclctd_is_none_but_only1_smpl",
                ]
            )
            code_str += ")"

        # Prepend the status message to the messages array
        messages.insert(0, code_str)

        return {
            "valid": valid,
            "level": level,
            "code": code,
            "bitcode": bit_str,
            "message": "\n\n".join(messages),
        }

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_gram(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact_g. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_disappearance_intact_per_gram

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_gram(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact_g, or
        sometimes Fcirc_intact. This is calculated on the Animal's
        final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_intact_per_gram

    @property  # type: ignore
    @cached_function
    def rate_disappearance_intact_per_animal(self):
        """
        Rate of Disappearance (intact), also referred to as Rd_intact. This is
        calculated on the Animal's final serum sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_disappearance_intact_per_animal

    @property  # type: ignore
    @cached_function
    def rate_appearance_intact_per_animal(self):
        """
        Rate of Appearance (intact), also referred to as Ra_intact, or sometimes
        Fcirc_intact_per_mouse. This is calculated on the Animal's final serum
        sample tracer's PeakGroup.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_intact_per_animal

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_gram(self):
        """
        Also referred to as Rd_avg_g = [Infusate] * 'Infusion Rate' / 'Enrichment Fraction' in
        nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_disappearance_average_per_gram

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_gram(self):
        """
        Also referred to as Ra_avg_g, and sometimes referred to as Fcirc_avg.
        Equivalent to Rd_avg_g - [Infusate] * 'Infusion Rate' in nmol/min/g
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_average_per_gram

    @property  # type: ignore
    @cached_function
    def rate_disappearance_average_per_animal(self):
        """
        Rate of Disappearance (avg), also referred to as Rd_avg
        Rd_avg = Rd_avg_g * 'Body Weight' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_disappearance_average_per_animal

    @property  # type: ignore
    @cached_function
    def rate_appearance_average_per_animal(self):
        """
        Rate of Appearance (avg), also referred to as Ra_avg or sometimes
        Fcirc_avg_per_mouse. Ra_avg = Ra_avg_g * 'Body Weight'' in nmol/min
        Calculated for the last serum sample collected, for the last tracer
        peakgroup analyzed.
        """
        if not self.last_peak_group_in_sample:
            warnings.warn(
                f"Serum sample {self.serum_sample.name} has no peak group for tracer {self.tracer}."
            )
            return None
        else:
            return self.last_peak_group_in_sample.labels.get(
                element__exact=self.element,
            ).rate_appearance_average_per_animal


class InvalidSerumSample(ValueError):
    pass
