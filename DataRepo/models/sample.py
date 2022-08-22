import warnings
from datetime import date, timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function

from .peak_data import PeakData
from .peak_group import PeakGroup


class Sample(HierCachedModel):
    parent_related_key_name = "animal"
    child_related_key_names = ["msruns", "fcircs"]

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        help_text="The unique name of the biological sample.",
    )
    date = models.DateField(
        default=date.today, help_text="The date the sample was collected."
    )
    researcher = models.CharField(
        max_length=256,
        help_text='The name of the researcher who prepared the sample (e.g. "Alex Medina").',
    )
    animal = models.ForeignKey(
        to="DataRepo.Animal",
        on_delete=models.CASCADE,
        null=False,
        related_name="samples",
        help_text="The source animal from which the sample was extracted.",
    )
    tissue = models.ForeignKey(
        to="DataRepo.Tissue",
        on_delete=models.RESTRICT,
        null=False,
        related_name="samples",
        help_text="The tissue type this sample was taken from.",
    )

    """
    researchers have advised that samples might have a time_collected up to a
    day prior-to and a week after infusion
    """
    MINIMUM_VALID_TIME_COLLECTED = timedelta(days=-1)
    MAXIMUM_VALID_TIME_COLLECTED = timedelta(weeks=1)
    time_collected = models.DurationField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(MINIMUM_VALID_TIME_COLLECTED),
            MaxValueValidator(MAXIMUM_VALID_TIME_COLLECTED),
        ],
        help_text="The time, relative to an infusion timepoint, "
        "that a sample was extracted from an animal.",
    )

    @property  # type: ignore
    @cached_function
    def is_serum_sample(self):
        """returns True if the sample is flagged as a "serum" sample"""
        return self.tissue.is_serum()

    @property  # type: ignore
    @cached_function
    def last_tracer_peak_groups(self):
        """
        Retrieves the last Peak Group for each tracer compound that has this.element
        """
        from DataRepo.models.peak_group import PeakGroup

        # Get every tracer's compound that contains this element
        if self.animal.tracers.count() == 0:
            warnings.warn(
                f"Animal [{self.animal}] has no tracers."
            )
            return PeakGroup.objects.none()

        # Get the last peakgroup for each tracer that has this label
        last_peakgroup_ids = []
        for tracer in self.animal.tracers.all():
            tracer_peak_group = (
                PeakGroup.objects.filter(
                    msrun__sample__id__exact=self.id
                )
                .filter(compounds__id__exact=tracer.compound.id)
                .order_by("msrun__date")
                .last()
            )
            if tracer_peak_group:
                last_peakgroup_ids.append(tracer_peak_group.id)
            else:
                warnings.warn(
                    f"Sample {self} has no peak group for tracer compound: [{tracer.compound}]."
                )
                return PeakGroup.objects.none()

        return PeakGroup.objects.filter(id__in=last_peakgroup_ids)

    def peak_groups(self, compound=None):
        """
        Retrieve a list of PeakGroup objects for a sample.  If an optional compound is passed (e.g.
        animal.infusate.tracers.compound), then is it used to filter the PeakGroup queryset to a specific compound's
        peakgroups.
        """
        from DataRepo.models.compound import Compound
        from DataRepo.models.tracer import Tracer

        peak_groups = PeakGroup.objects.filter(msrun__sample_id=self.id)
        if compound:
            if isinstance(compound, Compound):
                peak_groups = peak_groups.filter(compounds__id=compound.id)
            elif isinstance(compound, Tracer):
                peak_groups = peak_groups.filter(compounds__id=compound.compound.id)
            else:
                raise InvalidArgument("Argument must be a Compound or Tracer")
        return peak_groups.all()

    def peak_data(self, compound=None):
        """
        Retrieve a list of PeakData objects for a sample.  If an optional compound is passed (e.g.
        animal.infusate.tracers.compound), then is it used to filter the PeakData queryset to a specific compound's
        peakgroups.
        """
        from DataRepo.models.compound import Compound
        from DataRepo.models.tracer import Tracer

        peakdata = PeakData.objects.filter(peak_group__msrun__sample_id=self.id)

        if compound:
            if isinstance(compound, Compound):
                peakdata = peakdata.filter(peak_group__compounds__id=compound.id)
            elif isinstance(compound, Tracer):
                peakdata = peakdata.filter(
                    peak_group__compounds__id=compound.compound.id
                )
            else:
                raise InvalidArgument("Argument must be a Compound or Tracer")

        return peakdata.all()

    class Meta:
        verbose_name = "sample"
        verbose_name_plural = "samples"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)


class InvalidArgument(ValueError):
    pass
