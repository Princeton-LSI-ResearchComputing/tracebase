from datetime import date, timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models

from DataRepo.hier_cached_model import HierCachedModel, cached_function

from .animal import Animal
from .tissue import Tissue


class Sample(HierCachedModel):
    parent_related_key_name = "animal"
    child_related_key_names = ["msruns"]

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
        Animal,
        on_delete=models.CASCADE,
        null=False,
        related_name="samples",
        help_text="The source animal from which the sample was extracted.",
    )
    tissue = models.ForeignKey(
        Tissue,
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

    def peak_groups(self, compound=None):
        """
        Retrieve a list of PeakGroup objects for a sample instance.  If an optional compound is passed (e.g.
        animal.tracer_compound), then is it used to filter the PeakGroup queryset to a specific compound's peakgroup(s)
        [if multiple PeakGroupSets exist].
        """

        from .peakgroup import PeakGroup

        peak_groups = PeakGroup.objects.filter(msrun__sample_id=self.id)
        if compound:
            peak_groups = peak_groups.filter(compounds__id=compound.id)
        return peak_groups.all()

    def peak_data(self, compound=None):
        """
        Retrieve a list of PeakData objects for a sample instance.  If an optional compound is passed (e.g.
        animal.tracer_compound), then is it used to filter the PeakData queryset to a specific peakgroup.
        """
        from .peakdata import PeakData

        peakdata = PeakData.objects.filter(peak_group__msrun__sample_id=self.id)
        if compound:
            peakdata = peakdata.filter(peak_group__compounds__id=compound.id)

        return peakdata.all()

    @property  # type: ignore
    @cached_function
    def is_serum_sample(self):
        """returns True if the sample is flagged as a "serum" sample"""
        # NOTE: this logic may have to change in the future
        if self.tissue in Tissue.objects.filter(
            name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        ):
            return True
        else:
            return False

    class Meta:
        verbose_name = "sample"
        verbose_name_plural = "samples"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)
