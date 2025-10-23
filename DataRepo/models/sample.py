import warnings
from datetime import date, timedelta

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.forms.models import model_to_dict

from DataRepo.models.hier_cached_model import HierCachedModel, cached_function
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.peak_group import PeakGroup


class Sample(MaintainedModel, HierCachedModel):
    parent_related_key_name = "animal"
    child_related_key_names = ["msrun_samples", "fcircs"]

    detail_name = "sample_detail"

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=False,
        help_text="The unique name of the biological sample.",
    )
    date = models.DateField(
        default=date.today, help_text="The date the sample was collected."
    )
    researcher = models.CharField(
        max_length=256,
        help_text='The name of the researcher who prepared the sample (e.g. "Alex Medina").',
        verbose_name="Sample Owner",
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
    is_serum_sample = models.BooleanField(
        default=False,
        help_text="This field indicates whether this sample is a serum sample.",
    )

    # researchers have advised that samples might have a time_collected up to a day prior-to and 90 days after infusion
    MINIMUM_VALID_TIME_COLLECTED = timedelta(days=-1)
    MAXIMUM_VALID_TIME_COLLECTED = timedelta(days=90)
    time_collected = models.DurationField(
        null=True,
        blank=True,
        validators=[
            MinValueValidator(
                MINIMUM_VALID_TIME_COLLECTED,
                message=f"Sample.time_collected must be greater than or equal to {MINIMUM_VALID_TIME_COLLECTED}.",
            ),
            MaxValueValidator(
                MAXIMUM_VALID_TIME_COLLECTED,
                message=f"Sample.time_collected must be less than or equal to {MAXIMUM_VALID_TIME_COLLECTED}.",
            ),
        ],
        help_text="The time, relative to an infusion timepoint, "
        "that a sample was extracted from an animal.",
    )

    @MaintainedModel.setter(
        generation=1,
        parent_field_name="animal",
        child_field_names=["fcircs"],
        update_field_name="is_serum_sample",
        update_label="fcirc_calcs",
    )
    def _is_serum_sample(self):
        """returns True if the sample is flagged as a "serum" sample"""
        try:
            iss = self.tissue.is_serum()
        except Exception as e:
            print(
                f"ERROR: Could not determine tissue serum status in sample: {model_to_dict(self)}: {str(e)}"
            )
        return iss

    @property  # type: ignore
    @cached_function
    def last_tracer_peak_groups(self):
        """
        Retrieves the last Peak Group for each tracer compound
        """

        # Get every tracer's compound
        if self.animal.tracers.count() == 0:
            warnings.warn(f"Animal [{self.animal}] has no tracers.")
            return PeakGroup.objects.none()

        # Get the last peakgroup for each tracer
        last_peakgroup_ids = []
        for tracer in self.animal.tracers.all():
            tracer_peak_group = (
                PeakGroup.objects.filter(msrun_sample__sample__id__exact=self.id)
                .filter(compounds__id__exact=tracer.compound.id)
                .order_by("msrun_sample__msrun_sequence__date")
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

    class Meta:
        verbose_name = "sample"
        verbose_name_plural = "samples"
        ordering = ["name"]

    def __str__(self):
        return str(self.name)

    @classmethod
    def is_a_blank(cls, sample_name):
        return sample_name is not None and "blank" in sample_name.lower()

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
