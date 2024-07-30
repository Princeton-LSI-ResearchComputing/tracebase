from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Q
from django.utils.functional import cached_property


class PeakData(models.Model):
    """
    PeakData is a single observation (at the most atomic level) of a MS-detected molecule.
    For example, this could describe the data for M+2 in glucose from mouse 345 brain tissue.
    """

    id = models.AutoField(primary_key=True)
    peak_group = models.ForeignKey(
        to="DataRepo.PeakGroup",
        on_delete=models.CASCADE,
        null=False,
        related_name="peak_data",
    )
    raw_abundance = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The ion count of this observation.",
    )
    corrected_abundance = models.FloatField(
        validators=[MinValueValidator(0)],
        help_text="The ion counts corrected for natural abundance of isotopomers.",
    )
    med_mz = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The median mass/charge value of this measurement.",
    )
    med_rt = models.FloatField(
        null=True,
        blank=True,
        validators=[MinValueValidator(0)],
        help_text="The median retention time value of this measurement.",
    )

    # @cached_function is *slower* than uncached
    @cached_property
    def fraction(self):
        """
        The corrected abundance of the labeled element in this PeakData as a
        fraction of the total abundance of the labeled element in this
        PeakGroup.

        Accucor calculates this as "Normalized", but TraceBase renames it to
        "fraction" to avoid confusion with other variables like "normalized
        labeling".
        """
        try:
            fraction = self.corrected_abundance / self.peak_group.total_abundance
        except ZeroDivisionError:
            fraction = None
        return fraction

    class Meta:
        verbose_name = "peak data"
        verbose_name_plural = "peak data"
        ordering = ["peak_group", "-corrected_abundance"]

    @classmethod
    def get_or_create(cls, label_obs: list, **kwargs):
        """Get or create a PeakData record.

        PeakData has no unique fields or unique constraints.  There can be records that are essentially duplicates,
        however when taken together with PeakDataLabel records, they can be uniquely managed manually.  So this method
        provides get_or_create functionality by assessing uniqueness in the context of PeakDataLabel records.  It
        searches for a PeakData record either with no associated PeakDataLabel records or PeakData records with the
        associated label.  Note that PeakDataLabel records have a unique constraint that only allows a single instance
        of a particular element associated with a PeakData record, i.e. only a single count.  That's because it
        represents a single observation.  There cannot be multiple counts of an element.  Theoretically, there could be
        multiple mass numbers, but that's just something Tracebase currently does not support.

        NOTE: It should go without saying, but since it uses the create() method, it does a save(), but does not call
        full_clean().  That is up to the caller, just like with any get_cor_create method.

        Args:
            label_obs (List[ObservedIsotopeData]): List of isotope observations all associated with the same PeakData
                record.
            kwargs (dict): PeakData field values keyed on PeakData field names.
        Exceptions:
            None
        Returns:
            rec (PeakData)
            created (bool)
        """
        rec_dict = kwargs
        matching_recs = []
        orphan_recs = []

        # A PeakData record with identical values (e.g. med_mz=0, med_rt=0, raw_abundance=0, and corrected_abundance=0)
        # could exist, but have a different set of labels associated with it.  The only way to tell them apart is by
        # their associated labels (PeakDataLabel records), so we will be comparing associated labels to get or create
        # the record we want.

        # First, see if there are any records matching the basic criteria
        recs = PeakData.objects.filter(**kwargs)

        # If there are none, it's easy:
        if len(recs) == 0:
            return PeakData.objects.create(**kwargs), True

        for potential_rec in recs:
            # If there end up being no matches, we will return a lone orphan if one exists
            if potential_rec.labels.count() == 0:
                orphan_recs.append(potential_rec)
                continue
            # Build a Q expression for matching PeakDataLabel records
            labels_q = Q()
            for label in label_obs:
                labels_q |= Q(
                    peak_data=potential_rec,
                    element=label["element"],
                    count=label["count"],
                    mass_number=label["mass_number"],
                )
            matching_labels = potential_rec.labels.filter(labels_q)
            if potential_rec.labels.count() == len(label_obs) and set(
                potential_rec.labels.all()
            ) == set(matching_labels.all()):
                matching_recs.append(potential_rec)

        if len(matching_recs) == 1:
            return matching_recs[0], False
        elif len(matching_recs) > 1:
            raise PeakData.MultipleObjectsReturned()
        elif len(orphan_recs) == 0:
            return PeakData.objects.create(**rec_dict), True
        elif len(orphan_recs) == 1:
            # Even though this doesn't have the labels in the query, we will return the orphan.  This isn't a method
            # meant to create PeakDataLabel records.  It's up to the caller to do the create.
            return orphan_recs[0], False
        else:
            raise PeakData.MultipleObjectsReturned()
