from __future__ import annotations

from typing import Optional

from django.core.exceptions import ObjectDoesNotExist
from django.db import models, transaction
from django.db.models.functions import Lower

from DataRepo.models.element_label import ElementLabel
from DataRepo.models.maintained_model import MaintainedModel
from DataRepo.models.utilities import get_model_by_name
from DataRepo.utils.infusate_name_parser import TracerData


class TracerQuerySet(models.QuerySet):
    @transaction.atomic
    def get_or_create_tracer(self, tracer_data: TracerData) -> tuple[Tracer, bool]:
        """Get Tracer matching the tracer_data, or create a new tracer"""

        tracer = self.get_tracer(tracer_data)
        created = False
        if tracer is None:
            TracerLabel = get_model_by_name("TracerLabel")
            Compound = get_model_by_name("Compound")
            compound = Compound.compound_matching_name_or_synonym(
                tracer_data["compound_name"]
            )
            tracer = self.create(compound=compound)
            for isotope_data in tracer_data["isotopes"]:
                TracerLabel.objects.create_tracer_label(tracer, isotope_data)
            tracer.full_clean()
            tracer.save()
            created = True
        return (tracer, created)

    def get_tracer(self, tracer_data: TracerData) -> Optional[Tracer]:
        """Get Tracer matching the tracer_data"""
        matching_tracer = None

        # First, check if the compound is found
        Compound = get_model_by_name("Compound")
        try:
            compound = Compound.compound_matching_name_or_synonym(
                tracer_data["compound_name"]
            )
        except ObjectDoesNotExist:
            compound = None

        if compound:
            # Check for tracers of the compound with same number of labels
            tracers = Tracer.objects.annotate(num_labels=models.Count("labels")).filter(
                compound=compound, num_labels=len(tracer_data["isotopes"])
            )
            # Check that the labels match
            for tracer_label in tracer_data["isotopes"]:
                tracers = tracers.filter(
                    labels__element=tracer_label["element"],
                    labels__mass_number=tracer_label["mass_number"],
                    labels__count=tracer_label["count"],
                    labels__positions=tracer_label["positions"],
                )
            if tracers.count() == 1:
                matching_tracer = tracers.first()
        return matching_tracer


@MaintainedModel.relation(
    generation=1,
    parent_field_name="compound",
    update_label="tracer_stat",
)
class Tracer(MaintainedModel, ElementLabel):
    objects: TracerQuerySet = TracerQuerySet().as_manager()

    COMPOUND_DELIMITER = "-"
    LABELS_DELIMITER = ","
    LABELS_LEFT_BRACKET = "["
    LABELS_RIGHT_BRACKET = "]"
    LABELS_COMBO_DELIMITER = "+"

    detail_name = "tracer_detail"

    id = models.AutoField(primary_key=True)
    name = models.CharField(
        max_length=256,
        unique=True,
        null=True,
        editable=False,
        help_text="A unique name or lab identifier of the tracer, e.g. 'lysine-C14'.",
    )
    compound = models.ForeignKey(
        to="DataRepo.Compound",
        on_delete=models.RESTRICT,
        null=False,
        related_name="tracers",
        help_text="A compound used as a tracer, containing some elements replaced with isotopes.",
    )
    label_combo = models.CharField(
        max_length=16,  # Max of 8, 2-letter elements
        null=True,
        editable=False,
        help_text=f"The tracer's ordered combination of elements, delimited by '{LABELS_COMBO_DELIMITER}'.",
    )

    class Meta:
        verbose_name = "tracer"
        verbose_name_plural = "tracers"
        ordering = [Lower("name")]

    def __str__(self):
        return str(self._name())

    @MaintainedModel.setter(
        generation=2,
        update_field_name="name",
        parent_field_name="infusates",
        update_label="name",
    )
    def _name(self):
        return self.name_with_synonym()

    @MaintainedModel.setter(
        generation=3,
        update_field_name="label_combo",
        parent_field_name="infusates",
        update_label="label_combo",
    )
    def _label_combo(self):
        """Generates a string to populate the label_combo field.

        The update of this record is triggered when the record is saved.  NOTE: TracerLabel record changes trigger
        updates here, but they do so from the update_label 'name'.

        TODO: Create a separate trigger/label or add the ability for the same label to be applied to multiple methods.

        Updates here trigger label_combo updates to linked Infusate records."""
        return self.LABELS_COMBO_DELIMITER.join(
            [str(label.element) for label in self.labels.order_by("element")]
        )

    def name_with_synonym(self, synonym=None):
        """This will create the same name that _name does, but it will use the supplied compound synonym instead (after
        checking that it is a valid synonym)."""
        if (
            synonym is not None
            and self.compound.synonyms.filter(name__iexact=synonym).count() > 0
        ):
            name = synonym
        else:
            name = self.compound.name
        # format: `compound-[labelname,labelname,...]`, e.g. lysine-[13C6,15N2]
        if (
            self.id is None
            or self.labels is None  # pylint: disable=no-member
            or self.labels.count() == 0
        ):
            return name
        labels_string = self.LABELS_DELIMITER.join(
            [str(label) for label in self.labels.all()]
        )
        return (
            f"{name}{self.COMPOUND_DELIMITER}"
            f"{self.LABELS_LEFT_BRACKET}{labels_string}{self.LABELS_RIGHT_BRACKET}"
        )

    @classmethod
    def name_from_data(cls, tracer_data: TracerData):
        """Build a tracer name (not in the database) from a TracerData object.

        Assumptions:
            sorted() sorts the same way the TracerLabel records are sorted WRT alphanumeric characters
        Args:
            tracer_data (TracerData)
        Exceptions:
            None
        Returns:
            (str)
        """
        from DataRepo.models.tracer_label import TracerLabel

        labels_string = cls.LABELS_DELIMITER.join(
            sorted(
                map(
                    lambda iso_data: TracerLabel.name_from_data(iso_data),
                    tracer_data["isotopes"],
                )
            )
        )
        return (
            f"{tracer_data['compound_name']}{cls.COMPOUND_DELIMITER}"
            f"{cls.LABELS_LEFT_BRACKET}{labels_string}{cls.LABELS_RIGHT_BRACKET}"
        )

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})
