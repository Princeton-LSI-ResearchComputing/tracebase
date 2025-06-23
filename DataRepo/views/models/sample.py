from django.db.models import CharField, F, Func, Value
from django.db.models.functions import Extract
from django.views.generic import DetailView

from DataRepo.models import Sample
from DataRepo.models.researcher import Researcher
from DataRepo.views.models.bst.list_view import BSTListView

# Postgres-specific function values for annotations
DATE_FORMAT = "YYYY-MM-DD"  # Postgres date format syntax
DBSTRING_FUNCTION = "to_char"  # Postgres function
DURATION_SECONDS_ATTRIBUTE = "epoch"  # Postgres interval specific


class SampleListView(BSTListView):
    model = Sample

    # Column order
    column_ordering = [
        "name",
        "animal",
        "tissue",
        "animal__studies",
        "animal__genotype",
        "animal__infusate",
        "animal__infusate__tracer_links__tracer",
        "animal__infusate__tracer_links__tracer__compound",
        "animal__infusate__tracer_links__concentration",
        "animal__label_combo",
        "animal__infusion_rate",
        "animal__treatment",
        "animal__body_weight",
        "age_weeks",
        "animal__sex",
        "animal__diet",
        "animal__feeding_status",
        "researcher",
        "date_str",
        "time_collected_mins",
        "msrun_samples__msrun_sequence__researcher",
        "msrun_samples__msrun_sequence__date",
        "msrun_samples",
    ]

    # fmt: off

    # Exclude these auto-added columns
    exclude = [
        # Fields that are only used internally
        "id",
        "is_serum_sample",

        # Annotations to convert the way these columns are displayed/searched.
        # These make the search/filter guaranteed to match what the user sees.
        "time_collected",  # time_collected_mins
        "animal__age",  # age_weeks
        "date",  # date_str

        # Reverse relations that are not desired
        "animals",  # Animal.last_serum_sample
        "fcircs",
    ]

    # This applies custom settings to select columns.
    column_settings = {
        # Column customizations
        "animal__genotype": {"filterer": {"distinct_choices": True}},
        "animal__label_combo": {"filterer": {"distinct_choices": True}},
        "animal__body_weight": {"visible": False},
        "animal__sex": {"visible": False},
        "animal__diet": {
            "filterer": {"distinct_choices": True},
            "visible": False,
        },
        "animal__feeding_status": {"filterer": {"distinct_choices": True}},
        "researcher": {"filterer": {"choices": Researcher.get_researchers}},

        # Annotation column customizations
        "time_collected_mins": {
            "header": "Time Collected (m)",
            "tooltip": "Units: minutes.",
        },
        "age_weeks": {
            "header": "Animal Age (w)",
            "visible": False,
            "tooltip": "Units: weeks.",
        },
        "date_str": {"header": "Date"},
        "tracer_links_mm_count": {"header": "Tracer Count"},
        "msrun_samples_mm_count": {"header": "MSRun Count"},

        # Many-related model column customizations

        # Tracer columns
        "animal__infusate__tracer_links__tracer": {"header": "Tracer"},
        "animal__infusate__tracer_links__tracer__compound": {
            "header": "Tracer Compound"
        },
        "animal__infusate__tracer_links__concentration": {
            "header": "Tracer Concentration (mM)"
        },

        # MSRun columns
        "msrun_samples": {"header": "MSRun"},
        "msrun_samples__msrun_sequence__researcher": {
            "filterer": {"choices": Researcher.get_researchers},
            "header": "MSRun Operator",
        },
        "msrun_samples__msrun_sequence__date": {"header": "MSRun Date"},
    }

    # fmt: on

    annotations = {
        "time_collected_mins": Extract(
            F("time_collected"),
            DURATION_SECONDS_ATTRIBUTE,
        )
        / Value(60),
        "date_str": Func(
            F("date"),
            Value(DATE_FORMAT),
            output_field=CharField(),
            function=DBSTRING_FUNCTION,
        ),
        "age_weeks": Extract(
            F("animal__age"),
            DURATION_SECONDS_ATTRIBUTE,
        )
        / Value(604800),
    }


class SampleDetailView(DetailView):
    """Generic class-based detail view for a sample"""

    model = Sample
    template_name = "models/sample/sample_detail.html"
