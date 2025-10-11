from django.db.models import CharField, F, Func, Value
from django.views.generic import DetailView

from DataRepo.models import DATE_FORMAT, DBSTRING_FUNCTION, MSRunSequence
from DataRepo.models.researcher import Researcher
from DataRepo.views.models.bst.query import BSTListView


class MSRunSequenceDetailView(DetailView):
    model = MSRunSequence
    template_name = "models/msrunsequence/msrunsequence_detail.html"
    context_object_name = "sequence"


class MSRunSequenceListView(BSTListView):
    model = MSRunSequence
    exclude = ["id", "date", "msrun_samples"]
    column_ordering = [
        "details",
        "instrument",
        "lc_method",
        "date_str",
        "researcher",
        "notes",
        "msrun_samples_mm_count",
    ]
    annotations = {
        # A formatted date is necessary for searchability, because otherwise, the date is rendered using Django's
        # __str__ method, but search applies to how the database renders a date (and they differ)
        "date_str": Func(
            F("date"),
            Value(DATE_FORMAT),
            output_field=CharField(),
            function=DBSTRING_FUNCTION,
        ),
        "details": Value("details"),
    }
    column_settings = {
        "lc_method": {"filterer": {"distinct_choices": True}},
        "researcher": {"filterer": {"choices": Researcher.get_researchers}},
        "date_str": {"header": "Date"},
        "details": {
            "linked": True,
            "searchable": False,
            "filterable": False,
            "sortable": False,
            "header": "MS Run Sequence",
            "tooltip": "Links to the details of an MS Run Sequence record.",
        },
        "msrun_samples_mm_count": {
            "header": "MSRun Sample Count",
            "tooltip": (
                "The number of MS Run Sample records that peak groups link to (not representative of the number of MS "
                "Run Sequences or the number of samples analyzed in a sequence due to placeholder records)."
            ),
        },
    }
