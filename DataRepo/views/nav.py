from django.shortcuts import render
from django.urls import reverse

from DataRepo.models import (
    Animal,
    Compound,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
)
from DataRepo.utils import leaderboard_data


def home(request):
    """
    Home page contains 8 cards for browsing data
    keep card attributes in two lists for displaying cards in two rows
    """
    card_attrs_list1 = []
    card_attrs_list2 = []

    # first list
    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Study.objects.all().count()) + " Studies",
            "card_foot_url": reverse("study_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Animal.objects.all().count()) + " Animals",
            "card_foot_url": reverse("animal_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Tissue.objects.all().count()) + " Tissues",
            "card_foot_url": reverse("tissue_list"),
        }
    )

    card_attrs_list1.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Sample.objects.all().count()) + " Samples",
            "card_foot_url": reverse("sample_list"),
        }
    )

    # second list
    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(PeakGroupSet.objects.all().count())
            + " AccuCor Files",
            "card_foot_url": reverse("peakgroupset_list"),
        }
    )

    comp_count = Compound.objects.all().count()
    tracer_count = (
        Animal.objects.exclude(infusate__tracers__compound__id__isnull=True)
        .order_by("infusate__tracers__compound__id")
        .values_list("infusate__tracers__compound__id")
        .distinct("infusate__tracers__compound__id")
        .count()
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(comp_count)
            + " Compounds ("
            + str(tracer_count)
            + " tracers)",
            "card_foot_url": reverse("compound_list"),
        }
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Protocol.objects.all().count()) + " Protocols",
            "card_foot_url": reverse("protocol_list"),
        }
    )

    card_attrs_list2.append(
        {
            "card_bg_color": "bg-card-2",
            "card_body_title": "Advanced Search",
            "card_foot_url": reverse("search_advanced"),
        }
    )

    card_row_list = [card_attrs_list1, card_attrs_list2]

    context = {}
    context["card_rows"] = card_row_list
    context["leaderboards"] = leaderboard_data()

    return render(request, "home.html", context)
