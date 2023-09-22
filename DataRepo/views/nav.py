from django.shortcuts import render
from django.urls import reverse

from DataRepo.models import (
    Animal,
    Compound,
    LCMethod,
    PeakGroupSet,
    Protocol,
    Sample,
    Study,
    Tissue,
)
from DataRepo.utils import leaderboard_data


def home(request):
    """
    Home page contains cards for browsing data
    keep model cards in two lists for displaying in two rows
    keep card for advanced search in separate row
    """
    card_grid = [[], [], []]

    # first list
    card_grid[0].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Study.objects.all().count()) + " Studies",
            "card_foot_url": reverse("study_list"),
        }
    )

    card_grid[0].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Animal.objects.all().count()) + " Animals",
            "card_foot_url": reverse("animal_list"),
        }
    )

    card_grid[0].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Tissue.objects.all().count()) + " Tissues",
            "card_foot_url": reverse("tissue_list"),
        }
    )

    card_grid[0].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(Sample.objects.all().count()) + " Samples",
            "card_foot_url": reverse("sample_list"),
        }
    )

    # second list
    card_grid[1].append(
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

    card_grid[1].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(comp_count)
            + " Compounds ("
            + str(tracer_count)
            + " tracers)",
            "card_foot_url": reverse("compound_list"),
        }
    )

    card_grid[1].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(
                Protocol.objects.filter(category=Protocol.ANIMAL_TREATMENT).count()
            )
            + " Animal Treatments",
            "card_foot_url": reverse("animal_treatment_list"),
        }
    )

    card_grid[1].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(
                Protocol.objects.filter(category=Protocol.MSRUN_PROTOCOL).count()
            )
            + " Mass Spectrometry Protocols",
            "card_foot_url": reverse("msrun_protocol_list"),
        }
    )

    card_grid[2].append(
        {
            "card_bg_color": "bg-card-1",
            "card_body_title": str(LCMethod.objects.count())
            + " LiquidChromatography Protocols",
            "card_foot_url": reverse("lcmethod_list"),
        }
    )

    card_grid[2].append(
        {
            "card_bg_color": "bg-card-2",
            "card_body_title": "Advanced Search",
            "card_foot_url": reverse("search_advanced"),
        }
    )

    context = {}
    context["card_grid"] = card_grid
    context["leaderboards"] = leaderboard_data()

    return render(request, "home.html", context)
