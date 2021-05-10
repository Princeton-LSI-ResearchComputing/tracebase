from django.urls import path

from . import views
from DataRepo.views import compound_list, study_list, animal_list, tissue_list, sample_list, protocol_list, msrun_list, peakgroup_list, peakdata_list

urlpatterns = [
    # Home
    path("", views.home, name="home"),

    # List Views
    path("compound/", compound_list.as_view(), name="compound_list"),
    path("study/", study_list.as_view(), name="study_list"),
    path("animal/", animal_list.as_view(), name="animal_list"),
    path("tissue/", tissue_list.as_view(), name="tissue_list"),
    path("sample/", sample_list.as_view(), name="sample_list"),
    path("protocol/", protocol_list.as_view(), name="protocol_list"),
    path("msrun/", msrun_list.as_view(), name="msrun_list"),
    path("peakgroup/", peakgroup_list.as_view(), name="peakgroup_list"),
    path("peakdata/", peakdata_list.as_view(), name="peakdata_list"),

    # Detail Views
    path("compound/<int:cpd_id>/", views.compound_detail, name="compound_detail"),
]
