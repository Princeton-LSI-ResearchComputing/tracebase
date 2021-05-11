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
    path("compound/<slug:slug>/", views.compound_detail.as_view(), name="compound_detail"),
    path("study/<slug:slug>/", views.study_detail.as_view(), name="study_detail"),
    path("animal/<slug:slug>/", views.animal_detail.as_view(), name="animal_detail"),
    path("tissue/<slug:slug>/", views.tissue_detail.as_view(), name="tissue_detail"),
    path("sample/<slug:slug>/", views.sample_detail.as_view(), name="sample_detail"),
    path("protocol/<slug:slug>/", views.protocol_detail.as_view(), name="protocol_detail"),
    path("msrun/<slug:slug>/", views.msrun_detail.as_view(), name="msrun_detail"),
    path("peakgroup/<slug:slug>/", views.peakgroup_detail.as_view(), name="peakgroup_detail"),
    path("peakdata/<slug:slug>/", views.peakdata_detail.as_view(), name="peakdata_detail"),
]
