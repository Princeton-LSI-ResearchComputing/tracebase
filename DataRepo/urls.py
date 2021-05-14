from django.urls import path

from . import views

urlpatterns = [
    # Home
    path("", views.home, name="home"),
    # List Views
    path("compound/", views.compound_list.as_view(), name="compound_list"),
    path("study/", views.study_list.as_view(), name="study_list"),
    path("animal/", views.animal_list.as_view(), name="animal_list"),
    path("tissue/", views.tissue_list.as_view(), name="tissue_list"),
    path("sample/", views.sample_list.as_view(), name="sample_list"),
    path("protocol/", views.protocol_list.as_view(), name="protocol_list"),
    path("msrun/", views.msrun_list.as_view(), name="msrun_list"),
    path("peakgroup/", views.peakgroup_list.as_view(), name="peakgroup_list"),
    path("peakdata/", views.peakdata_list.as_view(), name="peakdata_list"),
    # Detail Views
    path(
        "compound/<slug:slug>/", views.compound_detail.as_view(), name="compound_detail"
    ),
    path("study/<slug:slug>/", views.study_detail.as_view(), name="study_detail"),
    path("animal/<slug:slug>/", views.animal_detail.as_view(), name="animal_detail"),
    path("tissue/<slug:slug>/", views.tissue_detail.as_view(), name="tissue_detail"),
    path("sample/<slug:slug>/", views.sample_detail.as_view(), name="sample_detail"),
    path(
        "protocol/<slug:slug>/", views.protocol_detail.as_view(), name="protocol_detail"
    ),
    path("msrun/<slug:slug>/", views.msrun_detail.as_view(), name="msrun_detail"),
    path(
        "peakgroup/<slug:slug>/",
        views.peakgroup_detail.as_view(),
        name="peakgroup_detail",
    ),
    path(
        "peakdata/<slug:slug>/", views.peakdata_detail.as_view(), name="peakdata_detail"
    ),
]
