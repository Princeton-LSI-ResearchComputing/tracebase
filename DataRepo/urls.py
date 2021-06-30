from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path(
        "search_basic/<str:mdl>/<str:fld>/<str:cmp>/<str:val>/<str:fmt>/",
        views.search_basic,
        name="search_basic",
    ),
    path(
        "search_peakgroups/",
        views.AdvSearchPeakGroupsView.as_view(),
        name="search_peakgroups",
    ),
    path("compounds/", views.CompoundListView.as_view(), name="compound_list"),
    path(
        "compounds/<int:pk>/",
        views.CompoundDetailView.as_view(),
        name="compound_detail",
    ),
    path("studies/", views.StudyListView.as_view(), name="study_list"),
    path("studies/<int:pk>/", views.StudyDetailView.as_view(), name="study_detail"),
    path("protocols/", views.ProtocolListView.as_view(), name="protocol_list"),
    path(
        "protocols/<int:pk>/",
        views.ProtocolDetailView.as_view(),
        name="protocol_detail",
    ),
    path("animals/", views.AnimalListView.as_view(), name="animal_list"),
    path("animals/<int:pk>/", views.AnimalDetailView.as_view(), name="animal_detail"),
    path("samples/", views.SampleListView.as_view(), name="sample_list"),
    path("samples/<int:pk>/", views.SampleDetailView.as_view(), name="sample_detail"),
    path("msruns/", views.MSRunListView.as_view(), name="msrun_list"),
    path("msruns/<int:pk>/", views.MSRunDetailView.as_view(), name="msrun_detail"),
    path(
        "peakgroupsets/", views.PeakGroupSetListView.as_view(), name="peakgroupset_list"
    ),
    path(
        "peakgroupsets/<int:pk>/",
        views.PeakGroupSetDetailView.as_view(),
        name="peakgroupset_detail",
    ),
    path("peakgroups/", views.PeakGroupListView.as_view(), name="peakgroup_list"),
    path(
        "peakgroups/<int:pk>/",
        views.PeakGroupDetailView.as_view(),
        name="peakgroup_detail",
    ),
    path("peakdata/", views.PeakDataListView.as_view(), name="peakdata_list"),
]
