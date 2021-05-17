from django.urls import path, re_path

from DataRepo import views

urlpatterns = [
    path("", views.HomeView.as_view(), name="home"),
    path("compound/", views.CompoundListView.as_view(), name="compound_list"),
    path(
        "compound/<int:pk>/", views.CompoundDetailView.as_view(), name="compound_detail"
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
    re_path(
        "^samples/(?P<animal_id>\d+$)/",
        views.SampleListView.as_view(),
        name="sample_list",
    ),
    path("samples/<int:pk>/", views.SampleDetailView.as_view(), name="sample_detail"),
    path("msruns/", views.MSRunListView.as_view(), name="msrun_list"),
    path("msruns/<int:pk>/", views.MSRunDetailView.as_view(), name="msrun_detail"),
    path("peakgroups/", views.PeakGroupListView.as_view(), name="peakgroup_list"),
    re_path(
        "^peakgroups/(?P<ms_run_id>\d+\$)/",
        views.PeakGroupListView.as_view(),
        name="peakgroup_list",
    ),
    path(
        "peakgroups/<int:pk>/",
        views.PeakGroupDetailView.as_view(),
        name="peakgroup_detail",
    ),
    path("peakdata/", views.PeakDataListView.as_view(), name="peakdata_list"),
    re_path(
        "^peakdata/(?P<peak_group_id>\d+\+$)/",
        views.PeakDataListView.as_view(),
        name="peakdata_list",
    ),
]
