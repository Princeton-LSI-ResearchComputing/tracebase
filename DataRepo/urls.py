from django.urls import path

from DataRepo import views

urlpatterns = [
    path("", views.HomeView.as_view(), name="home"),
    path("compound/", views.CompoundListView.as_view(), name="compound_list"),
    path(
        "compound/<int:pk>", views.CompoundDetailView.as_view(), name="compound_detail"
    ),
    path("study", views.StudyListView.as_view(), name="study_list"),
    path("study/<int:pk>", views.StudyDetailView.as_view(), name="study_detail"),
    path("protocol", views.ProtocolListView.as_view(), name="protocol_list"),
    path(
        "protocol/<int:pk>", views.ProtocolDetailView.as_view(), name="protocol_detail"
    ),
    path("animal", views.AnimalListView.as_view(), name="animal_list"),
    path("animal/<int:pk>", views.AnimalDetailView.as_view(), name="animal_detail"),   
    path("sample", views.SampleListView.as_view(), name="sample_list"),
    path("sample/<int:pk>", views.SampleDetailView.as_view(), name="sample_detail"),
    path("msrun", views.MSRunListView.as_view(), name="msrun_list"),
    path("msrun/<int:pk>", views.MSRunDetailView.as_view(), name="msrun_detail"),
    path("peakgroup", views.PeakGroupListView.as_view(), name="peakgroup_list"),
    path("peakgroup/<int:pk>", views.PeakGroupDetailView.as_view(), name="peakgroup_detail"),
]
