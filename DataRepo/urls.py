from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("compound/", views.compound_list, name="compound_list"),
    path("compound/<int:cpd_id>/", views.compound_detail, name="compound_detail"),
    path("study/", views.StudyListView.as_view(), name="study_list"),
    path("study/<int:pk>/", views.StudyDetailView.as_view(), name="study_detail"),
    path(
        "study_peakgroups/<str:idval>/",
        views.study_peakgroups,
        name="study_peakgroups",
    ),
]
