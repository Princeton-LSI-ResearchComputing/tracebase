from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("dashboard/", views.dashboard, name="dashboard"),
    # path("compound/", views.compound_list, name="compound_list"),
    # path("compound/<int:cpd_id>/", views.compound_detail, name="compound_detail"
    # path for each CBV
    path("compounds/", views.CompoundListView.as_view(), name="compound_list"),
    path(
        "compounds/<int:pk>/",
        views.CompoundDetailView.as_view(),
        name="compound_detail",
    ),
    path("studies/", views.StudyListView.as_view(), name="study_list"),
    path("studies/<int:pk>/", views.StudyDetailView.as_view(), name="study_detail"),
]
