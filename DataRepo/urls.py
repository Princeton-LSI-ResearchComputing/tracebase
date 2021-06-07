from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("compounds/", views.CompoundListView.as_view(), name="compound_list"),
    path(
        "compounds/<int:pk>/",
        views.CompoundDetailView.as_view(),
        name="compound_detail",
    ),
    path("studies/", views.StudyListView.as_view(), name="study_list"),
    path("studies/<int:pk>/", views.StudyDetailView.as_view(), name="study_detail"),
    path(
        "search_basic/<str:mdl>/<str:fld>/<str:cmp>/<str:val>/<str:fmt>/",
        views.search_basic,
        name="search_basic",
    ),
]
