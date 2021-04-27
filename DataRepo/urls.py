from django.urls import path

from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("compound/", views.compound_list, name="compounds"),
    path("compound/<int:cpd_id>/", views.compound_detail, name="compound_detail"),
]
