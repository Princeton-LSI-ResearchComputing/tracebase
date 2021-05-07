from django.urls import path

from . import views
from DataRepo.views import compound_list #, study_list

urlpatterns = [
    path("", views.home, name="home"),
    path("compound/", compound_list.as_view(), name="compound_list"),
    path("compound/<int:cpd_id>/", views.compound_detail, name="compound_detail"),
    #path("study/", study_list.as_view(), name="study_list"),
]
