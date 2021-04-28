from django.urls import path

from DataRepo import views

urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path("compound/",  views.CompoundListView.as_view(), name="compound_list"),
    path("compound/<int:pk>", views.CompoundDetailView.as_view(), name="compound_detail"),
]
