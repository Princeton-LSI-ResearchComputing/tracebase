from django.apps import apps
from django.contrib import admin

from DataRepo.models import Compound


@admin.register(Compound)
class CompoundAdmin(admin.ModelAdmin):
    list_display = ["name", "formula", "hmdb_id"]


# Use default ModelAdmin for remaining DataRepo models
datarepo_models = apps.get_app_config("DataRepo").get_models()

for model in datarepo_models:
    try:
        admin.site.register(model)
    except admin.sites.AlreadyRegistered:
        pass
