from django.contrib import admin

from .models.compound import Compound


@admin.register(Compound)
class CompoundAdmin(admin.ModelAdmin):
    list_display = ["name", "formula", "hmdb_id"]
