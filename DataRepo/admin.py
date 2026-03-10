from django.contrib import admin

from .models import Compound


@admin.register(Compound)
class CompoundAdmin(admin.ModelAdmin):
    list_display = ["name", "formula", "hmdb_id"]
    readonly_fields = ["animals_by_tracer"]
