from django.contrib import admin

from .models import Compound


@admin.register(Compound)
class CompoundAdmin(admin.ModelAdmin):
    list_display = ["name", "formula", "hmdb_id"]
