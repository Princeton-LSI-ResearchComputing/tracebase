from django.contrib import admin

from .models import Animal, Compound, Sample, Study, Tissue


@admin.register(Compound)
class CompoundAdmin(admin.ModelAdmin):
    list_display = ["name", "formula", "hmdb_id"]


@admin.register(Study)
class StudyAdmin(admin.ModelAdmin):
    list_display = ["name", "description"]


@admin.register(Animal)
class AnimalAdmin(admin.ModelAdmin):
    list_display = ["name", "genotype"]


@admin.register(Tissue)
class TissueAdmin(admin.ModelAdmin):
    list_display = ["name"]


@admin.register(Sample)
class SampleAdmin(admin.ModelAdmin):
    list_display = ["name", "date", "researcher"]
