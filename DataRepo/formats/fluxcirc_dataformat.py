from DataRepo.formats.dataformat import Format
from DataRepo.models import Animal, ElementLabel, PeakGroup, Tissue


class FluxCircFormat(Format):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "fctemplate"
    name = "Fcirc"
    rootmodel = PeakGroup
    stats = None
    model_instances = {
        "PeakGroup": {
            "model": "PeakGroup",
            "path": "",
            "reverse_path": "",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    # "handoff": "",
                    # Using in link will expose the internal index field in the search form because there's no
                    # searchable unique field for handoff
                    "type": "number",
                },
            },
        },
        "PeakGroupLabel": {
            "model": "PeakGroupLabel",
            "path": "peak_group_labels",
            "reverse_path": "peak_group",
            "manyrelated": {
                "is": True,
                "through": False,
                "manytomany": False,
                "split_rows": True,
                "root_annot_fld": "peak_group_label",  # Used to annotate root rec w/ subtable ID when split_rows=True
            },
            "fields": {
                "element": {
                    "displayname": "Peak Group Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": ElementLabel.LABELED_ELEMENT_CHOICES,
                    "root_annot_fld": "element",  # Used to annotate root rec split_rows=True
                },
                "rate_disappearance_average_per_gram": {
                    "displayname": "Average Rd (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_average_per_gram": {
                    "displayname": "Average Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_average_weight_normalized": {
                    "displayname": "Average Rd (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_average_weight_normalized": {
                    "displayname": "Average Ra (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_intact_per_gram": {
                    "displayname": "Intact Rd (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_intact_per_gram": {
                    "displayname": "Intact Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_disappearance_intact_weight_normalized": {
                    "displayname": "Intact Rd (nmol/min)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "rate_appearance_intact_weight_normalized": {
                    "displayname": "Intact Ra (nmol/min/g)",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "msrun__sample__animal",
            "reverse_path": "samples__msruns__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Animal Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "body_weight": {
                    "displayname": "Body Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "age": {
                    "displayname": "Animal Age (d-hh:mm:ss)",
                    "searchable": False,  # currently no data available for testing and a bug: issue #180
                    "displayed": True,
                    "type": "number",
                },
                "sex": {
                    "displayname": "Sex",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": Animal.SEX_CHOICES,
                },
                "diet": {
                    "displayname": "Diet",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "infusion_rate": {
                    "displayname": "Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "InfusateTracer": {
            "model": "InfusateTracer",
            "path": "msrun__sample__animal__infusate__tracer_links",
            "reverse_path": "infusate__animal__samples__msruns__peak_groups",
            "manyrelated": {
                "is": True,
                "manytomany": True,
                "split_rows": False,
                "through": True,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Infusate Tracer Link Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "concentration",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "concentration": {
                    "displayname": "Tracer Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "model": "Protocol",
            "path": "msrun__sample__animal__treatment",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Sample": {
            "model": "Sample",
            "path": "msrun__sample",
            "reverse_path": "msruns__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    # "handoff": "",
                    # Using in link will expose the internal index field in the search form because there's no
                    # searchable unique field for handoff
                    "type": "number",
                },
                "time_collected": {
                    "displayname": "Time Collected (hh:mm:ss since infusion)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Compound": {
            "model": "Compound",
            "path": "msrun__sample__animal__infusate__tracers__compound",
            "reverse_path": "tracer__infusates__animals__samples__msruns__peak_groups",
            "manyrelated": {
                "is": True,
                "through": False,
                "manytomany": True,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tracer Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tracer Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "msrun__sample__animal__studies",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manyrelated": {
                "is": True,
                "through": False,
                "manytomany": True,
                "split_rows": False,
                "root_annot_fld": "study",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
    }

    def getRootQuerySet(self):
        # https://stackoverflow.com/questions/3397437/manually-create-a-django-queryset-or-rather-manually-add-objects-to-a-queryset
        serum_tracer_peakgroups = set()
        for pg in self.rootmodel.objects.filter(
            msrun__sample__tissue__name__istartswith=Tissue.SERUM_TISSUE_PREFIX
        ):
            if pg.is_tracer_compound_group:
                serum_tracer_peakgroups.add(pg.id)
        return self.rootmodel.objects.filter(id__in=serum_tracer_peakgroups)
