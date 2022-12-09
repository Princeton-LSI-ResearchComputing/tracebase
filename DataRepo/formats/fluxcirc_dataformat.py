from django.db.models import F

from DataRepo.formats.dataformat import Format
from DataRepo.models import Animal, ElementLabel, FCirc


class FluxCircFormat(Format):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "fctemplate"
    name = "Fcirc"
    rootmodel = FCirc
    stats = None
    model_instances = {
        "FCirc": {
            "model": "FCirc",
            "path": "",
            "reverse_path": "",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
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
                "is_last": {
                    "displayname": "Is Last Serum Tracer Peak Group",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": [
                        (True, "true"),
                        (False, "false"),
                    ],
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
        "Tracer": {
            "model": "Tracer",
            "path": "tracer",
            "reverse_path": "samples__fcircs",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tracer Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tracer",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "serum_sample__animal",
            "reverse_path": "samples__fcircs",
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
                    "displayname": "Animal Age",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                    "units": {  # See dataformat.py: Format.unit_options
                        "key": "postgres_interval",
                        "default": "weeks",
                        "subset": [
                            "months",
                            "weeks",
                            "days",
                            "hours",
                        ],
                    },
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
            "path": "serum_sample__animal__infusate__tracer_links",
            "reverse_path": "infusate__animals__samples__fcircs",
            "manyrelated": {
                "is": True,
                "manytomany": False,
                "split_rows": True,
                "through": True,
                "root_annot_fld": "tracer_link",
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
            "path": "serum_sample__animal__treatment",
            "reverse_path": "animals__samples__fcircs",
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
            "path": "serum_sample",
            "reverse_path": "fcircs",
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
                    "displayname": "Time Collected (since infusion)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                    "units": {  # See dataformat.py: Format.unit_options
                        "key": "postgres_interval",
                        "default": "minutes",
                        "subset": [
                            "hours",
                            "minutes",
                            "seconds",
                        ],
                    },
                },
            },
        },
        "Compound": {
            "model": "Compound",
            "path": "tracer__compound",
            "reverse_path": "tracers__fcircs",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
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
            "path": "serum_sample__animal__studies",
            "reverse_path": "animals__samples__fcircs",
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
        """
        Ensure we only get tracer_link records that are for the infusate linked to the animal and for the tracer linked
        in FCirc.
        """
        return FCirc.objects.filter(
            tracer__id__exact=F(
                "serum_sample__animal__infusate__tracer_links__tracer__id"
            )
        )
