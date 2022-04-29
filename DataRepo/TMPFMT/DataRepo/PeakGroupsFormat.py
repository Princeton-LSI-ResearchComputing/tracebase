from DataRepo.TMPFMT.Format import Format
from DataRepo.models import Animal, PeakGroup, TracerLabeledClass


class PeakGroupsFormat(Format):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pgtemplate"
    name = "PeakGroups"
    rootmodel = PeakGroup
    stats = [
        {
            "displayname": "Animals",
            "distincts": ["msrun__sample__animal__name"],
            "filter": None,
        },
        {
            "displayname": "Labeled Elements",
            "distincts": ["peak_data__labeled_element"],
            "filter": None,
        },
        {
            "displayname": "Measured Compounds",
            "distincts": ["compounds__name"],
            "filter": None,
        },
        {
            "displayname": "Samples",
            "distincts": ["msrun__sample__name"],
            "filter": None,
        },
        {
            "displayname": "Tissues",
            "distincts": ["msrun__sample__tissue__name"],
            "filter": None,
        },
        {
            "displayname": "Tracer Compounds",
            "distincts": ["msrun__sample__animal__tracer_compound__name"],
            "filter": None,
        },
        {
            "displayname": "Studies",
            "distincts": ["msrun__sample__animal__studies__name"],
            "filter": None,
        },
        {
            "displayname": "Feeding Statuses",
            "distincts": ["msrun__sample__animal__feeding_status"],
            "filter": None,
        },
        {
            "displayname": "Infusion Rates",
            "distincts": ["msrun__sample__animal__tracer_infusion_rate"],
            "filter": None,
        },
        {
            "displayname": "Infusion Concentrations",
            "distincts": ["msrun__sample__animal__tracer_infusion_concentration"],
            "filter": None,
        },
    ]
    model_instances = {
        "PeakGroupSet": {
            "model": "PeakGroupSet",
            "path": "peak_group_set",
            "reverse_path": "peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Set Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "Peak Group Set Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "CompoundSynonym": {
            "model": "CompoundSynonym",
            "path": "compounds__synonyms",
            "reverse_path": "compound__peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound Synonym Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Any Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "PeakGroup": {
            "model": "PeakGroup",
            "path": "",
            "reverse_path": "",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Peak Group",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "formula": {
                    "displayname": "Formula",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "enrichment_fraction": {
                    "displayname": "Enrichment Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "enrichment_abundance": {
                    "displayname": "Enrichment Abundance",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "total_abundance": {
                    "displayname": "Total Abundance",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "normalized_labeling": {
                    "displayname": "Normalized Labeling",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "model": "Protocol",
            "path": "msrun__sample__animal__treatment",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
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
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Tissue": {
            "model": "Tissue",
            "path": "msrun__sample__tissue",
            "reverse_path": "samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Tissue Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Animal": {
            "model": "Animal",
            "path": "msrun__sample__animal",
            "reverse_path": "samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
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
                    "displayname": "Age (d-hh:mm:ss)",
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
                "tracer_labeled_atom": {
                    "displayname": "Tracer Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
                },
                "tracer_infusion_rate": {
                    "displayname": "Tracer Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "tracer_infusion_concentration": {
                    "displayname": "Tracer Infusion Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "TracerCompound": {
            "model": "Compound",
            "path": "msrun__sample__animal__tracer_compound",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": False,
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
        "MeasuredCompound": {
            "model": "Compound",
            "path": "compounds",
            "reverse_path": "peak_groups",
            "manytomany": {
                "is": True,
                "split_rows": False,
                "root_annot_fld": "compound",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Measured Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Measured Compound (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,  # Will display due to the handoff
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "msrun__sample__animal__studies",
            "reverse_path": "animals__samples__msruns__peak_groups",
            "manytomany": {
                "is": True,
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
