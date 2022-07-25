from DataRepo.formats.dataformat import Format
from DataRepo.models import Animal, ElementLabel, PeakGroup


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
            "distincts": ["peak_data__labels__element"],
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
            "distincts": ["msrun__sample__animal__infusate__tracers__compound__name"],
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
            "distincts": ["msrun__sample__animal__infusion_rate"],
            "filter": None,
        },
        {
            "displayname": "Tracer Concentrations",
            "distincts": [
                "msrun__sample__animal__infusate__tracers__compound__name",
                "msrun__sample__animal__infusate__infusatetracer__concentration",
            ],
            "filter": None,
            "delimiter": ":",
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
        "PeakDataLabel": {
            "model": "PeakDataLabel",
            "path": "peak_data__labels",
            "reverse_path": "peak_data__peak_group",
            "manytomany": {
                "is": True,
                "split_rows": True,
            },
            "distinct": True,  # Makes all fields below distinct - warning, displayed=False fields thwart this
            "fields": {
                "element": {
                    "displayname": "Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": ElementLabel.LABELED_ELEMENT_CHOICES,
                    # A datamember by this name will be available off the root record in the template
                    "root_annot_fld": "element",  # Used to annotate root record when dinstinct=True & split_rows=True
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
                # TODO: The following properties/cached_functions each return a dict, a type that is not (yet)
                #       supported.  The value is only used for the search interface. If these fields are changed to
                #       searchable (e.g. made into maintained fields), this will have to be dealt with.
                "enrichment_fractions": {
                    "displayname": "Enrichment Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "enrichment_abundances": {
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
                "normalized_labelings": {
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
                "infusion_rate": {
                    "displayname": "Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "TracerCompound": {
            "model": "Compound",
            "path": "msrun__sample__animal__infusate__tracers__compound",
            "reverse_path": "tracer__infusates__animals__samples__msruns__peak_groups",
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
