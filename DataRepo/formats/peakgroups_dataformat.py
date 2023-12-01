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
            "distincts": ["msrun_sample__sample__animal__name"],
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
            "distincts": ["msrun_sample__sample__name"],
            "filter": None,
        },
        {
            "displayname": "Tissues",
            "distincts": ["msrun_sample__sample__tissue__name"],
            "filter": None,
        },
        {
            "displayname": "Tracer Compounds",
            "distincts": ["msrun_sample__sample__animal__infusate__tracers__compound__name"],
            "filter": None,
        },
        {
            "displayname": "Studies",
            "distincts": ["msrun_sample__sample__animal__studies__name"],
            "filter": None,
        },
        {
            "displayname": "Feeding Statuses",
            "distincts": ["msrun_sample__sample__animal__feeding_status"],
            "filter": None,
        },
        {
            "displayname": "Infusion Rates",
            "distincts": ["msrun_sample__sample__animal__infusion_rate"],
            "filter": None,
        },
        {
            "displayname": "Tracer Concentrations",
            "distincts": [
                "msrun_sample__sample__animal__infusate__tracers__compound__name",
                "msrun_sample__sample__animal__infusate__tracer_links__concentration",
            ],
            "filter": None,
            "delimiter": ":",
        },
    ]
    model_instances = {
        "PeakAnnotationFile": {
            "model": "ArchiveFile",
            "path": "peak_annotation_file",
            "reverse_path": "peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Annotation File Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "Peak Annotation Filename",
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
                "total_abundance": {
                    "displayname": "Total Abundance",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "PeakGroupLabel": {
            "model": "PeakGroupLabel",
            "path": "labels",
            "reverse_path": "peak_group",
            "manyrelated": {
                "is": True,
                "manytomany": False,  # searching for peakGroups via PeakGroupLabel.pk can produce only 1 peak group
                "through": False,
                "split_rows": True,
                "root_annot_fld": "peak_group_label",  # Used to annotate root rec w/ subtable ID when split_rows=True
            },
            "fields": {
                "element": {
                    "displayname": "Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": ElementLabel.LABELED_ELEMENT_CHOICES,
                    "root_annot_fld": "element",  # Used to annotate root rec split_rows=True
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
            "path": "msrun_sample__sample__animal__treatment",
            "reverse_path": "animals__samples__msrun_samples__peak_groups",
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
            "path": "msrun_sample__sample",
            "reverse_path": "msrun_samples__peak_groups",
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
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
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
        "Tissue": {
            "model": "Tissue",
            "path": "msrun_sample__sample__tissue",
            "reverse_path": "samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
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
            "path": "msrun_sample__sample__animal",
            "reverse_path": "samples__msrun_samples__peak_groups",
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
                    "displayname": "Age",
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
        "Infusate": {
            "model": "InfusateTracer",
            "path": "msrun_sample__sample__animal__infusate",
            "reverse_path": "animals__samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": True,  # searching for PeakGroups via Infusate.pk can produce many peak groups
                "manytomany": False,  # but animal->infusate results in 1 infusate
                "through": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Infusate Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Infusate",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "InfusateTracer": {
            "model": "InfusateTracer",
            "path": "msrun_sample__sample__animal__infusate__tracer_links",
            "reverse_path": "infusate__animals__samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": True,
                "manytomany": True,  # searching for peakGroups via InfusateTracer.pk can produce many peak groups
                "through": True,
                "split_rows": False,
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
        "Tracer": {
            "model": "Tracer",
            "path": "msrun_sample__sample__animal__infusate__tracers",
            "reverse_path": "infusate__animals__samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": True,  # searching for PeakGroups via Tracer.pk can produce many peak groups
                "manytomany": True,  # and searching for tracers via PG.pk can also produce many tracers
                "through": False,
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
        "TracerCompound": {
            "model": "Compound",
            "path": "msrun_sample__sample__animal__infusate__tracers__compound",
            "reverse_path": "tracer__infusates__animals__samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": True,
                "manytomany": True,  # searching for peakGroups via compound.pk can produce many peak groups
                "through": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound (Tracer) Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Compound (Tracer) (Primary Synonym)",
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
            "manyrelated": {
                "is": True,
                "manytomany": True,
                "through": False,
                "split_rows": False,
                "root_annot_fld": "compound",
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound (Measured) Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Compound (Measured) (Primary Synonym)",
                    "searchable": True,
                    "displayed": True,  # Will display due to the handoff
                    "type": "string",
                },
            },
        },
        "CompoundSynonym": {
            "model": "CompoundSynonym",
            "path": "compounds__synonyms",
            "reverse_path": "compound__peak_groups",
            "manyrelated": {
                "is": True,
                "manytomany": True,
                "through": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound Synonym (Measured) Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Compound (Measured) (Any Synonym)",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "model": "Study",
            "path": "msrun_sample__sample__animal__studies",
            "reverse_path": "animals__samples__msrun_samples__peak_groups",
            "manyrelated": {
                "is": True,
                "manytomany": True,
                "through": False,
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
        "MSRunSequence": {
            "model": "MSRunSequence",
            "path": "msrun_sample__msrun_sequence",
            "reverse_path": "msrun_samples__peak_groups",
            "manyrelated": {
                "is": False,
                "manytomany": False,
                "through": False,
                "split_rows": False,
            },
            "fields": {
                # There is no single identifying field, so no ID field.  No handoff.  This means that a link from the
                # basic_search to MSRunSample cannot be serviced.  Date would be the most likely field to be unique as a
                # handoff, but date search is not yet supported in the advanced search interface.
                "researcher": {
                    "displayname": "Mass Spec Operator",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "instrument": {
                    "displayname": "Mass Spectrometer Name",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MSRunSample": {
            "model": "MSRunSample",
            "path": "msrun_sample",
            "reverse_path": "peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                # There is no single identifying field, so no ID field.  No handoff.  This means that a link from the
                # basic_search to MSRunSample cannot be serviced.
                "polarity": {
                    "displayname": "Polarity",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MZFile": {
            "model": "ArchiveFile",
            "path": "msrun_sample__ms_data_file",
            "reverse_path": "mz_to_msrunsamples__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) MZ File Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "MZ Data Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "RAWFile": {
            "model": "ArchiveFile",
            "path": "msrun_sample__ms_raw_file",
            "reverse_path": "raw_to_msrunsamples__peak_groups",
            "manyrelated": {
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) RAW File Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "filename",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "RAW Data Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
    }
