from DataRepo.formats.dataformat import Format
from DataRepo.models import Animal, ArchiveFile


class MzxmlFormat(Format):
    """This class encapsulates all the metadata of a single search output format for mzXML files."""

    id = "mztemplate"
    name = "mzXML"
    rootmodel = ArchiveFile
    stats = [
        {
            "displayname": "Animals",
            "distincts": ["mz_to_msrunsamples__sample__animal__name"],
            "filter": None,
        },
        {
            "displayname": "Samples",
            "distincts": ["mz_to_msrunsamples__sample__name"],
            "filter": None,
        },
        {
            "displayname": "Tissues",
            "distincts": ["mz_to_msrunsamples__sample__tissue__name"],
            "filter": None,
        },
        {
            "displayname": "Studies",
            "distincts": ["mz_to_msrunsamples__sample__animal__studies__name"],
            "filter": None,
        },
        {
            "displayname": "Feeding Statuses",
            "distincts": ["mz_to_msrunsamples__sample__animal__feeding_status"],
            "filter": None,
        },
        {
            "displayname": "Infusion Rates",
            "distincts": ["mz_to_msrunsamples__sample__animal__infusion_rate"],
            "filter": None,
        },
        {
            "displayname": "Polarities",
            "distincts": ["mz_to_msrunsamples__polarity"],
            "filter": None,
        },
    ]
    model_instances = {
        "mzXMLFile": {
            "model": "ArchiveFile",
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
                    "displayname": "(Internal) mzXML File Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "checksum",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "mzXML Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "checksum": {
                    "displayname": "mzXML Checksum",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "imported_timestamp": {
                    "displayname": "Imported Timestamp",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MSRunSample": {
            "model": "MSRunSample",
            "path": "mz_to_msrunsamples",
            "reverse_path": "ms_data_file",
            "manyrelated": {
                # This is technically many-related, but the unique constraint in MSRunSample makes it 1:1
                "is": False,
                "through": False,
                "manytomany": False,
                "split_rows": False,
            },
            "fields": {
                # There is no single identifying field, so no ID field.  No handoff.  This means that a link from the
                # basic_search to MSRunSample cannot be serviced.  Date would be the most likely field to be unique as a
                # handoff, but date search is not yet supported in the advanced search interface.
                "polarity": {
                    "displayname": "Polarity",
                    "searchable": False,
                    "displayed": True,
                    "type": "string",
                },
                "mz_min": {
                    "displayname": "MZ Min",
                    "searchable": False,
                    "displayed": True,
                    "type": "number",
                },
                "mz_max": {
                    "displayname": "MZ Max",
                    "searchable": False,
                    "displayed": True,
                    "type": "number",
                },
                "mzxml_export_path": {
                    "displayname": "mzXML File",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Sample": {
            "model": "Sample",
            "path": "mz_to_msrunsamples__sample",
            "reverse_path": "msrun_samples__ms_data_file",
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
                "date": {
                    "displayname": "Date Collected",
                    "searchable": True,  # NOTE: Date search is not fully supported in advanced search yet
                    "displayed": True,
                    "type": "string",
                },
                "time_collected": {
                    "displayname": "Time Collected (m)",
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
                "researcher": {
                    "displayname": "Handler",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Tissue": {
            "model": "Tissue",
            "path": "mz_to_msrunsamples__sample__tissue",
            "reverse_path": "samples__msrun_samples__ms_data_file",
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
            "path": "mz_to_msrunsamples__sample__animal",
            "reverse_path": "samples__msrun_samples__ms_data_file",
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
                "age": {
                    "displayname": "Age (w)",
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
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "body_weight": {
                    "displayname": "Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
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
        "Treatment": {
            "model": "Protocol",
            "path": "mz_to_msrunsamples__sample__animal__treatment",
            "reverse_path": "animals__samples__msrun_samples__ms_data_file",
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
        "Infusate": {
            "model": "Infusate",
            "path": "mz_to_msrunsamples__sample__animal__infusate",
            "reverse_path": "animals__samples__msrun_samples__ms_data_file",
            "manyrelated": {
                "is": False,
                "manytomany": False,  # but mzXML->infusate results in 1 infusate
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
                    # NOTE: "name" is a maintained field. All maintained fields must be built for search to produce all
                    # relevant results, but this is an advantage over cached_functions, which cannot be searched.
                    "displayname": "Infusate",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "MSRunSequence": {
            "model": "MSRunSequence",
            "path": "mz_to_msrunsamples__msrun_sequence",
            "reverse_path": "msrun_samples__ms_data_file",
            "manyrelated": {
                "is": False,
                "manytomany": False,
                "through": False,
                "split_rows": False,
            },
            "fields": {
                # There is no single identifying field, so no ID field.  No handoff.  This means that a link from the
                # basic_search to MSRunSample cannot be serviced.
                "researcher": {
                    "displayname": "Operator",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "instrument": {
                    "displayname": "Instrument",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "date": {
                    "displayname": "Run Date",
                    "searchable": True,  # NOTE: Date search is not fully supported in advanced search yet
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "LCProtocol": {
            "model": "LCMethod",
            "path": "mz_to_msrunsamples__msrun_sequence__lc_method",
            "reverse_path": "msrun_sequences__msrun_samples__ms_data_file",
            "manyrelated": {
                "is": False,
                "manytomany": False,
                "through": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) LC Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "LC Protocol",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "RAWFile": {
            "model": "ArchiveFile",
            "path": "mz_to_msrunsamples__ms_raw_file",
            "reverse_path": "raw_to_msrunsamples__ms_data_file",
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
                    "handoff": "checksum",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "filename": {
                    "displayname": "RAW Filename",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "checksum": {
                    "displayname": "RAW Checksum",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Studies": {
            "model": "Study",
            "path": "mz_to_msrunsamples__sample__animal__studies",
            "reverse_path": "animals__samples__msrun_samples__ms_data_file",
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
    }

    def getRootQuerySet(self):
        """Ensure we only get mzXML records."""
        return ArchiveFile.objects.filter(
            data_type__code__exact="ms_data",
            data_format__code__exact="mzxml",
        )
