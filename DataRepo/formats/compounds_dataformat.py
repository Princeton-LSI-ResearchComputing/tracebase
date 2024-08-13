from DataRepo.formats.dataformat import Format
from DataRepo.models.compound import Compound


class CompoundsFormat(Format):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "cptemplate"
    name = "Compounds"
    rootmodel = Compound
    stats = [
        {
            "displayname": "Animals with these Tracers",
            "distincts": ["tracers__infusates__animals__id"],
            "filter": None,
        },
        {
            "displayname": "Tracers",
            "distincts": [
                "tracers__id"
            ],
            "filter": None,
        },
        {
            "displayname": "Studies with these Tracers",
            "distincts": ["tracers__infusates__animals__study__id"],
            "filter": None,
        },
    ]
    model_instances = {
        "Compound": {
            "model": "Compound",
            "path": "",
            "reverse_path": "",
            "manyrelated": {
                "is": False,
                "manytomany": False,
                "through": False,
                "split_rows": False,
            },
            "fields": {
                "id": {
                    "displayname": "(Internal) Compound Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Compound",
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
                "hmdb_id": {
                    "displayname": "HMDB ID",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "CompoundSynonym": {
            "model": "CompoundSynonym",
            "path": "synonyms",
            "reverse_path": "compound",
            "manyrelated": {
                "is": True,
                "manytomany": False,
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
        "Tracer": {
            "model": "Tracer",
            "path": "tracers",
            "reverse_path": "compound",
            "manyrelated": {
                "is": True,
                "manytomany": False,
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
        "Animal": {
            "model": "Animal",
            "path": "tracers__infusates__animals",
            "reverse_path": "infusate__tracers__compound",
            "manyrelated": {
                "is": True,
                "through": False,
                "manytomany": True,
                "split_rows": False,
                # TODO: If root_annot_fld is missing when manytomany is True and split_rows is False, you end up with more rows than is reported in the pagination, and with apparent duplicate rows, so I think that this should be caught and an error generated that explains what to do.
                "root_annot_fld": "animalwithtracers",  # Added to see if it tamps down the number of rows (which is too many, with repeated compounds, when there exist tracers with the compound)
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
            },
        },
    }
