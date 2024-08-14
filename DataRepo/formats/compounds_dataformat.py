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
            "distincts": ["tracers__infusates__animals__name"],
            "filter": None,
        },
        {
            "displayname": "Tracers",
            "distincts": ["tracers__name"],
            "filter": None,
        },
        {
            "displayname": "Studies with these Tracers",
            "distincts": ["tracers__infusates__animals__studies__name"],
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
                    "displayname": "Compound (Primary Name)",
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
                    "displayname": "(Internal) Compound Synonym Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "name": {
                    "displayname": "Compound (Any Synonym)",
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
                # Adds a field named "tracer" to the root rec as an annotation with the value of the tracer record's
                # primary key.  When there are multiple tracers for this compound, the root compound record is
                # essentially duplicated, and each duplicate gets a different tracer associated with it, like a real SQL
                # left join.  In the template, you use the template tag "get_many_related" tag and supply it the
                # queryset of tracers and the tracer ID by supplying this field, e.g.
                # {% get_many_related rec.tracers.all rec.tracer %}
                "root_annot_fld": "tracer",
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
        "Infusate": {
            "model": "Infusate",
            "path": "tracers__infusates",
            "reverse_path": "tracers__compound",
            "manyrelated": {
                "is": True,
                "manytomany": True,
                "through": False,
                "split_rows": False,
                "root_annot_fld": "infusate",
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
        "Animal": {
            "model": "Animal",
            "path": "tracers__infusates__animals",
            "reverse_path": "infusate__tracers__compound",
            "manyrelated": {
                "is": True,
                "through": False,
                "manytomany": True,
                "split_rows": False,
                # TODO: If root_annot_fld is missing when manytomany is True and split_rows is False, you end up with
                # more rows than is reported in the pagination, and with apparent duplicate rows, so I think that this
                # should be caught and an error generated that explains what to do.
                "root_annot_fld": "animalwithtracers",
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
