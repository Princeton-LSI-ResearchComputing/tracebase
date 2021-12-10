from copy import deepcopy
from typing import Dict, List

from django.db.models import Model

from DataRepo.models import (
    Animal,
    PeakData,
    PeakGroup,
    Tissue,
    TracerLabeledClass,
)


class BaseSearchView:
    """
    This class holds common data/functions for search output formats.
    """

    name = ""
    models: Dict[str, Dict] = {}
    prefetches: List[str] = []
    rootmodel: Model = None
    ncmp_choices = {  # If any values are changed in any category, update valueMatches()
        "number": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("lt", "<"),
            ("lte", "<="),
            ("gt", ">"),
            ("gte", ">="),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
        "string": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("icontains", "contains"),
            ("not_icontains", "does not contain"),
            ("istartswith", "starts with"),
            ("not_istartswith", "does not start with"),
            ("iendswith", "ends with"),
            ("not_iendswith", "does not end with"),
            ("lt", "is alphabetically less than"),
            ("lte", "is alphabetically less than or equal to"),
            ("gt", "is alphabetically greater than"),
            ("gte", "is alphabetically greater than or equal to"),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
        "enumeration": [
            ("iexact", "is"),
            ("not_iexact", "is not"),
            ("not_isnull", "has a value (ie. is not None)"),
            ("isnull", "does not have a value (ie. is None)"),
        ],
    }
    static_filter = {  # Same as qry['tree']
        "type": "group",
        "val": "all",
        "static": False,
        "queryGroup": [
            {
                "type": "query",
                "pos": "",
                "static": False,
                "ncmp": "",
                "fld": "",
                "val": "",
            },
        ],
    }

    # static_filter example WITH static=True (below).  Note that a non-static empty query must be present in a non-
    # static queryGroup because that is where the user is prompted to add their search.  When static is True, the user
    # will not be allowed to edit that portion of the search form.
    #
    # static_filter = {
    #     "type": "group",
    #     "val": "all",
    #     "static": True,
    #     "queryGroup": [
    #         {
    #             'type': 'query',
    #             'pos': '',
    #             'ncmp': 'istartswith',
    #             'fld': 'msrun__sample__tissue__name',
    #             'val': Tissue.SERUM_TISSUE_PREFIX,
    #             'static': True,
    #         },
    #         {
    #             'type': 'group',
    #             'val': 'all',
    #             "queryGroup": [
    #                 {
    #                     'type': 'query',
    #                     'pos': '',
    #                     "static": False,
    #                     'ncmp': '',
    #                     'fld': '',
    #                     'val': '',
    #                 },
    #             ],
    #         },
    #     ]
    # }

    @classmethod
    def getSearchFieldChoices(self):
        """
        This generates the tuple to populate the select list choices for the AdvSearchForm fld field.
        """

        choices = ()
        for mkey in self.models.keys():
            mpath = self.models[mkey]["path"]
            for fkey in self.models[mkey]["fields"].keys():
                # We only want it in the select list if it is both searchable and displayed
                if (
                    self.models[mkey]["fields"][fkey]["searchable"] is True
                    and self.models[mkey]["fields"][fkey]["displayed"] is True
                ):
                    fpath = ""
                    if mpath != "":
                        fpath = mpath + "__"
                    fpath += fkey
                    fname = self.models[mkey]["fields"][fkey]["displayname"]
                    choices = choices + ((fpath, fname),)
        return tuple(sorted(choices, key=lambda x: x[1]))

    def getComparisonChoices(self):
        """
        Returns ncmp_choices (same for all derived classes)
        """
        return self.ncmp_choices

    def getAllComparisonChoices(self):
        """
        Returns the union of all ncmp_choices, ignoring differences in the second value. This is mainly only for form
        validation because it only validates known values (the first value in each tuple) regardless of the particular
        sub-population controlled by javascript in the advanced search form.
        """
        all_ncmp_choices = ()
        seen = []
        for fldtype in self.ncmp_choices.keys():
            for opt in self.ncmp_choices[fldtype]:
                if opt[0] not in seen:
                    seen.append(opt[0])
                    all_ncmp_choices = all_ncmp_choices + ((opt[0], opt[1]),)
        return all_ncmp_choices

    def getKeyPathList(self, mdl):
        """
        Returns a list of foreign key names for a composite view from the root table to the supplied table.
        """
        return self.models[mdl]["path"].split("__")

    def getPrefetches(self):
        """
        Returns a list of prefetch strings for a composite view from the root table to the supplied table.  It includes
        a unique set of "foreign key paths" that encompass all tables.
        """
        return self.prefetches

    def getModels(self):
        """
        Returns a list of all tables containing fields that are in an output format.  It does not include intermediate
        tables in key paths that do not have visibl;e fields in the composite view.
        """
        return list(self.models.keys())

    def getFieldTypes(self):
        """
        Returns a dict of path__field -> {type -> field_type (number, string, enumeration), choices -> list of tuples}.
        """

        typedict = {}
        # For each model
        for mdl in self.models.keys():
            # Grab the path
            path = self.models[mdl]["path"]
            # If the path has a value (i.e. it's not the root table), append the Q object separator
            if path != "":
                path += "__"
            # For each field
            for fld in self.models[mdl]["fields"].keys():
                # Create the field key (mimmicking the keys in the fld select list - but containing ALL fields)
                fldkey = path + fld
                typedict[fldkey] = {}
                # Save a dict with values for type and choices (if present)
                typedict[fldkey]["type"] = self.models[mdl]["fields"][fld]["type"]
                if "choices" in self.models[mdl]["fields"][fld].keys():
                    typedict[fldkey]["choices"] = self.models[mdl]["fields"][fld][
                        "choices"
                    ]
                else:
                    typedict[fldkey]["choices"] = []
        return typedict

    def getSearchFields(self, mdl):
        """
        Returns a dict of searchable fields for a given model/table whose keys are the field names and whose values are
        strings of the full foreign key path (delimited by dunderscores).
        """

        fielddict = {}
        path = self.models[mdl]["path"]
        if path != "":
            path += "__"
        for field in self.models[mdl]["fields"].keys():
            if self.models[mdl]["fields"][field]["searchable"] is True:
                fielddict[field] = path + field
        return fielddict

    def getDisplayFields(self, mdl):
        """
        Returns a dict of displayed fields for a given model/table whose keys are the field names and whose values are
        searchable field names in the same model/table that should be displayed in their stead.  The values of the
        fields in the dict values returned must have the same relation requirements as the field in the dict key.  E.g.
        If a non-displayed dict key field is unique, the displayed field dict value must also be unique.  I.e. A search
        using the dict key field and the dict value field must return the same records.
        """

        fielddict = {}
        for field in self.models[mdl]["fields"].keys():
            if self.models[mdl]["fields"][field]["displayed"] is False:
                if "handoff" in self.models[mdl]["fields"][field].keys():
                    fielddict[field] = self.models[mdl]["fields"][field]["handoff"]
                # Nothing if a handoff is not present
            else:
                fielddict[field] = field
        return fielddict

    def getRootQuerySet(self):
        if self.rootmodel is not None:
            return self.rootmodel.objects.all()
        print("ERROR: rootmodel not set.")
        return None

    def getMMKeyPaths(self):
        """
        Gathers and returns all the model key paths that are in a many-to-many relationship
        """
        paths = []
        for mdl in self.models.keys():
            if self.models[mdl]["manytomany"]:
                paths.append(self.models[mdl]["path"])
        return paths

    # Don't send the root qry. Send this: qry["searches"][fmt]["tree"]
    def shouldReFilter(self, qrytree):
        """
        Provided a root qry object and the selected format, this determines whether an advanced search includes a
        search term on a field in a table that has a many-to-many relationship with the root model.
        """
        mm_keypaths = self.getMMKeyPaths()
        if len(mm_keypaths) > 0:
            fld_list = self.getFldValues(qrytree)
            for mm_keypath in mm_keypaths:
                for fld in fld_list:
                    if mm_keypath in fld:
                        return True
        return False

    # Don't send the root qry. Send this qry["searches"][fmt]["tree"]
    def getFldValues(self, query):
        """
        This takes a qry object's tree (for a particular format's searches) and returns a list of all the fld values
        involved in the search
        """
        if query["type"] == "query":
            return [query["fld"]]
        else:
            fld_list = []
            for subquery in query["queryGroup"]:
                fld_list += self.getFldValues(subquery)
            return fld_list

    def isAMatch(self, rootrec, mm_lookup, query):
        """
        Given a pair of records from tables in a many-to-many relationship, the key path of the M:M model, and a qry
        object's tree of the searched format, this method returns false if the row should be omitted from the table.
        It basically re-applies the search defined in qry, because when there's a search term from a M:M table,
        django's join method provides no means to filter out records that do not match those search terms.  It simple
        left-joins everything related to the root table records.
        """
        if query["type"] == "query":
            recval = self.getValue(rootrec, mm_lookup, query["fld"])
            return self.valueMatches(recval, query["ncmp"], query["val"])
        else:
            if query["val"] == "all":
                for subquery in query["queryGroup"]:
                    if not self.isAMatch(rootrec, mm_lookup, subquery):
                        return False
                return True
            else:
                for subquery in query["queryGroup"]:
                    if self.isAMatch(rootrec, mm_lookup, subquery):
                        return True
                return False

    def getValue(self, rootrec, mm_lookup, qry_keypath):
        """
        Given a pair of records from tables in a many-to-many relationship, the key path of the M:M model, and the
        keypath of a single fld value in the qry search tree, the value of the fld in the record in which it resides
        (whether it's in the M:M table record or the root table record).  The qry_keypath is assumed to end in a field
        name, which is used to obtain the value.
        """
        ptr, qry_keypath_list = self.getMMKeyPathList(qry_keypath, mm_lookup, rootrec)
        for key in qry_keypath_list:
            ptr = getattr(ptr, key)
        return ptr

    def getMMKeyPathList(self, qry_keypath, mm_lookup, rootrec):
        """
        This takes a single keypath from the qry object, the M:M lookup dict that holds individual records keyed by
        their model's keypath in string format, and a root model record, and returns the record (either the root record
        or a M:M record) along with its key path (in list format) that can be used to obtain the field value from that
        record.
        """
        qry_keypath_list = self.keypathStringToList(qry_keypath)
        rec = rootrec
        for mm_keypath in mm_lookup.keys():
            if mm_keypath in qry_keypath:
                mm_keypath_list = self.keypathStringToList(mm_keypath)
                # shift the mm_keypath off the qry_keypath
                start = len(mm_keypath_list)
                tmp_qry_keypath_list = qry_keypath_list[start:]
                if len(tmp_qry_keypath_list) == 1:
                    qry_keypath_list = tmp_qry_keypath_list
                    rec = mm_lookup[mm_keypath]
        return rec, qry_keypath_list

    def keypathStringToList(self, keypath_str):
        return keypath_str.split("__")

    def valueMatches(self, recval, condition, searchterm):
        """
        Determines whether the recval and search term match, given the matching condition.
        This is only useful for re-filtering records in a template when the qry includes a fld from a many-to-many
        related model relative to the root model.
        Note that any changes to ncmp_choices must also be implemented here.
        """
        if condition == "iexact":
            return recval.lower() == searchterm.lower()
        elif condition == "not_iexact":
            return recval.lower() != searchterm.lower()
        elif condition == "lt":
            return recval < searchterm
        elif condition == "lte":
            return recval <= searchterm
        elif condition == "gt":
            return recval > searchterm
        elif condition == "gte":
            return recval >= searchterm
        elif condition == "isnull":
            return recval is None
        elif condition == "not_isnull":
            return recval is not None
        elif condition == "icontains":
            return searchterm.lower() in recval.lower()
        elif condition == "not_icontains":
            return searchterm.lower() not in recval.lower()
        elif condition == "istartswith":
            return recval.lower().startswith(searchterm.lower())
        elif condition == "not_istartswith":
            return not recval.lower().startswith(searchterm.lower())
        elif condition == "iendswith":
            return recval.lower().endswith(searchterm.lower())
        elif condition == "not_iendswith":
            return not recval.lower().endswith(searchterm.lower())
        elif condition == "exact":
            # For search_basic
            return recval == searchterm
        else:
            raise UnknownComparison(
                f"Unrecognized negatable comparison (ncmp) value: {condition}."
            )


class PeakGroupsSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pgtemplate"
    name = "PeakGroups"
    rootmodel = PeakGroup
    prefetches = [
        "peak_group_set",
        "compounds__synonyms",
        "msrun__sample__tissue",
        "msrun__sample__animal__treatment",
        "msrun__sample__animal__tracer_compound",
        "msrun__sample__animal__studies",
    ]
    models = {
        "PeakGroupSet": {
            "path": "peak_group_set",
            "manytomany": False,
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
            "path": "compounds__synonyms",
            "manytomany": True,
            "fields": {
                "name": {
                    "displayname": "Measured Compound",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "PeakGroup": {
            "path": "",
            "manytomany": False,
            "fields": {
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
            "path": "msrun__sample__animal__treatment",
            "manytomany": False,
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
            "path": "msrun__sample",
            "manytomany": False,
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
            "path": "msrun__sample__tissue",
            "manytomany": False,
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
            "path": "msrun__sample__animal",
            "manytomany": False,
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
        "Compound": {
            "path": "msrun__sample__animal__tracer_compound",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Tracer Compound",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "path": "msrun__sample__animal__studies",
            "manytomany": True,
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


class PeakDataSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pdtemplate"
    name = "PeakData"
    rootmodel = PeakData
    prefetches = [
        "peak_group__peak_group_set",
        "peak_group__msrun__sample__tissue",
        "peak_group__msrun__sample__animal__tracer_compound",
        "peak_group__msrun__sample__animal__treatment",
        "peak_group__msrun__sample__animal__studies",
    ]
    models = {
        "PeakData": {
            "path": "",
            "manytomany": False,
            "fields": {
                "labeled_element": {
                    "displayname": "Labeled Element",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": TracerLabeledClass.TRACER_LABELED_ELEMENT_CHOICES,
                },
                "labeled_count": {
                    "displayname": "Labeled Count",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "raw_abundance": {
                    "displayname": "Raw Abundance",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "corrected_abundance": {
                    "displayname": "Corrected Abundance",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "fraction": {
                    "displayname": "Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                    "type": "number",
                },
                "med_mz": {
                    "displayname": "Median M/Z",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "med_rt": {
                    "displayname": "Median RT",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "PeakGroup": {
            "path": "peak_group",
            "manytomany": False,
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
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
            },
        },
        "PeakGroupSet": {
            "path": "peak_group__peak_group_set",
            "manytomany": False,
            "fields": {
                "id": {
                    "displayname": "(Internal) Peak Group Set Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
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
        "Sample": {
            "path": "peak_group__msrun__sample",
            "manytomany": False,
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "type": "number",
                    "handoff": "name",
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
            "path": "peak_group__msrun__sample__tissue",
            "manytomany": False,
            "fields": {
                "id": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": False,
                    "type": "number",
                    "handoff": "name",
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
            "path": "peak_group__msrun__sample__animal",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Animal Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
                "body_weight": {
                    "displayname": "Body Weight (g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "genotype": {
                    "displayname": "Genotype",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "sex": {
                    "displayname": "Sex",
                    "searchable": True,
                    "displayed": True,
                    "type": "enumeration",
                    "choices": Animal.SEX_CHOICES,
                },
                "age": {
                    "displayname": "Age (d-hh:mm:ss)",
                    "searchable": False,
                    "displayed": True,
                    "type": "number",
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "diet": {
                    "displayname": "Diet",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "tracer_infusion_concentration": {
                    "displayname": "Tracer Infusion Concentration (mM)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
                "tracer_infusion_rate": {
                    "displayname": "Tracer Infusion Rate (ul/min/g)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Protocol": {
            "path": "peak_group__msrun__sample__animal__treatment",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
            },
        },
        "Compound": {
            "path": "peak_group__msrun__sample__animal__tracer_compound",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Tracer Compound",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
            },
        },
        "Study": {
            "path": "peak_group__msrun__sample__animal__studies",
            "manytomany": True,
            "fields": {
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
            },
        },
    }


class FluxCircSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "fctemplate"
    name = "Fcirc"
    rootmodel = PeakGroup
    prefetches = [
        "msrun__sample__animal__tracer_compound",
        "msrun__sample__animal__treatment",
        "msrun__sample__animal__studies",
    ]
    models = {
        "PeakGroup": {
            "path": "",
            "manytomany": False,
            "fields": {
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
            "path": "msrun__sample__animal",
            "manytomany": False,
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
        "Protocol": {
            "path": "msrun__sample__animal__treatment",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Treatment",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Protocol Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
            },
        },
        "Sample": {
            "path": "msrun__sample",
            "manytomany": False,
            "fields": {
                "time_collected": {
                    "displayname": "Time Collected (hh:mm:ss since infusion)",
                    "searchable": True,
                    "displayed": True,
                    "type": "number",
                },
            },
        },
        "Compound": {
            "path": "msrun__sample__animal__tracer_compound",
            "manytomany": False,
            "fields": {
                "name": {
                    "displayname": "Tracer Compound",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Tracer Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
                },
            },
        },
        "Study": {
            "path": "msrun__sample__animal__studies",
            "manytomany": True,
            "fields": {
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
                    "type": "string",
                },
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                    "type": "number",
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


class BaseAdvancedSearchView:
    """
    This class groups all search output formats in a single class and adds metadata that applies to all
    search output formats as a whole.  It includes all derived classes of BasSearchView.  BaseSearchView
    differs from this class in that BaseSearchView has an instance for each search output format class and
    this class has a single instance for all search view output format classes.
    """

    modes = ["search", "browse"]
    default_mode = "search"
    default_format = ""
    modeldata: Dict[int, BaseSearchView] = {}

    def __init__(self):
        """
        This is a constructor that adds all search output format classes to modeldata, keyed on their IDs.
        """

        for cls in (PeakGroupsSearchView(), PeakDataSearchView(), FluxCircSearchView()):
            self.modeldata[cls.id] = cls
        self.default_format = PeakGroupsSearchView.id

    def getRootGroup(self, selfmt=None):
        """
        This method builds a fresh qry object (aka "rootGroup"). This is the object that defines an advanced search and
        is modified via javascript as the user composes a search.  It is passed back and forth via json.  The starting
        structure is:
            rootGroup = {
              selectedtemplate: 'pgtemplate',
              searches: {
                pgtemplate: {
                  name: 'PeakGroups',
                  tree: {
                    type: 'group',
                    val: 'all',
                    static: false,
                    queryGroup: [...]
                  }
                },
                ...
              }
            }
        The value of the tree item is defined by the static_filter in each derived class and that is what must contain
        an empty query.  At least 1 empty query is required in every item of type group, because the starting search
        form must contain a spot for the user to enter their search.
        """
        if selfmt is None:
            selfmt = self.default_format
        if selfmt not in self.modeldata.keys():
            print(
                f"WARNING: Unknown format: [{selfmt}]. Falling back to default format: [{self.default_format}]"
            )
            selfmt = self.default_format
        rootGroup = {
            "selectedtemplate": selfmt,
            "searches": {},
        }
        for format in self.modeldata.keys():
            rootGroup["searches"][format] = {}
            rootGroup["searches"][format]["name"] = self.modeldata[format].name
            if self.staticFilterIsValid(self.modeldata[format].static_filter):
                rootGroup["searches"][format]["tree"] = deepcopy(
                    self.modeldata[format].static_filter
                )
            else:
                print(
                    f"ERROR: No empty queries in format {format}: ",
                    self.modeldata[format].static_filter,
                )
                raise Exception(
                    f"Static filter for format {format} must contain at least 1 non-static empty query."
                )
        return rootGroup

    def staticFilterIsValid(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and raises an exception for missing keys or
        invalid values in the root query and calls staticFilterIsValidHelper to recursively validate the treee.
        """

        if (
            "type" not in filter
            or filter["type"] != "group"
            or "queryGroup" not in filter
            or len(filter["queryGroup"]) == 0
        ):
            raise Exception(
                "Invalid root query group.  Must be of type 'group' and contain a populated queryGroup array."
            )
        else:
            num_nonstatic = self.getNumNonStaticGroups(filter)
            if num_nonstatic == 0:
                raise Exception(
                    "Invalid root query group.  There must exist at least 1 non-static query group."
                )
            return self.staticFilterIsValidHelper(filter)

    def staticFilterIsValidHelper(self, filter):
        """
        Raises an exception for missing keys or invalid values and returns true if at least 1 empty query exists among
        all recursively checked objects of type query.
        """
        # Validate the keys present in both query and group types
        if "type" not in filter or "val" not in filter or "static" not in filter:
            raise Exception(
                "Static filter is missing 1 or more required keys: [type, val, static]."
            )
        elif filter["type"] == "query":
            # Validate the keys of the query
            if "ncmp" not in filter or "pos" not in filter or "fld" not in filter:
                raise Exception(
                    "Missing keys in query.  At least 1 of the following keys is missing: [fld, ncmp, pos]."
                )
            # If empty (i.e. val holds an empty string), return true
            if not filter["static"] and filter["val"] == "":
                return True
            return False
        elif filter["type"] == "group":
            # Validate the keys & values of the group
            if (
                "queryGroup" not in filter
                or len(filter["queryGroup"]) == 0
                or (filter["val"] != "all" and filter["val"] != "any")
            ):
                raise Exception(
                    "Invalid group.  Must contain a queryGroup key with a populated array and val must be either "
                    "'all' or 'any'."
                )
            empty_exists = False
            for query in filter["queryGroup"]:
                if self.staticFilterIsValidHelper(query):
                    empty_exists = True
            return empty_exists
        else:
            raise Exception(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def getNumEmptyQueries(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and recursively counts the number of empty
        queries.
        """
        if filter["type"] == "query":
            # If empty (i.e. val holds an empty string), return 1
            if not filter["static"] and filter["val"] == "":
                return 1
            return 0
        elif filter["type"] == "group":
            total_empty = 0
            for query in filter["queryGroup"]:
                total_empty += self.getNumEmptyQueries(query)
            return total_empty
        else:
            raise Exception(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def getNumNonStaticGroups(self, filter):
        """
        Takes a "tree" value of 1 format from the rootGroup query object and recursively counts the number of nonstatic
        groups.
        """
        if filter["type"] == "query":
            return 0
        elif filter["type"] == "group":
            total_nonstatic = 0
            if not filter["static"]:
                total_nonstatic = 1
            for query in filter["queryGroup"]:
                total_nonstatic += self.getNumNonStaticGroups(query)
            return total_nonstatic
        else:
            raise Exception(
                f"Invalid query type {filter['type']}.  Must be either 'query' or 'group'."
            )

    def getRootQuerySet(self, format):
        """
        Calls getRootQuerySet of the supplied format.
        """
        return self.modeldata[format].getRootQuerySet()

    def getPrefetches(self, format):
        """
        Calls getPrefetches of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getPrefetches()

    def getSearchFieldChoices(self, format):
        """
        Calls getSearchFieldChoices of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getSearchFieldChoices()

    def getComparisonChoices(self):
        """
        Calls getComparisonChoices of the default output format class.
        """
        return self.modeldata[self.default_format].getComparisonChoices()

    def getAllComparisonChoices(self):
        """
        Calls getAllComparisonChoices of the default output format class.
        """
        return self.modeldata[self.default_format].getAllComparisonChoices()

    def getModels(self, format):
        """
        Calls getModels of the supplied ID of the search output format class.
        """
        return self.modeldata[format].getModels()

    def getFormatNames(self):
        """
        Returns a dict of search output format class IDs to their user-facing names.
        """

        namedict = {}
        for fmtid in self.modeldata.keys():
            namedict[fmtid] = str(self.modeldata[fmtid].name)
        return namedict

    def getFieldTypes(self):
        """
        Returns a dict of fmt -> path__field -> {type -> field_type (number, string, enumeration), choices -> list of
        tuples}.
        """

        typedict = {}
        for fmtid in self.modeldata.keys():
            typedict[fmtid] = self.modeldata[fmtid].getFieldTypes()
        return typedict

    def getSearchFields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of searchable field name -> field key path
        """
        return self.modeldata[fmt].getSearchFields(mdl)

    def getDisplayFields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of field name -> display field name (if there exists a handoff
        from a non-displayed field to a displayed one)
        """
        return self.modeldata[fmt].getDisplayFields(mdl)

    def getKeyPathList(self, fmt, mdl):
        """
        Calls getKeyPathList of the supplied ID of the search output format class.
        """
        return self.modeldata[fmt].getKeyPathList(mdl)

    def formatNameOrKeyToKey(self, fmtsubmitted):
        """
        Takes a search output format ID or name and returns the corresponding search output format
        ID.  This method exists to facilitate the usage of (case-insensitive) format names in search_basic URLs.
        """

        fmtkey = fmtsubmitted
        names = self.getFormatNames()
        foundit = False
        if fmtsubmitted in names:
            foundit = True
        else:
            for fmtid, fmtnm in names.items():
                if fmtnm.lower() == fmtsubmitted.lower():
                    fmtkey = fmtid
                    foundit = True
                    break
        if foundit is False:
            return None
        return fmtkey

    def getMMKeyPaths(self, fmt):
        """
        Gathers and returns all the model key paths that are in a many-to-many relationship in the provided format
        """
        return self.modeldata[fmt].getMMKeyPaths()

    def shouldReFilter(self, qry):
        """
        Provided a root qry object and the selected format, this determines whether an advanced search includes a
        search term on a field in a table that has a many-to-many relationship with the root model.
        """
        if "selectedtemplate" not in qry:
            print("ERROR: Invalid qry object: ", qry)
            raise InvalidQryObject("selectedtemplate key is missing")
        fmt = qry["selectedtemplate"]
        return self.modeldata[fmt].shouldReFilter(qry["searches"][fmt]["tree"])

    def isAMatch(self, rootrec, mm_lookup, query):
        """
        Given a pair of records from tables in a many-to-many relationship, the key path of the M:M model, and a qry
        object, this method returns false if the row should be omitted from the table.
        It basically re-applies the search defined in qry, because when there's a search term from a M:M table,
        django's join method provides no means to filter out records that do not match those search terms.  It simple
        left-joins everything related to the root table records.
        """
        fmt = query["selectedtemplate"]
        return self.modeldata[fmt].isAMatch(
            rootrec, mm_lookup, query["searches"][fmt]["tree"]
        )


class UnknownComparison(Exception):
    pass


class InvalidQryObject(Exception):
    pass
