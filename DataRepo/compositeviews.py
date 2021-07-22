from typing import Dict, List


class BaseSearchView:
    """
    This class holds common data/functions for search output formats.
    """

    name = ""
    models: Dict[str, Dict] = {}
    prefetches: List[str] = []

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
        return choices

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
                fielddict[field] = self.models[mdl]["fields"][field]["handoff"]
            else:
                fielddict[field] = field
        return fielddict


class PeakGroupsSearchView(BaseSearchView):
    """
    This class encapsulates all the metadata of a single search output format, which includes multiple tables/fields.
    """

    id = "pgtemplate"
    name = "PeakGroups"
    prefetches = [
        "msrun__sample__tissue",
        "msrun__sample__animal__tracer_compound",
        "msrun__sample__animal__studies",
    ]
    models = {
        "PeakGroup": {
            "path": "",
            "fields": {
                "name": {
                    "displayname": "Output Compound",
                    "searchable": True,
                    "displayed": True,
                },
                "enrichment_fraction": {
                    "displayname": "Enrichment Fraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                },
                "total_abundance": {
                    "displayname": "TIC",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                },
                "normalized_labeling": {
                    "displayname": "NormFraction",
                    "searchable": False,  # Cannot search cached property
                    "displayed": True,
                },
            },
        },
        "Sample": {
            "path": "msrun__sample",
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Tissue": {
            "path": "msrun__sample__tissue",
            "fields": {
                "name": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Animal": {
            "path": "msrun__sample__animal",
            "fields": {
                "tracer_labeled_atom": {
                    "displayname": "Atom",
                    "searchable": True,
                    "displayed": True,
                },
                "id": {
                    "displayname": "(Internal) Animal Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                },
                "feeding_status": {
                    "displayname": "Feeding Status",
                    "searchable": True,
                    "displayed": True,
                },
                "tracer_infusion_rate": {
                    "displayname": "Infusion Rate",
                    "searchable": True,
                    "displayed": True,
                },
                "tracer_infusion_concentration": {
                    "displayname": "[Infusion]",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Compound": {
            "path": "msrun__sample__animal__tracer_compound",
            "fields": {
                "name": {
                    "displayname": "Input Compound",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Study": {
            "path": "msrun__sample__animal__studies",
            "fields": {
                "id": {
                    "displayname": "(Internal) Study Index",
                    "searchable": True,
                    "displayed": False,  # Used in link
                    "handoff": "name",  # This is the field that will be loaded in the search form
                },
                "name": {
                    "displayname": "Study",
                    "searchable": True,
                    "displayed": True,
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
    prefetches = [
        "peak_group__msrun__sample__tissue",
        "peak_group__msrun__sample__animal__tracer_compound",
    ]
    models = {
        "PeakData": {
            "path": "",
            "fields": {
                "labeled_element": {
                    "displayname": "Atom",
                    "searchable": True,
                    "displayed": True,
                },
                "corrected_abundance": {
                    "displayname": "Corrected Abundance",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "PeakGroup": {
            "path": "peak_group",
            "fields": {
                "id": {
                    "displayname": "(Internal) PeakGroup Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "handoff": "name",  # This is the field that will be loaded in the search form
                },
                "name": {
                    "displayname": "Output Compound",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Sample": {
            "path": "peak_group__msrun__sample",
            "fields": {
                "id": {
                    "displayname": "(Internal) Sample Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                },
                "name": {
                    "displayname": "Sample",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Tissue": {
            "path": "peak_group__msrun__sample__tissue",
            "fields": {
                "name": {
                    "displayname": "Tissue",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Animal": {
            "path": "peak_group__msrun__sample__animal",
            "fields": {
                "id": {
                    "displayname": "(Internal) Animal Index",  # Used in link
                    "searchable": True,
                    "displayed": False,
                    "handoff": "name",  # This is the field that will be loaded in the search form
                },
                "name": {
                    "displayname": "Animal",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
        "Compound": {
            "path": "peak_group__msrun__sample__animal__tracer_compound",
            "fields": {
                "name": {
                    "displayname": "Input Compound",
                    "searchable": True,
                    "displayed": True,
                },
            },
        },
    }


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

        for cls in (PeakGroupsSearchView(), PeakDataSearchView()):
            self.modeldata[cls.id] = cls
        self.default_format = PeakGroupsSearchView.id

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
