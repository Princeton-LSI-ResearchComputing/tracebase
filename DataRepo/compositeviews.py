from typing import Dict, List


class BaseSearchView:
    name = ""
    models: Dict[str, Dict] = {}
    prefetches: List[str] = []

    @classmethod
    def getSearchFieldChoices(self):
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
        return self.models[mdl]["path"].split("__")

    def getPrefetches(self):
        return self.prefetches

    def getModels(self):
        return self.models.keys()

    def getSearchFields(self, mdl):
        fielddict = {}
        path = self.models[mdl]["path"]
        if path != "":
            path += "__"
        for field in self.models[mdl]["fields"].keys():
            if self.models[mdl]["fields"][field]["searchable"] is True:
                fielddict[field] = path + field
        return fielddict

    def getDisplayFields(self, mdl):
        fielddict = {}
        for field in self.models[mdl]["fields"].keys():
            if self.models[mdl]["fields"][field]["displayed"] is False:
                fielddict[field] = self.models[mdl]["fields"][field]["handoff"]
            else:
                fielddict[field] = field
        return fielddict


class PeakGroupsSearchView(BaseSearchView):
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
                    "handoff": "name",  # This is the field that will be loaded in the advanced search form
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
                    "handoff": "name",  # This is the field that will be loaded in the advanced search form
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
                    "handoff": "name",  # This is the field that will be loaded in the advanced search form
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
                    "handoff": "name",  # This is the field that will be loaded in the advanced search form
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
                    "handoff": "name",  # This is the field that will be loaded in the advanced search form
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
    modes = ["search", "browse"]
    default_mode = "search"
    default_format = ""
    modeldata: Dict[int, BaseSearchView] = {}

    def __init__(self):
        for cls in (PeakGroupsSearchView(), PeakDataSearchView()):
            self.modeldata[cls.id] = cls
        self.default_format = PeakGroupsSearchView.id

    def getPrefetches(self, format):
        return self.modeldata[format].getPrefetches()

    def getSearchFieldChoices(self, format):
        return self.modeldata[format].getSearchFieldChoices()

    def getModels(self, format):
        return self.modeldata[format].getModels()

    def getFormatNames(self):
        namedict = {}
        for fmtid in self.modeldata.keys():
            namedict[fmtid] = str(self.modeldata[fmtid].name)
        return namedict

    def getSearchFields(self, fmt, mdl):
        """Takes a format key and model and returns a dict of searchable field name -> field key path"""
        return self.modeldata[fmt].getSearchFields(mdl)

    def getDisplayFields(self, fmt, mdl):
        """
        Takes a format key and model and returns a dict of field name -> display field name (if there exists a handoff
        from a non-displayed field to a displayed one)
        """
        return self.modeldata[fmt].getDisplayFields(mdl)

    def getKeyPathList(self, fmt, mdl):
        return self.modeldata[fmt].getKeyPathList(mdl)

    def formatNameOrKeyToKey(self, fmtsubmitted):
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
