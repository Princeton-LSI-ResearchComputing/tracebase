from copy import deepcopy

from DataRepo.tests.tracebase_test_case import TracebaseTestCase


class FormatsTestCase(TracebaseTestCase):
    """This base class defines methods that generate some data for use in derived test classes"""

    archive_file_instances = [
        [
            "PeakAnnotationFile",
            "PeakAnnotationFile",
            "ArchiveFile",
        ],
        ["RAWFile", "RAWFile", "ArchiveFile"],
        ["MZFile", "MZFile", "ArchiveFile"],
    ]

    def getPdtemplateChoicesTuple(self):
        return (
            ("peak_group__msrun_sample__sample__animal__age", "Age"),
            ("peak_group__msrun_sample__sample__animal__name", "Animal"),
            (
                "peak_group__msrun_sample__sample__animal__body_weight",
                "Body Weight (g)",
            ),
            ("corrected_abundance", "Corrected Abundance"),
            ("peak_group__msrun_sample__sample__animal__diet", "Diet"),
            (
                "peak_group__msrun_sample__sample__animal__feeding_status",
                "Feeding Status",
            ),
            ("peak_group__formula", "Formula"),
            ("peak_group__msrun_sample__sample__animal__genotype", "Genotype"),
            ("peak_group__msrun_sample__sample__animal__infusate__name", "Infusate"),
            (
                "peak_group__msrun_sample__sample__animal__infusion_rate",
                "Infusion Rate (ul/min/g)",
            ),
            ("labels__count", "Labeled Count"),
            ("labels__element", "Labeled Element"),
            (
                "peak_group__msrun_sample__sample__msrun_samples__ms_data_file__filename",
                "MZ Data Filename",
            ),
            (
                "peak_group__msrun_sample__msrun_sequence__researcher",
                "Mass Spec Operator",
            ),
            (
                "peak_group__msrun_sample__msrun_sequence__instrument",
                "Mass Spectrometer Name",
            ),
            (
                "peak_group__compounds__synonyms__name",
                "Measured Compound (Any Synonym)",
            ),
            ("peak_group__compounds__name", "Measured Compound (Primary Synonym)"),
            ("med_mz", "Median M/Z"),
            ("med_rt", "Median RT"),
            ("peak_group__peak_annotation_file__filename", "Peak Annotation Filename"),
            ("peak_group__name", "Peak Group"),
            (
                "peak_group__msrun_sample__sample__msrun_samples__ms_raw_file__filename",
                "RAW Data Filename",
            ),
            ("raw_abundance", "Raw Abundance"),
            ("peak_group__msrun_sample__sample__name", "Sample"),
            ("peak_group__msrun_sample__sample__animal__sex", "Sex"),
            ("peak_group__msrun_sample__sample__animal__studies__name", "Study"),
            (
                "peak_group__msrun_sample__sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("peak_group__msrun_sample__sample__tissue__name", "Tissue"),
            (
                "peak_group__msrun_sample__sample__animal__infusate__tracers__name",
                "Tracer",
            ),
            (
                "peak_group__msrun_sample__sample__animal__infusate__tracers__compound__name",
                "Tracer Compound (Primary Synonym)",
            ),
            (
                "peak_group__msrun_sample__sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("peak_group__msrun_sample__sample__animal__treatment__name", "Treatment"),
        )

    def getPgtemplateChoicesTuple(self):
        return (
            ("msrun_sample__sample__animal__age", "Age"),
            ("msrun_sample__sample__animal__name", "Animal"),
            ("msrun_sample__sample__animal__body_weight", "Body Weight (g)"),
            ("compounds__synonyms__name", "Compound (Measured) (Any Synonym)"),
            ("compounds__name", "Compound (Measured) (Primary Synonym)"),
            (
                "msrun_sample__sample__animal__infusate__tracers__compound__name",
                "Compound (Tracer) (Primary Synonym)",
            ),
            ("msrun_sample__sample__animal__diet", "Diet"),
            ("msrun_sample__sample__animal__feeding_status", "Feeding Status"),
            ("formula", "Formula"),
            ("msrun_sample__sample__animal__genotype", "Genotype"),
            ("msrun_sample__sample__animal__infusate__name", "Infusate"),
            ("msrun_sample__sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("labels__element", "Labeled Element"),
            (
                "msrun_sample__sample__msrun_samples__ms_data_file__filename",
                "MZ Data Filename",
            ),
            ("msrun_sample__msrun_sequence__researcher", "Mass Spec Operator"),
            ("msrun_sample__msrun_sequence__instrument", "Mass Spectrometer Name"),
            ("peak_annotation_file__filename", "Peak Annotation Filename"),
            ("name", "Peak Group"),
            (
                "msrun_sample__sample__msrun_samples__ms_raw_file__filename",
                "RAW Data Filename",
            ),
            ("msrun_sample__sample__name", "Sample"),
            ("msrun_sample__sample__animal__sex", "Sex"),
            ("msrun_sample__sample__animal__studies__name", "Study"),
            (
                "msrun_sample__sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("msrun_sample__sample__tissue__name", "Tissue"),
            ("msrun_sample__sample__animal__infusate__tracers__name", "Tracer"),
            (
                "msrun_sample__sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("msrun_sample__sample__animal__treatment__name", "Treatment"),
        )

    def getFctemplateChoicesTuple(self):
        return (
            ("serum_sample__animal__name", "Animal"),
            ("serum_sample__animal__age", "Animal Age"),
            ("serum_sample__animal__body_weight", "Body Weight (g)"),
            ("serum_sample__animal__diet", "Diet"),
            ("serum_sample__animal__feeding_status", "Feeding Status"),
            ("serum_sample__animal__genotype", "Genotype"),
            ("serum_sample__animal__infusion_rate", "Infusion Rate (ul/min/g)"),
            ("is_last", "Is Last Serum Tracer Peak Group"),
            ("element", "Peak Group Labeled Element"),
            ("serum_sample__animal__sex", "Sex"),
            ("serum_sample__animal__studies__name", "Study"),
            (
                "serum_sample__time_collected",
                "Time Collected (since infusion)",
            ),
            ("tracer__name", "Tracer"),
            ("tracer__compound__name", "Tracer Compound (Primary Synonym)"),
            (
                "serum_sample__animal__infusate__tracer_links__concentration",
                "Tracer Concentration (mM)",
            ),
            ("serum_sample__animal__treatment__name", "Treatment"),
        )

    def getQueryObject(self):
        return {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "static": False,
                                "ncmp": "icontains",
                                "fld": "msrun_sample__sample__animal__studies__name",
                                "val": "obob_fasted",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "PeakGroups",
                },
                "pdtemplate": {"name": "PeakData", "tree": {}},
                "fctemplate": {"name": "Fcirc", "tree": {}},
            },
        }

    def getQueryObject2(self):
        qry = deepcopy(self.getQueryObject())
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"].append(
            {
                "type": "query",
                "pos": "",
                "static": False,
                "ncmp": "icontains",
                "fld": "compounds__synonyms__name",
                "val": "glucose",
                "units": "identity",
            }
        )
        return qry

    def get_advanced_qry(self):
        """
        Create a simple advanced query
        """
        return {
            "selectedtemplate": "pgtemplate",
            "searches": {
                "pgtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "msrun_sample__sample__tissue__name",
                                "ncmp": "iexact",
                                "static": "",
                                "val": "Brain",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "PeakGroups",
                },
                "pdtemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "ncmp": "iexact",
                                "static": "",
                                "fld": "labels__element",
                                "val": "",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "PeakData",
                },
                "fctemplate": {
                    "tree": {
                        "pos": "",
                        "type": "group",
                        "val": "all",
                        "static": False,
                        "queryGroup": [
                            {
                                "type": "query",
                                "pos": "",
                                "fld": "msrun_sample__sample__animal__name",
                                "ncmp": "iexact",
                                "static": "",
                                "val": "",
                                "units": "identity",
                            }
                        ],
                    },
                    "name": "FCirc",
                },
            },
        }

    def get_advanced_qry2(self):
        """
        Modify the query returned by get_advanced_qry to include search terms on 2 M:M related tables in a sub-group.
        """
        qry = self.get_advanced_qry()
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0][
            "fld"
        ] = "msrun_sample__sample__name"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"][0]["val"] = "BAT-xz971"
        qry["searches"]["pgtemplate"]["tree"]["queryGroup"].append(
            {
                "type": "group",
                "val": "all",
                "static": False,
                "queryGroup": [
                    {
                        "type": "query",
                        "pos": "",
                        "static": False,
                        "ncmp": "iexact",
                        "fld": "msrun_sample__sample__animal__studies__name",
                        "val": "obob_fasted",
                        "units": "identity",
                    },
                    {
                        "type": "query",
                        "pos": "",
                        "static": False,
                        "ncmp": "iexact",
                        "fld": "compounds__synonyms__name",
                        "val": "glucose",
                        "units": "identity",
                    },
                ],
            }
        )
        return qry

    def getExpectedStats(self):
        return {
            "available": True,
            "based_on": None,
            "data": {
                "Animals": {
                    "count": 1,
                    "filter": None,
                    "sample": [
                        {
                            "cnt": 2,
                            "val": "971",
                        },
                    ],
                },
                "Feeding Statuses": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "Fasted"}],
                },
                "Infusion Rates": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "0.11"}],
                },
                "Labeled Elements": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "C"}],
                },
                "Measured Compounds": {
                    "count": 2,
                    "filter": None,
                    "sample": sorted(
                        [
                            {"cnt": 1, "val": "glucose"},
                            {"cnt": 1, "val": "lactate"},
                        ],
                        key=lambda d: d["val"],
                    ),
                },
                "Samples": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "Br-xz971"}],
                },
                "Studies": {
                    "count": 2,
                    "filter": None,
                    "sample": sorted(
                        [
                            {"cnt": 2, "val": "Small OBOB"},
                            {"cnt": 2, "val": "obob_fasted"},
                        ],
                        key=lambda d: d["val"],
                    ),
                },
                "Tissues": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "brain"}],
                },
                "Tracer Compounds": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "lysine"}],
                },
                "Tracer Concentrations": {
                    "count": 1,
                    "filter": None,
                    "sample": [{"cnt": 2, "val": "lysine:23.2"}],
                },
            },
            "populated": True,
            "show": True,
        }
