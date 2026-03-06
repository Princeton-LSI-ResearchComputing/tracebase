from parameterized import parameterized

from DataRepo.formats.peakgroups_dataformat import PeakGroupsFormat
from DataRepo.models.compound import CompoundSynonym
from DataRepo.tests.formats.formats_test_base import FormatsTestCase


class PeakgroupsDataformatMainTests(FormatsTestCase):

    def test_peak_groups_format(self):
        """Test __main__.PeakGroupsFormat - no exception = successful test"""
        PeakGroupsFormat()


class PeakGroupsFormatTests(FormatsTestCase):

    @parameterized.expand(FormatsTestCase.archive_file_instances)
    def test_peak_groups_format_get_model_from_instance(self, _, instance, model):
        pgsv = PeakGroupsFormat()
        res = pgsv.get_model_from_instance(instance)
        self.assertEqual(res, model)

    def test_get_f_k_model_name(self):
        pgf = PeakGroupsFormat()
        mdl_name = pgf.get_f_k_model_name(CompoundSynonym(), "compound")
        self.assertEqual("Compound", mdl_name)

    def test_get_order_by_fields_model(self):
        pgf = PeakGroupsFormat()
        mdl = "Compound"

        order_bys = pgf.get_order_by_fields(model_name=mdl)
        expected_order_bys = ["name"]
        self.assertEqual(expected_order_bys, order_bys)

    def test_get_order_by_fields_both(self):
        pgsv = PeakGroupsFormat()
        mdl_inst = "MeasuredCompound"
        mdl = "Compound"

        with self.assertRaises(
            Exception, msg="mdl_inst_nm and model_name are mutually exclusive options."
        ):
            pgsv.get_order_by_fields(mdl_inst_nm=mdl_inst, model_name=mdl)

    def test_get_order_by_fields_neither(self):
        pgsv = PeakGroupsFormat()
        mdl_inst = "MeasuredCompound"
        mdl = "Compound"

        with self.assertRaises(
            Exception, msg="Either a model instance name or model name is required."
        ):
            pgsv.get_order_by_fields(mdl_inst_nm=mdl_inst, model_name=mdl)

    def test_re_root_field_path(self):
        fld = "msrun_sample__sample__animal__studies__name"
        reroot_instance_name = "CompoundSynonym"
        pgf = PeakGroupsFormat()
        rerooted_fld = pgf.re_root_field_path(fld, reroot_instance_name)
        expected_fld = (
            "compound__peak_groups__msrun_sample__sample__animal__studies__name"
        )
        self.assertEqual(expected_fld, rerooted_fld)

    def test_path_to_model_instance_name(self):
        pgf = PeakGroupsFormat()
        mi = pgf.path_to_model_instance_name("msrun_sample__sample__animal__studies")
        self.assertEqual("Study", mi)

    def test_get_distinct_fields_split_all(self):
        """
        Ensures that meta ordering fields are expanded to real database fields.  I.e. it tests that fields from every
        M:M model (WRT root) like "compounds__synonyms__compound" are dereferenced to the field from that model's
        Meta.ordering, like "compounds__synonyms__compound__name".

        It also tests that every model instance whose model has ["manyrelated"]["manytomany"] as True is included in
        the returned field set.
        """
        pgf = PeakGroupsFormat()
        self.assertIn(
            "compound",
            CompoundSynonym._meta.__dict__["ordering"],
            msg="CompoundSynonym must have 'compound' in meta.ordering for the next assertion to be meaningful",
        )
        distincts = pgf.get_distinct_fields(split_all=True)
        # This includes fields expanded from every M:M model
        expected_distincts = [
            "name",
            "pk",
            "labels__peak_group__name",
            "labels__element",
            "labels__pk",
            "msrun_sample__sample__animal__infusate__name",
            "msrun_sample__sample__animal__infusate__tracer_links__tracer__name",
            "msrun_sample__sample__animal__infusate__tracer_links__concentration",
            "msrun_sample__sample__animal__infusate__tracer_links__pk",
            "msrun_sample__sample__animal__infusate__tracers__name",
            "msrun_sample__sample__animal__infusate__tracers__pk",
            "msrun_sample__sample__animal__infusate__tracers__compound__name",
            "msrun_sample__sample__animal__infusate__tracers__compound__pk",
            "compounds__name",
            "compounds__pk",
            "compounds__synonyms__compound__name",
            "compounds__synonyms__name",
            "compounds__synonyms__pk",
            "msrun_sample__sample__animal__studies__name",
            "msrun_sample__sample__animal__studies__pk",
            "msrun_sample__sample__msrun_samples__ms_data_file__pk",
            "msrun_sample__sample__msrun_samples__ms_raw_file__pk",
        ]
        self.assertEqual(expected_distincts, distincts)

    def test_get_stats_params(self):
        pgf = PeakGroupsFormat()
        stats = pgf.get_stats_params()
        got = stats[2]
        expected_i2 = {
            "displayname": "Measured Compounds",
            "distincts": ["compounds__name"],
            "filter": None,
        }
        self.assertEqual(expected_i2, got)
