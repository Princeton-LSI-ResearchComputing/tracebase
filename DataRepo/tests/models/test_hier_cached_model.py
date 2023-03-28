from django.core.management import call_command

from DataRepo.management.commands.build_caches import cached_function_call
from DataRepo.models import Animal, MSRun, PeakGroup, Sample
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_retrievals,
    disable_caching_updates,
    enable_caching_retrievals,
    enable_caching_updates,
    get_cache,
    get_cache_key,
    get_cached_method_names,
    set_cache,
)
from DataRepo.tests.tracebase_test_case import TracebaseTestCase


def load_data():
    load_minimum_data()
    call_command(
        "load_accucor_msruns",
        protocol="Default",
        accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_serum.xlsx",
        date="2021-06-03",
        researcher="Michael Neinast",
        new_researcher=False,
    )


def load_minimum_data():
    call_command("load_study", "DataRepo/example_data/tissues/loading.yaml")
    call_command(
        "load_compounds",
        compounds="DataRepo/example_data/small_dataset/small_obob_compounds.tsv",
    )
    call_command(
        "load_samples",
        "DataRepo/example_data/small_dataset/small_obob_sample_table.tsv",
        sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
    )
    call_command(
        "load_samples",
        "DataRepo/example_data/small_dataset/small_obob_sample_table_2ndstudy.tsv",
        sample_table_headers="DataRepo/example_data/sample_table_headers.yaml",
    )
    call_command(
        "load_accucor_msruns",
        protocol="Default",
        accucor_file="DataRepo/example_data/small_dataset/small_obob_maven_6eaas_inf.xlsx",
        date="2021-06-03",
        researcher="Michael Neinast",
        new_researcher=True,
    )


class GlobalCacheTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        load_data()
        super().setUpTestData()

    def test_load_not_cached(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        v, s = get_cache(a, f)
        self.assertEqual(
            v,
            None,
            msg="Load commands in setUpTestData should not have cached anything (value is None)",
        )
        self.assertFalse(
            s,
            msg="Load commands in setUpTestData should not have cached anything (status is false)",
        )

    def test_get_cache_disabled(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        # Ensure I get the value not using the cache
        disable_caching_retrievals()
        v = getattr(a, f)  # same as `v = a.last_serum_sample`
        delete_all_caches()
        enable_caching_updates()
        set_cache(a, "last_serum_sample", v)
        disable_caching_retrievals()
        res, sts = get_cache(a, "last_serum_sample")
        self.assertEqual(
            res,
            None,
            msg="Cached value should be None when caching retrievals are disabled",
        )
        self.assertFalse(
            sts,
            msg="Cached status should be false when caching retrievals are disabled",
        )

    def test_get_cache_uncached(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        delete_all_caches()
        enable_caching_retrievals()
        res, sts = get_cache(a, f)
        print(f"Returned cached value: [{str(res.__class__)}]")
        self.assertEqual(
            res, None, msg="Cached value should be None when no cached value exists"
        )
        self.assertFalse(
            sts, msg="Cached status should be false when when no cached value exists"
        )

    def test_get_cache_enabled(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        # Ensure I get the value not using the cache
        disable_caching_retrievals()
        v = getattr(a, f)  # same as `v = a.last_serum_sample`
        delete_all_caches()
        enable_caching_updates()
        set_cache(a, "last_serum_sample", v)
        enable_caching_retrievals()
        res, sts = get_cache(a, "last_serum_sample")
        self.assertEqual(
            res,
            v,
            msg="Cached value should be correct when caching retrievals are enabled and the value is cached",
        )
        self.assertTrue(
            sts,
            msg="Cached status should be true when caching retrievals are enabled and the value is cached",
        )

    def test_set_cache_disabled(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        # Ensure I get the value not using the cache
        disable_caching_retrievals()
        v = getattr(a, f)  # same as `v = a.last_serum_sample`
        delete_all_caches()
        disable_caching_updates()
        sts = set_cache(a, "last_serum_sample", v)
        self.assertFalse(
            sts,
            msg="Cache save status should be false when caching updates are disabled",
        )

    def test_set_cache_enabled(self):
        a = Animal.objects.all().first()
        # Ensure I get the value not using the cache
        disable_caching_retrievals()
        # Representative
        rr, rf = a.get_representative_root_rec_and_method()
        rv = getattr(rr, rf)  # Representative value
        # Value in question
        f = "last_serum_sample"  # Field
        v = getattr(a, f)  # same as `v = a.last_serum_sample`

        delete_all_caches()
        enable_caching_retrievals()
        vres, vsts = get_cache(a, f)
        self.assertFalse(
            vsts,
            msg="Initial cache retrieval status must be false for the next assertions to be meaningful",
        )
        self.assertEqual(
            vres,
            None,
            msg="Initial cache retrieval value must be None for the next assertions to be meaningful",
        )
        rvres, rvsts = get_cache(a, rf)
        self.assertFalse(
            rvsts,
            msg="Initial cache retrieval representative status must be false for the next assertions to be meaningful",
        )
        self.assertEqual(
            rvres,
            None,
            msg="Initial cache retrieval representative value must be None for the next assertions to be meaningful",
        )

        enable_caching_updates()
        sts = set_cache(a, f, v)
        gvres, gvsts = get_cache(a, f)
        self.assertTrue(
            gvsts,
            msg="After cache save retrieval status must be true for the next assertions to be meaningful",
        )
        self.assertEqual(
            gvres,
            v,
            msg="Cached value should be the same as before it was saved in the cache",
        )
        self.assertTrue(
            sts, msg="Cache save status should be true when caching updates are enabled"
        )

        grvres, grvsts = get_cache(a, rf)
        self.assertTrue(
            grvsts,
            msg="After cache save representative retrieval status must be true for the next assertion to be meaningful",
        )
        self.assertEqual(
            grvres.count(),
            rv.count(),
            msg=(
                "Caching a child value should trigger a caching of the root model's representative value, which should "
                "be the same as before it was saved in the cache"
            ),
        )
        self.assertEqual(
            grvres.count(),
            1,
            msg=(
                "Caching a child value should trigger a caching of the root model's representative value, which should "
                "be the same as before it was saved in the cache"
            ),
        )
        self.assertEqual(
            grvres.first().id,
            rv.first().id,
            msg=(
                "Caching a child value should trigger a caching of the root model's representative value, which should "
                "be the same as before it was saved in the cache"
            ),
        )

    def test_get_cache_key(self):
        a = Animal.objects.all().first()
        f = "last_serum_sample"
        expected_key = f"Animal.{a.id}.{f}"
        res = get_cache_key(a, f)
        self.assertEqual(
            res, expected_key, msg="Cache key is not in the expected format"
        )

    def test_get_cached_method_names(self):
        res = get_cached_method_names()
        expected_structure = {
            "Animal": [
                "tracers",
                "last_serum_tracer_peak_groups",
            ],
            "AnimalLabel": [
                "tracers",
                "last_serum_tracer_label_peak_groups",
                "serum_tracers_enrichment_fraction",
            ],
            "Sample": [
                "last_tracer_peak_groups",
                "is_last_serum_sample",
            ],
            "PeakGroup": ["peak_labeled_elements"],
            "FCirc": [
                "last_peak_group_in_animal",
                "last_peak_group_in_sample",
                "peak_groups",
                "serum_validity",
                "rate_disappearance_intact_per_gram",
                "rate_appearance_intact_per_gram",
                "rate_disappearance_intact_per_animal",
                "rate_appearance_intact_per_animal",
                "rate_disappearance_average_per_gram",
                "rate_appearance_average_per_gram",
                "rate_disappearance_average_per_animal",
                "rate_appearance_average_per_animal",
            ],
            "PeakGroupLabel": [
                "enrichment_fraction",
                "enrichment_abundance",
                "normalized_labeling",
                "tracer",
                "tracer_label_count",
                "tracer_concentration",
                "get_peak_group_label_tracer_info",
                "is_tracer_label_compound_group",
                "from_serum_sample",
                "can_compute_tracer_label_rates",
                "can_compute_body_weight_intact_tracer_label_rates",
                "can_compute_body_weight_average_tracer_label_rates",
                "can_compute_intact_tracer_label_rates",
                "can_compute_average_tracer_label_rates",
                "rate_disappearance_intact_per_gram",
                "rate_appearance_intact_per_gram",
                "rate_disappearance_intact_per_animal",
                "rate_appearance_intact_per_animal",
                "rate_disappearance_average_per_gram",
                "rate_appearance_average_per_gram",
                "rate_disappearance_average_per_animal",
                "rate_appearance_average_per_animal",
            ],
        }
        self.assertEqual(
            expected_structure,
            res,
            "The methods with @cached_function decorators are not what is expected.",
        )


class HierCachedModelTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        load_data()
        super().setUpTestData()

    def test_cached_function_decorator(self):
        delete_all_caches()
        pg = PeakGroup.objects.all().first().labels.first()
        f = "normalized_labeling"

        # Get uncached value
        disable_caching_retrievals()
        disable_caching_updates()
        nl = getattr(pg, f)

        # Trigger caching via decorator
        enable_caching_retrievals()
        enable_caching_updates()
        saved_return = getattr(pg, f)  # same as `saved_return = pg.normalized_labeling`
        cnl, sts = get_cache(pg, f)
        self.assertTrue(
            sts,
            msg="Ensure what we got back was a cached value so the next assertion is meaningful",
        )
        self.assertEqual(
            saved_return,
            nl,
            msg="Ensure decorator cache save returns the correct value",
        )
        self.assertEqual(
            cnl,
            nl,
            msg="Ensure decorator works to save and retrieve cache, and that the value is correct",
        )

    def createASampleCache(self):
        delete_all_caches()
        smp = Sample.objects.all().first()
        f = "last_tracer_peak_groups"

        enable_caching_retrievals()
        enable_caching_updates()
        # Trigger caching via decorator
        getattr(smp, f)  # same as `smp.is_serum_sample`
        rep_rec, rep_fnc = smp.get_representative_root_rec_and_method()

        # Ensure cached
        v, s = get_cache(smp, f)
        rv, rs = get_cache(rep_rec, rep_fnc)
        self.assertTrue(
            s,
            msg=(
                "Ensure what we got back was a cached value for the called function so the next assertions are "
                "meaningful"
            ),
        )
        self.assertTrue(
            rs,
            msg=(
                "Ensure what we got back was a cached value for the representative function so the next assertions "
                "are meaningful"
            ),
        )
        return smp, f, rep_rec, rep_fnc

    def test_save_override(self):
        smp, f, rep_rec, rep_fnc = self.createASampleCache()

        smp.save()

        # Ensure no longer cached
        nv, ns = get_cache(smp, f)
        nrv, nrs = get_cache(rep_rec, rep_fnc)
        self.assertFalse(
            ns,
            msg="Ensure the value for the called function in not cached after a save",
        )
        self.assertFalse(
            nrs,
            msg="Ensure the value for the representative function in not cached after a save",
        )

    def test_delete_override(self):
        delete_all_caches()
        pg = PeakGroup.objects.all().first().labels.first()
        f = "enrichment_fraction"

        enable_caching_retrievals()
        enable_caching_updates()
        # Trigger caching via decorator
        getattr(pg, f)
        rep_rec, rep_fnc = pg.get_representative_root_rec_and_method()

        # Ensure cached
        v, s = get_cache(pg, f)
        rv, rs = get_cache(rep_rec, rep_fnc)
        self.assertTrue(
            s,
            msg=(
                "Ensure what we got back was a cached value for the called function so the next assertions are "
                "meaningful"
            ),
        )
        self.assertTrue(
            rs,
            msg=(
                "Ensure what we got back was a cached value for the representative function so the next assertions "
                "are meaningful"
            ),
        )

        # delete() calls from animal, sample, and MSRun are restricted
        pg.delete()

        # Ensure no longer cached
        nv, ns = get_cache(pg, f)
        nrv, nrs = get_cache(rep_rec, rep_fnc)
        self.assertFalse(
            ns,
            msg="Ensure the value for the called function in not cached after a delete",
        )
        self.assertFalse(
            nrs,
            msg=(
                f"Ensure the value for the representative function {rep_rec.__class__.__name__}.{rep_fnc} in not "
                "cached after a delete"
            ),
        )

    def test_delete_descendant_caches(self):
        smp, f, rep_rec, rep_fnc = self.createASampleCache()

        smp.delete_descendant_caches()

        # Ensure no longer cached
        nv, ns = get_cache(smp, f)
        nrv, nrs = get_cache(rep_rec, rep_fnc)
        self.assertFalse(
            ns,
            msg="Ensure the value for the called function in not cached after a delete_descendant_caches",
        )

        # Ensure root cache is still intact
        self.assertTrue(
            nrs,
            msg=(
                f"Ensure the value for the representative function {rep_rec.__class__.__name__}.{rep_fnc} is still "
                "cached after a delete_descendant_caches on a descendant"
            ),
        )

    def test_delete_related_caches(self):
        smp, f, rep_rec, rep_fnc = self.createASampleCache()

        smp.delete_related_caches()

        # Ensure no longer cached
        nv, ns = get_cache(smp, f)
        nrv, nrs = get_cache(rep_rec, rep_fnc)
        self.assertFalse(
            ns,
            msg="Ensure the value for the called function in not cached after a delete_descendant_caches",
        )

        # Ensure root cache is still intact
        self.assertFalse(
            nrs,
            msg=(
                f"Ensure the value for the representative function {rep_rec.__class__.__name__}.{rep_fnc} is not "
                "cached after a delete_related_caches on a descendant"
            ),
        )

    def test_get_my_cached_method_names(self):
        pg = PeakGroup.objects.all().first()
        expected = ["peak_labeled_elements"]
        self.assertEqual(
            expected,
            pg.get_my_cached_method_names(),
            msg="Ensure get_my_cached_method_names returns all expected cache_functions.",
        )

    def test_caches_exist(self):
        delete_all_caches()
        # Get 2 samples belonging to the same animal and the second one's first peak group
        a = Animal.objects.all().first()
        samples = Sample.objects.filter(animal__id__exact=a.id)
        s1 = samples[0]
        s2 = samples[1]
        s2pg = (
            PeakGroup.objects.filter(msrun__sample__id__exact=s2.id)
            .first()
            .labels.first()
        )
        pgf = "enrichment_fraction"

        res1 = s1.caches_exist()
        self.assertFalse(
            res1,
            msg=(
                "caches_exist from uncached related object returns false when related record's cache does not yet "
                "exist"
            ),
        )

        # Cache sample 2's first peak group's enrichment_fraction value
        enable_caching_retrievals()
        enable_caching_updates()
        getattr(s2pg, pgf)

        # Call caches_exist on the first sample
        res2 = s1.caches_exist()

        self.assertTrue(
            res2,
            msg="caches_exist from uncached related object returns true for related record's cache existing",
        )

    def test_set_caches_exist(self):
        delete_all_caches()
        a = Animal.objects.all().first()
        samples = Sample.objects.filter(animal__id__exact=a.id)
        s1 = samples[0]
        s2 = samples[1]
        s2.set_caches_exist()
        res = s1.caches_exist()

        self.assertTrue(
            res,
            msg="set_caches_exist results in caches_exist returning true for related records",
        )

    def test_get_representative_root_rec_and_method(self):
        a = Animal.objects.all().first()
        s = Sample.objects.filter(animal__id__exact=a.id).first()
        msr = MSRun.objects.filter(sample__id__exact=s.id).first()

        rep_rec, rep_fnc = msr.get_representative_root_rec_and_method()
        cached_fncs = a.get_my_cached_method_names()
        first_fnc = cached_fncs[0]

        self.assertEqual(
            rep_rec.__class__.__name__,
            "Animal",
            msg=(
                "The representative record from get_representative_root_rec_and_method is from the root model (Animal)"
            ),
        )
        self.assertEqual(
            rep_rec.id,
            a.id,
            msg="The representative root model record from get_representative_root_rec_and_method is directly related",
        )
        self.assertEqual(
            rep_fnc,
            first_fnc,
            msg=(
                "The representative root model cached function from get_representative_root_rec_and_method is its "
                "first decorated function"
            ),
        )

    def test_get_root_record(self):
        a = Animal.objects.all().first()
        s = Sample.objects.filter(animal__id__exact=a.id).first()
        pg = PeakGroup.objects.filter(msrun__sample__id__exact=s.id).first()

        rep_rec = pg.get_root_record()

        self.assertEqual(
            rep_rec.__class__.__name__,
            "Animal",
            msg="The record returned by get_root_record is from the root model (Animal)",
        )
        self.assertEqual(
            rep_rec.id,
            a.id,
            msg="The root model record returned by get_root_record is directly related",
        )


class BuildCachesTests(TracebaseTestCase):
    @classmethod
    def setUpTestData(cls):
        load_minimum_data()
        super().setUpTestData()

    def test_cached_function_call(self):
        c = Animal
        f = "tracers"
        a = Animal.objects.all().first()
        la = Animal.objects.all().last()
        disable_caching_retrievals()
        # Get the first and last uncached value
        uv = getattr(a, f)
        lv = getattr(la, f)

        enable_caching_retrievals()
        enable_caching_updates()
        delete_all_caches()

        # Call cached_function_call to populate all cached values for f
        cached_function_call(c, f)

        # Try to retrieve those cached values
        v, s = get_cache(a, f)
        lv, ls = get_cache(la, f)

        # Ensure the value was cached for both the first and last record
        # Results are querysets, which never equate, but are equatable as lists
        self.assertEqual(list(v), list(uv))
        self.assertTrue(s)
        self.assertEqual(list(lv), list(uv))
        self.assertTrue(ls)
