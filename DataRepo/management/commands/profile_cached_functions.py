import timeit
import warnings

from django.conf import settings
from django.core.management import BaseCommand

from DataRepo.hier_cached_model import (
    disable_caching_retrievals,
    enable_caching_errors,
    enable_caching_retrievals,
    get_cached_method_names,
)
from DataRepo.models.animal import Animal  # noqa: F401
from DataRepo.models.peak_data import PeakData  # noqa: F401
from DataRepo.models.peak_group import PeakGroup  # noqa: F401
from DataRepo.models.sample import Sample  # noqa: F401


def profile():
    def cached_function_call(cls, cfunc_name, max_recs):
        for rec in cls.objects.all()[:max_recs]:
            try:
                getattr(rec, cfunc_name)
            except Exception as e:
                print(e)
                return False
        return True

    # Let's ignore all(/most) of the noise  and allow exceptions to be thrown so we can catch and report them
    warnings.filterwarnings("ignore")
    settings.DEBUG = False
    enable_caching_errors()

    func_name_lists = get_cached_method_names()
    iterations = {
        "default": 1,
        "Animal": 20,
        "Sample": 2,
        "PeakGroup": 1,
        "PeakData": 1,
    }
    max_num_recs = 1300

    for class_name in func_name_lists.keys():
        cls = eval(class_name)
        if class_name in iterations:
            iters = iterations[class_name]
        else:
            iters = iterations["default"]

        for cfunc_name in func_name_lists[class_name]:
            print()
            print(f"{class_name}.{cfunc_name}")

            print("\tProcessing:")
            print("\t\tProfiling without caching...")

            disable_caching_retrievals()
            nocache_time = timeit.timeit(
                "cached_function_call(cls, cfunc_name, max_num_recs)",
                globals=locals(),
                number=iters,
            )

            print("\t\tBuilding cache...")

            enable_caching_retrievals()
            # Ensure everything is cached
            status = cached_function_call(cls, cfunc_name, max_num_recs)

            if status:
                print("\t\tProfiling with caching...")
                cache_time = timeit.timeit(
                    "cached_function_call(cls, cfunc_name, max_num_recs)",
                    globals=locals(),
                    number=iters,
                )

                imp = nocache_time / cache_time
                improvement = str(round(imp, 2))
                degradation = str(round(1 / imp, 2))

                print("\tResults:")
                print(f"\t\tCACHED:   {cache_time}")
                print(f"\t\tUNCACHED: {nocache_time}")
                print("\tConclusion:")
                if imp > 1:
                    print(f"\t\t{improvement}x speedup with caching")
                    if imp >= 1.01:
                        print(
                            f"\t\tKEEP {class_name}.{cfunc_name} as a @cached_function"
                        )
                    else:
                        print(
                            f"\t\tREMOVE @cached_function decorator from {class_name}.{cfunc_name}"
                        )
                else:
                    print(f"\t\t{degradation}x slowdown with caching")
                    print(
                        f"\t\tREMOVE @cached_function decorator from {class_name}.{cfunc_name}"
                    )
            else:
                print("\tResults:")
                print("\t\tExceptn occurred with caching")
                print("\tConclusion:")
                print("\t\tCaching doesn't work for this function")
                print(
                    f"\t\tREMOVE @cached_function decorator from {class_name}.{cfunc_name}"
                )


class Command(BaseCommand):
    # Show this when the user types help
    help = "Profiles all cached_functions"

    def handle(self, *args, **options):
        profile()
