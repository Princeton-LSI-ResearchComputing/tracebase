import timeit
from django.conf import settings
from DataRepo.hier_cached_model import disable_caching_retrievals, enable_caching_retrievals, get_cached_method_names, enable_caching_errors
from DataRepo.models import Animal, Sample, PeakGroup, PeakData
import warnings

# Let's ignore all(/most) of the noise - 
warnings.filterwarnings("ignore")
settings.DEBUG = False
enable_caching_errors()

def cached_function_call(cls, cfunc_name):
    for rec in cls.objects.all():
        try:
            getattr(rec, cfunc_name)
        except Exception as e:
            print(e)
            return False
    return True

func_name_lists = get_cached_method_names()
for class_name in func_name_lists.keys():
    cls = eval(class_name)
    for cfunc_name in func_name_lists[class_name]:
        print()
        print(f"{cls.__name__}.{cfunc_name}")

        print("\tProcessing:")
        print("\t\tProfiling without caching...")
        disable_caching_retrievals()
        nocache_time = timeit.timeit("cached_function_call(cls, cfunc_name)", globals=locals(), number=100)

        print("\t\tBuilding cache...")
        enable_caching_retrievals()
        # Ensure everything is cached
        status = cached_function_call(cls, cfunc_name)

        if status:
            print("\t\tProfiling with caching...")
            cache_time = timeit.timeit("cached_function_call(cls, cfunc_name)", globals=locals(), number=100)

            imp = nocache_time / cache_time
            improvement = str(round(imp, 2))
            degradation = str(round(1/imp, 2))

            print("\tResults:")
            print(f"\t\tCACHED:   {cache_time}")
            print(f"\t\tUNCACHED: {nocache_time}")
            if imp > 1:
                print(f"\t\t{improvement}x speedup with caching")
                if imp >= 1.01:
                    print(f"\tConclusion:\n\t\tKEEP {cls.__name__}.{cfunc_name} as a @cached_function")
                else:
                    print(f"\tConclusion:\n\t\tREMOVE @cached_function decorator from {cls.__name__}.{cfunc_name}")
            else:
                print(f"\t\t{degradation}x slowdown with caching")
                print(f"\tConclusion:\n\t\tREMOVE @cached_function decorator from {cls.__name__}.{cfunc_name}")
        else:
            print(f"\t\tException occurred with caching")
            print(f"\tConclusion:\n\t\tREMOVE @cached_function decorator from {cls.__name__}.{cfunc_name}")
