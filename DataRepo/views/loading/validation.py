import traceback
from typing import List

from django.conf import settings
from django.core.management import call_command
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.models import Compound, CompoundSynonym, Protocol, Tissue
from DataRepo.models.utilities import get_all_models
from DataRepo.utils import MissingSamplesError, ResearcherError


class DataValidationView(FormView):
    form_class = DataSubmissionValidationForm
    template_name = "DataRepo/validate_submission.html"
    success_url = ""
    accucor_files: List[str] = []
    animal_sample_file = None
    submission_url = settings.DATA_SUBMISSION_URL

    def dispatch(self, request, *args, **kwargs):
        # check if there is some video onsite
        if not settings.VALIDATION_ENABLED:
            return redirect("validatedown")
        else:
            return super(DataValidationView, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)
        self.accucor_files = request.FILES.getlist("accucor_files")
        try:
            self.animal_sample_file = request.FILES["animal_sample_table"]
        except Exception:
            # Ignore missing accucor files
            print("ERROR: No accucor file")
        if form.is_valid():
            return self.form_valid(form)
        else:
            return self.form_invalid(form)

    def form_valid(self, form):
        """
        Upon valid file submission, adds validation messages to the context of the validation page.
        """

        errors = {}
        debug = "untouched"
        valid = True
        results = {}

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)}"

        animal_sample_dict = {
            str(self.animal_sample_file): self.animal_sample_file.temporary_file_path(),
        }
        accucor_dict = dict(
            map(lambda x: (str(x), x.temporary_file_path()), self.accucor_files)
        )

        [results, valid, errors] = self.validate_load_files(
            animal_sample_dict, accucor_dict
        )

        return self.render_to_response(
            self.get_context_data(
                results=results,
                debug=debug,
                valid=valid,
                form=form,
                errors=errors,
                submission_url=self.submission_url,
            )
        )

    def validate_load_files(self, animal_sample_dict, accucor_dict):
        errors = {}
        valid = True
        results = {}
        animal_sample_name = list(animal_sample_dict.keys())[0]

        # Copy protocols from the tracebase database to the validation database
        # This assumes the Protocol table in the validation database is empty, which should be valid given the call to
        # clear_validation_database in the `finally` block below and the fact that the protocols loader does not
        # default-load the validation database (in the current code)
        for rec in Protocol.objects.using(settings.DEFAULT_DB).all():
            rec.save(using=settings.VALIDATION_DB)

        try:
            errors[animal_sample_name] = []
            results[animal_sample_name] = ""

            # Load the animal treatments
            try:
                call_command(
                    "load_protocols",
                    protocols=animal_sample_dict[animal_sample_name],
                    validate=True,
                    verbosity=2,
                )
                # Do not set PASSED here. If the full animal/sample table load passes, THEN this file has passed.
            except Exception as e:
                if settings.DEBUG:
                    traceback.print_exc()
                    print(str(e))
                valid = False
                errors[animal_sample_name].append(f"{e.__class__.__name__}: {str(e)}")
                results[animal_sample_name] = "FAILED"

            # If the protocol load didn't fail...
            if results[animal_sample_name] != "FAILED":
                # Load the animal and sample table in debug mode to check the researcher and sample name uniqueness
                # We are doing this debug run to be able to tell if the researcher exception should be ignored
                try:
                    # debug=True is supposed to NOT commit the DB changes, but it IS creating the study, so even though
                    # I'm using debug here, I am also setting the database to the validation database...
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=animal_sample_dict[
                            animal_sample_name
                        ],
                        debug=True,
                        validate=True,
                    )
                    results[animal_sample_name] = "PASSED"
                except ResearcherError as re:
                    valid = False
                    errors[animal_sample_name].append(
                        "[The following error about a new researcher name should only be addressed if the name "
                        "already exists in the database as a variation.  If this is a truly new researcher name in "
                        f"the database, it may be ignored.]\n{animal_sample_name}: {str(re)}"
                    )
                    results[animal_sample_name] = "WARNING"
                except Exception as e:
                    estr = str(e)
                    # We are using the presence of the string "Debugging..." to infer that it got to the end of the
                    # load without an exception.  If there is no "Debugging" message, then an exception did not occur
                    # anyway
                    if settings.DEBUG:
                        traceback.print_exc()
                        print(estr)
                    if "Debugging" not in estr:
                        valid = False
                        errors[animal_sample_name].append(
                            f"{e.__class__.__name__}: {estr}"
                        )
                        results[animal_sample_name] = "FAILED"
                    else:
                        results[animal_sample_name] = "PASSED"

            can_proceed = False
            if results[animal_sample_name] != "FAILED":
                # Load the animal and sample data into the validation database, so the data is available for the
                # accucor file validation
                try:
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=animal_sample_dict[
                            animal_sample_name
                        ],
                        skip_researcher_check=True,
                        validate=True,
                    )
                    can_proceed = True
                except Exception as e:
                    estr = str(e)
                    # We are using the presence of the string "Debugging..." to infer that it got to the end of the
                    # load without an exception.  If there is no "Debugging" message, then an exception did not occur
                    # anyway
                    if settings.DEBUG:
                        traceback.print_exc()
                        print(estr)
                    valid = False
                    errors[animal_sample_name].append(
                        f"{animal_sample_name} {e.__class__.__name__}: {str(e)}"
                    )
                    results[animal_sample_name] = "FAILED"
                    can_proceed = False

            # Load the accucor file into a temporary test database in debug mode
            for af, afp in accucor_dict.items():
                errors[af] = []
                if can_proceed is True:
                    try:
                        self.validate_accucor(afp, [])
                        results[af] = "PASSED"
                    except MissingSamplesError as mse:
                        blank_samples = []
                        real_samples = []

                        # Determine whether all the missing samples are blank samples
                        for sample in mse.sample_list:
                            if "blank" in sample:
                                blank_samples.append(sample)
                            else:
                                real_samples.append(sample)

                        # Rerun ignoring blanks if all were blank samples, so we can check everything else
                        if len(blank_samples) > 0 and len(blank_samples) == len(
                            mse.sample_list
                        ):
                            try:
                                self.validate_accucor(afp, blank_samples)
                                results[af] = "PASSED"
                            except Exception as e:
                                estr = str(e)
                                # We are using the presence of the string "Debugging..." to infer that it got to the
                                # end of the load without an exception.  If there is no "Debugging" message, then an
                                # exception did not occur anyway
                                if settings.DEBUG:
                                    traceback.print_exc()
                                    print(estr)
                                if "Debugging" not in estr:
                                    valid = False
                                    results[af] = "FAILED"
                                    errors[af].append(estr)
                                else:
                                    results[af] = "PASSED"
                        else:
                            valid = False
                            results[af] = "FAILED"
                            errors[af].append(
                                "Samples in the accucor file are missing in the animal and sample table: "
                                + f"[{', '.join(real_samples)}]"
                            )
                    except Exception as e:
                        estr = str(e)
                        # We are using the presence of the string "Debugging..." to infer that it got to the end of the
                        # load without an exception.  If there is no "Debugging" message, then an exception did not
                        # occur anyway
                        if settings.DEBUG:
                            traceback.print_exc()
                            print(estr)
                        if "Debugging" not in estr:
                            valid = False
                            results[af] = "FAILED"
                            errors[af].append(estr)
                        else:
                            results[af] = "PASSED"
                else:
                    # Cannot check because the samples did not load
                    results[af] = "UNCHECKED"
        finally:
            # Clear out the user's validated data so that they'll be able to try again
            self.clear_validation_database()

        return [
            results,
            valid,
            errors,
        ]

    def clear_validation_database(self):
        """
        Clear out every table aside from compounds and tissues, which are intended to persist in the validation
        database, as they are needed to create related links for data inserted by the load animals/samples scripts
        """
        skips = [Compound, CompoundSynonym, Tissue]

        for mdl in get_all_models():
            if mdl not in skips:
                mdl.objects.using(settings.VALIDATION_DB).all().delete()

    def validate_accucor(self, accucor_file, skip_samples):
        if len(skip_samples) > 0:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file=accucor_file,
                date="2021-09-14",
                researcher="anonymous",
                debug=True,
                skip_samples=skip_samples,
                validate=True,
            )
        else:
            call_command(
                "load_accucor_msruns",
                protocol="Default",
                accucor_file=accucor_file,
                date="2021-09-13",
                researcher="anonymous",
                debug=True,
                validate=True,
            )


def validation_disabled(request):
    return render(request, "validation_disabled.html")
