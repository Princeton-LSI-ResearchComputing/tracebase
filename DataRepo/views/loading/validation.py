import traceback
from typing import List

from django.conf import settings
from django.core.management import call_command
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.models import Compound, CompoundSynonym, Protocol, Tissue
from DataRepo.models.researcher import UnknownResearcherError
from DataRepo.models.utilities import get_all_models
from DataRepo.utils import DryRun, MissingSamplesError
from DataRepo.utils.exceptions import AggregatedErrors


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
        for rec in Protocol.objects.using(settings.DEFAULT_DB).values():
            # We must delete AutoField key/value pairs because it screws up the next AutoField generation
            del rec["id"]
            Protocol.objects.using(settings.VALIDATION_DB).create(**rec)

        try:
            errors[animal_sample_name] = []
            results[animal_sample_name] = ""

            # Load the animal treatments
            try:
                call_command(
                    "load_protocols",
                    protocols=animal_sample_dict[animal_sample_name],
                    validate=True,
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
                # Load the animal and sample table with the researcher check so we can catch any possible
                # UnknownResearcherErrors.  We will then run again without the researcher check so that the data
                # actually gets loaded, thus enabling the accucor data load validation in the next step.
                try:
                    # debug=True is supposed to NOT commit the DB changes, but it IS creating the study, so even though
                    # I'm using debug here, I am also running in validate mode, which uses the validation database...
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=animal_sample_dict[
                            animal_sample_name
                        ],
                        debug=True,
                        validate=True,
                    )
                    results[animal_sample_name] = "PASSED"
                except DryRun as dr:
                    if settings.DEBUG:
                        traceback.print_exc()
                        print(str(dr))
                    results[animal_sample_name] = "PASSED"
                except AggregatedErrors as ae:
                    # Set overall validity
                    valid = False

                    # Set the exception level (WARNING or FAILED)
                    if len(ae.errors) == 0:
                        results[animal_sample_name] = "WARNING"
                    else:
                        results[animal_sample_name] = "FAILED"

                    # Annotate the errors and warnings for the lab member
                    for warning in ae.warnings:
                        wstr = f"{type(warning).__name__}: {str(warning)}"
                        if settings.DEBUG:
                            traceback.print_exception(
                                type(warning), warning, warning.__traceback__
                            )
                            print(wstr)
                        if isinstance(warning, UnknownResearcherError):
                            errors[animal_sample_name].append(
                                "[The following error about a new researcher name should only be addressed if the "
                                "name already exists in the database as a variation.  If this is a truly new "
                                f"researcher name in the database, it may be ignored.]\n{animal_sample_name}: {wstr}"
                            )
                    for err in ae.errors:
                        estr = f"{type(err).__name__}: {str(err)}"
                        if settings.DEBUG:
                            traceback.print_exception(type(err), err, err.__traceback__)
                            print(estr)
                        errors[animal_sample_name].append(estr)

            # Now let's run without the researcher check
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
                except AggregatedErrors as ae:
                    if len(ae.errors) > 0:
                        errors[animal_sample_name].append(
                            "An unexpected validation error has occurred.  Unable to validate the accucor file(s).  "
                            f"The following {len(ae.errors)} errors were not raised in the researcher check run..."
                        )
                    for err in ae.errors:
                        if settings.DEBUG:
                            traceback.print_exc()
                            print(str(err))
                        errors[animal_sample_name].append(
                            f"{animal_sample_name} {err.__class__.__name__}: {str(err)}"
                        )
                    # We do not need to rehash the warnings that were reported in the first run
                    valid = False
                    results[animal_sample_name] = "FAILED"
                    can_proceed = False

            # Load the accucor file into a temporary test database in debug mode
            for af, afp in accucor_dict.items():
                errors[af] = []
                if can_proceed is True:
                    try:
                        self.validate_accucor(afp, [])
                        results[af] = "PASSED"
                    except AggregatedErrors as aes:

                        results[af] = "PASSED"
                        for error in aes.errors:
                            estr = f"{type(error).__name__}: {str(error)}"
                            if settings.DEBUG:
                                traceback.print_exception(
                                    type(error), error, error.__traceback__
                                )
                                print(estr)
                            if isinstance(error, MissingSamplesError):
                                blank_samples = []
                                real_samples = []
                                # Determine whether all the missing samples are blank samples
                                for sample in error.sample_list:
                                    if "blank" in sample:
                                        blank_samples.append(sample)
                                    else:
                                        real_samples.append(sample)

                                # Ignore blanks if all were blank samples, so we can check everything else
                                if len(blank_samples) > 0 and len(blank_samples) != len(
                                    error.sample_list
                                ):
                                    valid = False
                                    results[af] = "FAILED"
                                    errors[af].append(
                                        f"Samples in the accucor file [{af}] are missing in the animal and sample "
                                        + f"table: [{', '.join(real_samples)}]"
                                    )
                            else:
                                valid = False
                                results[af] = "FAILED"
                                errors[af].append(estr)

                    except DryRun:
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
