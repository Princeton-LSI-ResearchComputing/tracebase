import traceback
from typing import List

from django.conf import settings
from django.core.management import call_command
from django.db import transaction
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

        [results, valid, errors, warnings] = self.validate_load_files()

        return self.render_to_response(
            self.get_context_data(
                results=results,
                debug=debug,
                valid=valid,
                form=form,
                errors=errors,
                warnings=warnings,
                submission_url=self.submission_url,
            )
        )

    def validate_load_files(self):
        sf = self.animal_sample_file
        sfp = sf.temporary_file_path()

        valid = True
        results = {sf: "PASSED"}
        errors = {sf: []}
        warnings = {sf: []}
        for af in self.accucor_files:
            errors[af] = []
            warnings[af] = []
            results[af] = "PASSED"

        all_exceptions = []

        # Copy protocols from the tracebase database to the validation database
        # This assumes the Protocol table in the validation database is empty, which should be valid given the call to
        # clear_validation_database in the `finally` block below and the fact that the protocols loader does not
        # default-load the validation database (in the current code)
        # for rec in Protocol.objects.using(settings.DEFAULT_DB).values():
        #     # We must delete AutoField key/value pairs because it screws up the next AutoField generation
        #     del rec["id"]
        #     Protocol.objects.using(settings.VALIDATION_DB).create(**rec)

        try:
            # Load the animal treatments
            with transaction.atomic():
                try:
                    # Not running in debug, because these need to be loaded in order to run the next load
                    call_command(
                        "load_protocols",
                        protocols=sfp,
                        verbosity=3,
                    )
                    # Do not set PASSED here. If the full animal/sample table load passes, THEN this file has passed.
                except Exception as e:
                    valid = False
                    all_exceptions.append(e)
                    errors[sf].append(f"{e.__class__.__name__}: {str(e)}")
                    results[sf] = "FAILED"

                try:
                    # Not running in debug, because these need to be loaded in order to run the next load
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=sfp,
                        defer_autoupdates=True,
                        verbosity=3,
                        validate=True,
                    )

                except AggregatedErrors as aes:
                    results[sf] = "WARNING"
                    if len(aes.errors) > 0:
                        valid = False
                        results[sf] = "FAILED"

                    # Gather the warnings to send to the template
                    for wrn in aes.warnings:
                        all_exceptions.append(wrn)
                        warnings[sf].append(f"{type(wrn).__name__}: {str(wrn)}")

                    # Gather the errors to send to the template
                    for err in aes.errors:
                        all_exceptions.append(err)
                        errors[sf].append(f"{type(err).__name__}: {str(err)}")

                except Exception as e:
                    valid = False
                    all_exceptions.append(e)
                    results[sf] = "FAILED"
                    errors[sf].append(f"{type(e).__name__}: {str(e)}")

                # Load the accucor file into a temporary test database in debug mode
                for af in self.accucor_files:
                    afp = af.temporary_file_path()
                    try:
                        call_command(
                            "load_accucor_msruns",
                            protocol="Default",
                            accucor_file=afp,
                            date="1972-11-24",
                            researcher="anonymous",
                            validate=True,
                        )
                    except AggregatedErrors as aes:
                        results[af] = "WARNING"
                        if len(aes.errors) > 0:
                            valid = False
                            results[af] = "FAILED"

                        for wrn in aes.warnings:
                            all_exceptions.append(wrn)
                            warnings[af].append(f"{type(wrn).__name__}: {str(wrn)}")

                        for err in aes.errors:
                            all_exceptions.append(err)
                            errors[af].append(f"{type(err).__name__}: {str(err)}")
                
                raise DryRun
        except DryRun:
            if settings.DEBUG:
                print("Successfuly completion of validation.")
        finally:
            # The database should roll back here, but we don't want to raise the exception for the user's view here.
            print("Validation done.")
            if settings.DEBUG:
                for e in all_exceptions:
                    traceback.print_exception(type(e), e, e.__traceback__)
                    print(f"{type(e).__name__}: {str(e)}")

        return [
            results,
            valid,
            errors,
            warnings,
        ]

def validation_disabled(request):
    return render(request, "validation_disabled.html")
