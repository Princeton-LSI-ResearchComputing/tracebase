import os.path
import traceback
from typing import List

from django.conf import settings
from django.core.management import call_command
from django.db import transaction
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.utils import DryRun
from DataRepo.utils.exceptions import AggregatedErrors


class DataValidationView(FormView):
    form_class = DataSubmissionValidationForm
    template_name = "DataRepo/validate_submission.html"
    success_url = ""
    accucor_filenames: List[str] = []
    accucor_files: List[str] = []
    animal_sample_filename = None
    animal_sample_file = None
    submission_url = settings.DATA_SUBMISSION_URL

    def set_files(
        self, sample_file, accucor_files, sample_file_name=None, accucor_file_names=None
    ):
        """
        This method allows the files to be set.  It takes 2 different optional params for file names (that are used in
        reporting) to accommodate random temporary file names.  If file names are not supplied, the basename of the
        actual files is used for reporting.
        """
        self.animal_sample_file = sample_file
        self.accucor_files = accucor_files
        self.animal_sample_filename = sample_file_name
        if accucor_file_names:
            self.accucor_filenames = accucor_file_names
        else:
            self.accucor_filenames = []

    def dispatch(self, request, *args, **kwargs):
        # check if there is some video onsite
        if not settings.VALIDATION_ENABLED:
            return redirect("validatedown")
        else:
            return super(DataValidationView, self).dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        form_class = self.get_form_class()
        form = self.get_form(form_class)

        sample_file = request.FILES["animal_sample_table"]
        try:
            accucor_files = request.FILES.getlist("accucor_files")
        except Exception:
            # Ignore missing accucor files (allow user to validate just the sample file)
            print("ERROR: No accucor file")
            accucor_files = []

        self.set_files(
            sample_file.temporary_file_path(),
            [afp.temporary_file_path() for afp in accucor_files],
            sample_file_name=sample_file,
            accucor_file_names=accucor_files,
        )

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
        if self.animal_sample_filename:
            sf = self.animal_sample_filename
        else:
            sf = os.path.basename(self.animal_sample_file)
        sfp = self.animal_sample_file

        if self.animal_sample_file is None:
            # The form_invalid method should prevent this via the website, but if this is called in a test or other
            # code without calling self.set_files, this exception will be raised.
            raise ValueError(
                "An animal and sample table file is required.  Be sure to call set_files() before calling "
                "validate_load_files()."
            )

        valid = True
        results = {sf: "PASSED"}
        errors = {sf: []}
        warnings = {sf: []}
        for i, afp in enumerate(self.accucor_files):
            if len(self.accucor_files) == len(self.accucor_filenames):
                af = self.accucor_filenames[i]
            else:
                af = os.path.basename(afp)
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
                    if aes.num_errors > 0:
                        valid = False
                        results[sf] = "FAILED"

                    # Gather the errors/warnings to send to the template
                    for exc in aes.exceptions:
                        all_exceptions.append(exc)
                        exc_str = f"{type(exc).__name__}: {str(exc)}"
                        if exc.is_error:
                            errors[sf].append(exc_str)
                        else:
                            warnings[sf].append(exc_str)

                except Exception as e:
                    valid = False
                    all_exceptions.append(e)
                    results[sf] = "FAILED"
                    errors[sf].append(f"{type(e).__name__}: {str(e)}")

                # Load the accucor file into a temporary test database in debug mode
                for i, afp in enumerate(self.accucor_files):
                    if len(self.accucor_files) == len(self.accucor_filenames):
                        af = self.accucor_filenames[i]
                    else:
                        af = os.path.basename(afp)
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
                        if aes.num_errors > 0:
                            valid = False
                            results[af] = "FAILED"

                        # Gather the errors/warnings to send to the template
                        for exc in aes.exceptions:
                            all_exceptions.append(exc)
                            exc_str = f"{type(exc).__name__}: {str(exc)}"
                            if exc.is_error:
                                errors[af].append(exc_str)
                            else:
                                warnings[af].append(exc_str)

                raise DryRun
        except DryRun:
            if settings.DEBUG:
                print("Successfuly completion of validation.")
        finally:
            # The database should roll back here, but we don't want to raise the exception for the user's view here.
            print("Validation done.")
            if settings.DEBUG:
                for exc in all_exceptions:
                    traceback.print_exception(type(exc), exc, exc.__traceback__)
                    print(f"{type(exc).__name__}: {str(exc)}")

        return [
            results,
            valid,
            errors,
            warnings,
        ]


def validation_disabled(request):
    return render(request, "validation_disabled.html")
