import datetime
import os.path
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
    isocorr_filenames: List[str] = []
    isocorr_files: List[str] = []
    animal_sample_filename = None
    animal_sample_file = None
    submission_url = settings.DATA_SUBMISSION_URL

    def set_files(
        self,
        sample_file,
        accucor_files=None,
        isocorr_files=None,
        sample_file_name=None,
        accucor_file_names=None,
        isocorr_file_names=None,
    ):
        """
        This method allows the files to be set.  It takes 2 different optional params for file names (that are used in
        reporting) to accommodate random temporary file names.  If file names are not supplied, the basename of the
        actual files is used for reporting.
        """
        self.animal_sample_file = sample_file
        self.animal_sample_filename = sample_file_name

        if accucor_files:
            self.accucor_files = accucor_files
        else:
            self.accucor_files = []

        if isocorr_files:
            self.isocorr_files = isocorr_files
        else:
            self.isocorr_files = []

        if accucor_file_names:
            self.accucor_filenames = accucor_file_names
        else:
            self.accucor_filenames = []

        if isocorr_file_names:
            self.isocorr_filenames = isocorr_file_names
        else:
            self.isocorr_filenames = []

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
            accucor_files = []
        try:
            isocorr_files = request.FILES.getlist("isocorr_files")
        except Exception:
            # Ignore missing isocorr files (allow user to validate just the sample file)
            isocorr_files = []

        self.set_files(
            sample_file.temporary_file_path(),
            [afp.temporary_file_path() for afp in accucor_files],
            [ifp.temporary_file_path() for ifp in isocorr_files],
            sample_file_name=sample_file,
            accucor_file_names=accucor_files,
            isocorr_file_names=isocorr_files,
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

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)} num ifs: {len(self.isocorr_files)}"

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

        self.valid = True
        self.results = {sf: "PASSED"}
        self.errors = {sf: []}
        self.warnings = {sf: []}
        for i, afp in enumerate(self.accucor_files):
            if len(self.accucor_files) == len(self.accucor_filenames):
                af = self.accucor_filenames[i]
            else:
                af = os.path.basename(afp)
            self.errors[af] = []
            self.warnings[af] = []
            self.results[af] = "PASSED"
        for i, ifp in enumerate(self.isocorr_files):
            if len(self.isocorr_files) == len(self.isocorr_filenames):
                ifn = self.isocorr_filenames[i]
            else:
                ifn = os.path.basename(ifp)
            if ifn in self.errors.keys():
                raise KeyError(
                    f"Isocorr/Accucor filename conflict: {ifn}.  All Accucor/Isocorr filenames must be "
                    "unique."
                )
            self.errors[ifn] = []
            self.warnings[ifn] = []
            self.results[ifn] = "PASSED"

        self.all_exceptions = []

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
                    self.package_exception(e, sf)

                try:
                    # Not running in debug, because these need to be loaded in order to run the next load
                    call_command(
                        "load_animals_and_samples",
                        animal_and_sample_table_filename=sfp,
                        defer_autoupdates=True,
                        verbosity=3,
                        validate=True,
                    )
                except Exception as e:
                    self.package_exception(e, sf)

                # Create a unique date that is unlikely to match any previously loaded MSRun
                unique_date = datetime.datetime(1972, 11, 24, 15, 47, 0)

                # Validate the accucor files using the loader in validate mode
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
                            date=str(unique_date.date()),
                            researcher="anonymous",
                            validate=True,
                        )
                    except Exception as e:
                        self.package_exception(e, af)
                    finally:
                        unique_date += datetime.timedelta(days=1)

                # Validate the isocorr files using the loader in validate mode
                for i, ifp in enumerate(self.isocorr_files):
                    if len(self.isocorr_files) == len(self.isocorr_filenames):
                        ifn = self.isocorr_filenames[i]
                    else:
                        ifn = os.path.basename(ifp)
                    try:
                        call_command(
                            "load_accucor_msruns",
                            protocol="Default",
                            accucor_file=ifp,
                            date=str(unique_date.date()),
                            researcher="anonymous",
                            validate=True,
                            isocorr_format=True,
                        )
                    except Exception as e:
                        self.package_exception(e, af)
                    finally:
                        unique_date += datetime.timedelta(days=1)

                raise DryRun
        except DryRun:
            if settings.DEBUG:
                print("Successfuly completion of validation.")
        finally:
            # The database should roll back here, but we don't want to raise the exception for the user's view here.
            print("Validation done.")

        return [
            self.results,
            self.valid,
            self.errors,
            self.warnings,
        ]

    def package_exception(self, exception, file):
        """
        Packages an exception up for sending to the template
        """
        if isinstance(exception, AggregatedErrors):
            self.results[file] = "WARNING"
            self.valid = False
            if exception.num_errors > 0:
                self.results[file] = "FAILED"

            # Gather the errors/warnings to send to the template
            for exc in exception.exceptions:
                self.all_exceptions.append(exc)
                exc_str = f"{type(exc).__name__}: {str(exc)}"
                if exc.is_error:
                    self.errors[file].append(exc_str)
                else:
                    self.warnings[file].append(exc_str)
        else:
            self.valid = False
            self.all_exceptions.append(exception)
            self.errors[file].append(f"{type(exception).__name__}: {str(exception)}")
            self.results[file] = "FAILED"


def validation_disabled(request):
    return render(request, "validation_disabled.html")
