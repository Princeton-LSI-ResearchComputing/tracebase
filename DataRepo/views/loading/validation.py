import os.path
import shutil
import tempfile
from typing import List

import pandas as pd
import yaml  # type: ignore
from django.conf import settings
from django.core.management import call_command
from django.shortcuts import redirect, render
from django.views.generic.edit import FormView

from DataRepo.forms import DataSubmissionValidationForm
from DataRepo.utils.accucor_data_loader import get_sample_headers
from DataRepo.utils.exceptions import MultiLoadStatus


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
    mzxml_files: List[str] = []
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
            for afp in accucor_files:
                self.mzxml_files.extend(self.get_mzxml_names(afp))
        else:
            self.accucor_files = []

        if isocorr_files:
            self.isocorr_files = isocorr_files
            for ifp in isocorr_files:
                self.mzxml_files.extend(self.get_mzxml_names(ifp))
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

        debug = f"asf: {self.animal_sample_file} num afs: {len(self.accucor_files)} num ifs: {len(self.isocorr_files)}"

        valid, results, exceptions, ordered_keys = self.get_validation_results()

        return self.render_to_response(
            self.get_context_data(
                results=results,
                debug=debug,
                valid=valid,
                form=form,
                exceptions=exceptions,
                submission_url=self.submission_url,
                ordered_keys=ordered_keys,
            )
        )

    def get_validation_results(self):
        load_status_data = self.validate_study()

        valid = load_status_data.is_valid
        results = {}
        exceptions = {}
        ordered_keys = []

        for load_key in load_status_data.get_ordered_status_keys():
            # The load_key is the absolute path, but we only want to report errors in the context of the file's name
            short_load_key = os.path.basename(load_key)

            ordered_keys.append(short_load_key)
            results[short_load_key] = load_status_data.statuses[load_key]["state"]

            exceptions[short_load_key] = []
            # Get the AggregatedErrors object
            aes = load_status_data.statuses[load_key]["aggregated_errors"]
            # aes is None if there were no exceptions
            if aes is not None:
                for exc in aes.exceptions:
                    exc_str = aes.get_buffered_exception_summary_string(
                        exc, numbered=False, typed=False
                    )
                    exceptions[short_load_key].append(
                        {
                            "message": exc_str,
                            "is_error": exc.is_error,
                            "type": type(exc).__name__,
                        }
                    )

        return valid, results, exceptions, ordered_keys

    # No need to disable autoupdates adding the @no_autoupdates decorator to this function because supplying
    # `validate=True` automatically disables them
    def validate_study(self):
        tmpdir_obj = tempfile.TemporaryDirectory()
        tmpdir = tmpdir_obj.name

        # TODO: create_yaml() *could* raise ValueError or KeyError if the required sample file is not provided or if 2
        # accucor/isocorr files have the same name, but there should be checks on the submitted form data in the Django
        # form which should call form_invalid in those cases, so instead of adding a graceful exception to the user
        # *here* to catch those cases, add those checks to the form validation.  Check that the required sample file is
        # provided and that none of the accucor/isocorr files have the same name)
        yaml_file = self.create_yaml(tmpdir)

        load_status_data = MultiLoadStatus()

        try:
            call_command(
                "load_study",
                yaml_file,
                validate=True,
                verbosity=3,
            )
        except MultiLoadStatus as mls:
            load_status_data = mls

        tmpdir_obj.cleanup()

        return load_status_data

    def create_yaml(self, tmpdir):
        basic_loading_data = {
            # TODO: Add the ability for the validation interface to take tissues, compounds, and a separate protocols
            # file
            # The following are placeholders - Not yet supported by the validation view
            # "tissues": "tissues.tsv",
            # "compounds": "compounds.tsv",
            "protocols": None,  # Added by self.add_sample_data()
            "animals_samples_treatments": {
                "table": None,  # Added by self.add_sample_data()
                "skip_researcher_check": False,
            },
            "accucor_data": {
                "accucor_files": [
                    # {
                    #     "name": None,  # Added by self.add_ms_data()
                    #     "isocorr_format": False,  # Set by self.add_ms_data()
                    # },
                ],
                "msrun_protocol": "Default",
                "lc_protocol": "unknown",
                "instrument": "Default instrument",
                "date": "1972-11-24",
                "researcher": "anonymous",
                "new_researcher": False,
                "mzxml_files": self.mzxml_files,
            },
        }

        # Going to use a temp directory so we can report the user's given file names (not the randomized name supplied
        # by django forms)
        self.add_sample_data(basic_loading_data, tmpdir)
        self.add_ms_data(
            basic_loading_data,
            tmpdir,
            self.accucor_files,
            self.accucor_filenames,
            False,
        )
        self.add_ms_data(
            basic_loading_data, tmpdir, self.isocorr_files, self.isocorr_filenames, True
        )

        loading_yaml = os.path.join(tmpdir, "loading.yaml")

        with open(loading_yaml, "w") as file:
            yaml.dump(basic_loading_data, file)

        return loading_yaml

    def add_sample_data(self, basic_loading_data, tmpdir):
        if self.animal_sample_file is None:
            # The form_invalid method should prevent this via the website, but if this is called in a test or other
            # code without calling self.set_files, this exception will be raised.
            raise ValueError(
                "An animal and sample table file is required.  Be sure to call set_files() before calling "
                "validate_load_files()."
            )

        form_sample_file_path = self.animal_sample_file

        # The django form gives a random file name, but the user's name is available.  Here, we're supporting with or
        # without the user's file name to support tests that don't use a randomized temp file.
        if self.animal_sample_filename:
            sf = self.animal_sample_filename
        else:
            # This is for non-random file names (e.g. for the test code)
            sf = os.path.basename(form_sample_file_path)

        # To associate the file with the yaml file created in the temp directory, we must copy it
        sfp = os.path.join(tmpdir, str(sf))
        shutil.copyfile(form_sample_file_path, sfp)

        basic_loading_data["protocols"] = sfp
        basic_loading_data["animals_samples_treatments"]["table"] = sfp

    def add_ms_data(self, basic_loading_data, tmpdir, files, filenames, is_isocorr):
        for i, form_file_path in enumerate(files):
            if len(files) == len(filenames):
                fn = filenames[i]
            else:
                # This is for non-random file names (e.g. for the test code)
                fn = os.path.basename(form_file_path)

            fp = os.path.join(tmpdir, str(fn))
            shutil.copyfile(form_file_path, fp)

            if fn in [
                dct["name"]
                for dct in basic_loading_data["accucor_data"]["accucor_files"]
            ]:
                ft = "Isocorr" if is_isocorr else "Accucor"
                raise KeyError(
                    f"{ft} filename conflict: {fn}.  All Accucor/Isocorr file names must be unique."
                )

            basic_loading_data["accucor_data"]["accucor_files"].append(
                {
                    "name": fp,
                    "isocorr_format": is_isocorr,
                }
            )

    def get_mzxml_names(self, fp):
        """
        mzXML files are required for new submissions, but for now, we will simply compute the names based on the sample
        columns

        TODO: Add the ability to submit mzxml files.
        """
        corrected_df = pd.read_excel(
            fp,
            sheet_name=1,  # The second sheet
            engine="openpyxl",
        ).dropna(axis=0, how="all")

        return [f"{smpl_hdr}.mzxml" for smpl_hdr in get_sample_headers(corrected_df)]


def validation_disabled(request):
    return render(request, "validation_disabled.html")
