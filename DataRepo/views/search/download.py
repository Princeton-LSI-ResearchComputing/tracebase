import json
from datetime import datetime

from django.conf import settings
from django.http import Http404, StreamingHttpResponse
from django.template import loader
from django.views.generic.edit import FormView

from DataRepo.formats.dataformat_group_query import (
    isQryObjValid,
    isValidQryObjPopulated,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import AdvSearchDownloadForm


# Basis: https://stackoverflow.com/questions/29672477/django-export-current-queryset-to-csv-by-button-click-in-browser
class AdvancedSearchTSVView(FormView):
    """
    This is the download view for the advanced search page.
    """

    form_class = AdvSearchDownloadForm
    header_template = "DataRepo/search/downloads/download_header.tsv"
    row_template = "DataRepo/search/downloads/download_row.tsv"
    content_type = "application/text"
    success_url = ""
    basv_metadata = SearchGroup()

    def form_invalid(self, form):
        saved_form = form.saved_data
        qry = {}
        if "qryjson" in saved_form:
            # Discovered this can cause a KeyError during testing, so...
            qry = json.loads(saved_form["qryjson"])
        else:
            print("ERROR: qryjson hidden input not in saved form.")
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        res = {}
        return self.render_to_response(
            self.get_context_data(res=res, qry=qry, dt=dt_string, debug=settings.DEBUG)
        )

    def form_valid(self, form):
        cform = form.cleaned_data
        try:
            qry = json.loads(cform["qryjson"])
            # Apparently this causes a TypeError exception in test_views. Could not figure out why, so...
        except TypeError:
            qry = cform["qryjson"]
        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            print("ERROR: Invalid qry object: ", qry)
            raise Http404("Invalid json")

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        filename = (
            qry["searches"][qry["selectedtemplate"]]["name"]
            + "_"
            + now.strftime("%d.%m.%Y.%H.%M.%S")
            + ".tsv"
        )

        if isValidQryObjPopulated(qry):
            res, _, _ = self.basv_metadata.performQuery(
                qry, qry["selectedtemplate"]
            )
        else:
            res, _, _ = self.basv_metadata.getAllBrowseData(
                qry["selectedtemplate"]
            )

        headtmplt = loader.get_template(self.header_template)
        rowtmplt = loader.get_template(self.row_template)

        return StreamingHttpResponse(
            tsv_template_iterator(rowtmplt, headtmplt, res, qry, dt_string),
            content_type=self.content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )


def tsv_template_iterator(rowtmplt, headtmplt, res, qry, dt):
    yield headtmplt.render({"qry": qry, "dt": dt})
    for row in res:
        yield rowtmplt.render({"qry": qry, "row": row})
