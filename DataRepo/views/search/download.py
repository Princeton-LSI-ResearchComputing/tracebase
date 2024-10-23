import csv
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Union

import _csv
from django.conf import settings
from django.db.models import Model
from django.http import Http404, StreamingHttpResponse
from django.template import loader
from django.views.generic.edit import FormView

from DataRepo.formats.dataformat_group_query import (
    isQryObjValid,
    isValidQryObjPopulated,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import AdvSearchDownloadForm
from DataRepo.models.msrun_sample import MSRunSample
from DataRepo.models.peak_data import PeakData
from DataRepo.models.peak_group import PeakGroup
from DataRepo.utils.file_utils import date_to_string
from DataRepo.utils.text_utils import sigfig


# See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
class Echo:
    def write(self, value):
        return value


# Basis: https://stackoverflow.com/questions/29672477/django-export-current-queryset-to-csv-by-button-click-in-browser
class AdvancedSearchDownloadView(FormView):
    """This is the download view for the advanced search page.  It defaults to a basic tsv file type.

    Create a derived class to create new file types.  Every derived class should be able to handle the query coming from
    the AdvSearchDownloadForm and the queryset that comes from DataRepo.formats.SearchGroup.  They should be able to
    handle one or more formats (i.e. Format classes (e.g. PeakGroupsFormat.id) in SearchGroup that identifies the file
    format).  E.g.

    class AdvancedSearchDownloadMzxmlTSVView(AdvancedSearchDownloadView):

    """

    form_class = AdvSearchDownloadForm
    success_url = ""
    basv_metadata = SearchGroup()
    date_format = "%d/%m/%Y %H:%M:%S"
    datestamp_format = "%d.%m.%Y.%H.%M.%S"
    header_template = "DataRepo/search/downloads/download_header.tsv"
    row_template = "DataRepo/search/downloads/download_row.tsv"
    content_type = "application/text"

    def get_qry(self, form):
        qry = {}
        if "qryjson" in form:
            try:
                qry = json.loads(form["qryjson"])
            except TypeError:
                qry = form["qryjson"]
        else:
            print("ERROR: qryjson hidden input not in saved form.")
        return qry

    def get_query_results(self, qry):
        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            print("ERROR: Invalid qry object: ", qry)
            raise Http404("Invalid json")

        if isValidQryObjPopulated(qry):
            res, _, _ = self.basv_metadata.performQuery(qry, qry["selectedtemplate"])
        else:
            res, _, _ = self.basv_metadata.getAllBrowseData(qry["selectedtemplate"])

        return res

    def form_invalid(self, form):
        qry = self.get_qry(form.saved_data)
        now = datetime.now()
        return self.render_to_response(
            self.get_context_data(
                res={},
                qry=qry,
                dt=now.strftime(self.date_format),
                debug=settings.DEBUG,
            )
        )

    def form_valid(self, form):
        qry = self.get_qry(form.cleaned_data)
        now = datetime.now()
        filename = f"{qry['searches'][qry['selectedtemplate']]['name']}_{now.strftime(self.datestamp_format)}.tsv"
        res = self.get_query_results(qry)

        headtmplt = loader.get_template(self.header_template)
        rowtmplt = loader.get_template(self.row_template)

        return StreamingHttpResponse(
            self.tsv_template_iterator(
                rowtmplt, headtmplt, res, qry, now.strftime(self.date_format)
            ),
            content_type=self.content_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    @classmethod
    def tsv_template_iterator(cls, rowtmplt, headtmplt, res, qry, dt):
        yield headtmplt.render({"qry": qry, "dt": dt})
        for row in res:
            yield rowtmplt.render({"qry": qry, "row": row})


class RecordToMzxmlTSV(ABC):
    """This class defines a download format and type.  In this instance, it is a tsv file format that defines a file of
    mzXML metadata.  It is an abstract base class.  Derived classes define how to convert a queryset from one or more
    search Format classes belonging to the SearchGroup class.  Derived classes must set a format_id class attribute that
    matches one of the SearchGroup Format classes, and a rec_to_rows method that does the conversion of a queryset
    record to a list of lists.

    This class and its derived classes are used by AdvancedSearchDownloadMzxmlTSVView to generate mzXML tsv download
    data.
    """

    @property
    @abstractmethod
    def format_id(self) -> str:
        """Derived classes must implement this as a class attribute.  The format_id must match one of the ids in the
        Format classes in the SearchGroup class."""
        pass

    @abstractmethod
    def rec_to_rows(self, rec: Model) -> Union[List[list], List[str]]:
        """Derived classes must implement this as a class attribute."""
        pass

    headers = [
        "mzXML File",
        "Polarity",
        "MZ Min",
        "MZ Max",
        "Sample",
        "Animal",
        "Tissue",
        "Infusate",
        "Operator",
        "Instrument",
        "LC Protocol",
        "Date",
    ]

    @classmethod
    def get_rec_to_rows_method(cls, format_id: str):
        """Takes a format_id and returns a rec_to_rows method that comes from the subclass whose format_id matches the
        supplied format_id.

        Args:
            format_id (str)
        Exceptions:
            Http404
        Returns
            rec_to_rows (funtion)
        """
        for derived_class in cls.__subclasses__():
            if format_id == derived_class.format_id:
                return derived_class().rec_to_rows
        raise Http404(
            f"Invalid format ID: {format_id}.  Must be one of "
            f"{[c.format_id for c in RecordToMzxmlTSV.__subclasses__()]}"
        )

    def msrun_sample_rec_to_row(self, msrsrec: MSRunSample) -> List[str]:
        """Takes an MSRunSample record and returns a list of values for the download of mzXML metadata.

        It is intended to accompany a series of mzXML files organized into subdirectories.

        Args:
            msrsrec (MSRunSample)
        Exceptions:
            None
        Returns:
            (List[str])
        """
        filepath = os.path.join(
            date_to_string(msrsrec.msrun_sequence.date),
            msrsrec.msrun_sequence.researcher,
            msrsrec.msrun_sequence.instrument,
            msrsrec.msrun_sequence.lc_method.name,
            msrsrec.polarity,
            f"{sigfig(msrsrec.mz_min)}-{sigfig(msrsrec.mz_max)}",
            msrsrec.ms_data_file.filename,
        )
        return [
            filepath,
            msrsrec.polarity,
            msrsrec.mz_min,
            msrsrec.mz_max,
            msrsrec.sample.name,
            msrsrec.sample.animal.name,
            msrsrec.sample.tissue.name,
            msrsrec.sample.animal.infusate.name,
            msrsrec.msrun_sequence.researcher,
            msrsrec.msrun_sequence.instrument,
            msrsrec.msrun_sequence.lc_method.name,
            date_to_string(msrsrec.msrun_sequence.date),
        ]


class PeakGroupsToMzxmlTSV(RecordToMzxmlTSV):
    """This class defines how to convert a PeakGroupsFormat class's queryset record to a list of lists (1 PeakGroup
    record can link to 0 or more mzXML files)."""

    # This format ID matches PeakGroupsFormat.id
    format_id = "pgtemplate"

    def rec_to_rows(self, rec: PeakGroup) -> Union[List[list], List[str]]:
        """Takes a PeakGroup record (from a queryset) and returns a list of lists of column data.

        Each record can link to many mzXML files, hence it returns multiple rows.

        Args:
            rec (PeakGroup)
        Exceptions:
            None
        Returns:
            rows (List[List[str]])
        """
        rows = []
        msrsrec: MSRunSample
        for msrsrec in rec.msrun_sample.sample.msrun_samples.all():
            if msrsrec.ms_data_file:
                rows.append(self.msrun_sample_rec_to_row(msrsrec))
        return rows


class PeakDataToMzxmlTSV(RecordToMzxmlTSV):
    """This class defines how to convert a PeakDataFormat class's queryset record to a list of lists (1 PeakData record
    can link to 0 or more mzXML files)."""

    # This format ID matches PeakDataFormat.id
    format_id = "pdtemplate"

    def rec_to_rows(self, rec: PeakData) -> Union[List[list], List[str]]:
        """Takes a PeakData record (from a queryset) and returns a list of lists of column data.

        Each record can link to many mzXML files, hence it returns multiple rows.

        Args:
            rec (PeakData)
        Exceptions:
            None
        Returns:
            rows (List[List[str]])
        """
        rows = []
        msrsrec: MSRunSample
        for msrsrec in rec.peak_group.msrun_sample.sample.msrun_samples.all():
            if msrsrec.ms_data_file:
                rows.append(self.msrun_sample_rec_to_row(msrsrec))
        return rows


class AdvancedSearchDownloadMzxmlTSVView(AdvancedSearchDownloadView):
    """This is a secondary download view for the advanced search page.

    This view is for a subset of SearchGroup formats (those that include mz data files in their results:
    PeakGroupsFormat and PeakDataFormat).
    """

    header_template = "DataRepo/search/downloads/search_metadata.txt"
    content_type = "application/text"

    # See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
    def form_valid(self, form):
        # Get the query object (for the commented metadata header)
        self.qry = self.get_qry(form.cleaned_data)
        # Get the date (for the commented metadata header)
        now = datetime.now()
        self.date_str = now.strftime(self.date_format)
        # Set the metadata header template object
        self.headtmplt = loader.get_template(self.header_template)

        # Get the column headers list
        self.headers = RecordToMzxmlTSV.headers

        # Get the search format ID and name
        format_id = self.qry["selectedtemplate"]
        self.format_name = self.qry["searches"][format_id]["name"]

        # Get the method that converts a record from the results queryset to a list of lists
        self.rec_to_rows = RecordToMzxmlTSV.get_rec_to_rows_method(format_id)

        # Perform the query and save the queryset
        self.res = self.get_query_results(self.qry)

        # Set the output file name
        self.filename = f"{self.format_name}_{now.strftime(self.datestamp_format)}.tsv"

        # Create a fake buffer object (needed to be able to use the csv package for streaming the tsv)
        pseudo_buffer = Echo()
        writer: "_csv._writer" = csv.writer(pseudo_buffer, delimiter="\t")

        return StreamingHttpResponse(
            self.tsv_iterator(writer),
            content_type=self.content_type,
            headers={"Content-Disposition": f"attachment; filename={self.filename}"},
        )

    def tsv_iterator(self, writer: "_csv._writer"):
        """This method is an iterator that returns lines of a tsv file."""
        # Commented metadata header containing search date and query info
        yield self.headtmplt.render({"qry": self.qry, "dt": self.date_str})
        # Column headers
        yield writer.writerow(self.headers)
        for rec in self.res:
            rows = self.rec_to_rows(rec)
            if len(rows) > 0:
                for row in rows:
                    yield writer.writerow(row)
