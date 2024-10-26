import csv
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime
from typing import List, Optional, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

import _csv
from django.conf import settings
from django.db.models.fields.files import FieldFile
from django.http import Http404, StreamingHttpResponse
from django.shortcuts import render
from django.template import loader
from django.views.generic.edit import FormView

from DataRepo.formats.dataformat_group_query import (
    isQryObjValid,
    isValidQryObjPopulated,
)
from DataRepo.formats.search_group import SearchGroup
from DataRepo.forms import AdvSearchDownloadForm, AdvSearchForm
from DataRepo.models.msrun_sample import MSRunSample
from DataRepo.models.peak_data import PeakData
from DataRepo.models.peak_group import PeakGroup
from DataRepo.utils.file_utils import date_to_string
from DataRepo.views.search.advanced import AdvancedSearchView


# See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
class Echo:
    """A class that implements just the write method of a file-like interface.

    This is intended for use by a csv writer.
    """

    def write(self, value):
        return value


class ZipBuffer:
    """A class that implements a zip-file-like interface.

    This is intended for use by a zipfile writer.
    """

    def __init__(self):
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)
        return len(data)

    def flush(self):
        pass

    def take(self):
        buf = self.buf
        self.buf = bytearray()
        return bytes(buf)

    def end(self):
        buf = self.buf
        self.buf = None
        return bytes(buf)


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
    # This is only used for form_invalid, so that form submissions that are invalid go to the advanced search page
    template_name = "DataRepo/search/query.html"

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

    def get_query_results(self, qry, **kwargs):
        if not isQryObjValid(qry, self.basv_metadata.getFormatNames().keys()):
            print("ERROR: Invalid qry object: ", qry)
            raise Http404("Invalid json")

        if isValidQryObjPopulated(qry):
            res, tot, stats = self.basv_metadata.performQuery(
                qry, qry["selectedtemplate"], **kwargs
            )
        else:
            res, tot, stats = self.basv_metadata.getAllBrowseData(
                qry["selectedtemplate"], **kwargs
            )

        return res, tot, stats

    def form_invalid(self, form):
        # TODO: I could not figure out how to redirect to /DataRepo/advanced_search, but this is better than the
        # exception I was seeing when the download form was invalid.  Figure out how to properly redirect to the
        # previous page when this happens.
        qry = self.get_qry(form.saved_data)
        rows_per_page = 10  # Fallback, bec. we don't know
        res, tot, stats = self.get_query_results(
            qry,
            limit=rows_per_page,
            offset=0,  # Fallback, bec. we don't know
            order_by=None,  # Fallback, bec. we don't know
            order_direction=None,  # Fallback, bec. we don't know
            generate_stats=False,  # Fallback, bec. we don't know
        )
        asv = AdvancedSearchView()
        asv.pager.update(
            other_field_inits={
                "qryjson": json.dumps(qry),
                "show_stats": False,
                "stats": json.dumps(stats),
            },
            tot=tot,
            page=1,  # Fallback, bec. we don't know
            rows=rows_per_page,
        )
        return render(
            self.request,
            self.template_name,
            content_type="text/html",
            context={
                "res": res,
                "tot": tot,
                "stats": stats,
                "pager": asv.pager,
                "download_forms": [
                    AdvSearchDownloadForm(initial={"qryjson": json.dumps(qry)})
                ],
                "forms": AdvSearchForm().form_classes,
                "qry": qry,
                "debug": settings.DEBUG,
                "root_group": self.basv_metadata.getRootGroup(),
                "default_format": self.basv_metadata.default_format,
                "ncmp_choices": self.basv_metadata.getComparisonChoices(),
                "fld_types": self.basv_metadata.getFieldTypes(),
                "fld_choices": self.basv_metadata.getSearchFieldChoicesDict(),
                "fld_units": self.basv_metadata.getFieldUnitsDict(),
                "error": (
                    "The download feature is malfunctioning.  Please report this error and click your browser's back "
                    "button."
                ),
                "mode": self.basv_metadata.default_mode,
                "format": self.basv_metadata.default_format,
            },
        )

    def form_valid(self, form):
        qry = self.get_qry(form.cleaned_data)
        now = datetime.now()
        filename = f"{qry['searches'][qry['selectedtemplate']]['name']}_{now.strftime(self.datestamp_format)}.tsv"
        res = list(self.get_query_results(qry))[0]

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
    matches one of the SearchGroup Format classes, and a queryset_to_rows_iterator method that does the conversion of a
    queryset record to a list of lists.

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
    def queryset_to_rows_iterator(self, qs):
        """Derived classes must implement this as a class attribute."""
        pass

    headers = [
        "mzXML File",
        "Polarity",
        "MZ Min",
        "MZ Max",
        "Sample",
        "Tissue",
        "Date Collected",
        "Collection Time (m)",
        "Handler",
        "Animal",
        "Age",
        "Sex",
        "Genotype",
        "Weight (g)",
        "Diet",
        "Feeding Status",
        "Treatment",
        "Infusate",
        "Operator",
        "Instrument",
        "LC Protocol",
        "Date",
    ]

    @classmethod
    def get_converter_object(cls, format_id: str):
        """Takes a format_id and returns the derived class object that comes from the subclass whose format_id matches
        the supplied format_id.

        Args:
            format_id (str)
        Exceptions:
            Http404
        Returns:
            (RecordToMzxmlTSV)
        """
        for derived_class in cls.__subclasses__():
            if format_id == derived_class.format_id:
                return derived_class()
        raise Http404(
            f"Invalid format ID: {format_id}.  Must be one of "
            f"{[c.format_id for c in cls.__subclasses__()]}"
        )

    def msrun_sample_rec_to_row(self, msrsrec: MSRunSample) -> List[Optional[str]]:
        """Takes an MSRunSample record and returns a list of values for the download of mzXML metadata.

        It is intended to accompany a series of mzXML files organized into subdirectories.

        Args:
            msrsrec (MSRunSample)
        Exceptions:
            None
        Returns:
            (List[Optional[str]])
        """
        return [
            msrsrec.mzxml_export_path,
            msrsrec.polarity,
            msrsrec.mz_min,
            msrsrec.mz_max,
            msrsrec.sample.name,
            msrsrec.sample.tissue.name,
            date_to_string(msrsrec.sample.date),
            msrsrec.sample.time_collected.total_seconds() / 60,
            msrsrec.sample.researcher,
            msrsrec.sample.animal.name,
            msrsrec.sample.animal.age.total_seconds() / 604800,
            msrsrec.sample.animal.sex,
            msrsrec.sample.animal.genotype,
            msrsrec.sample.animal.body_weight,
            msrsrec.sample.animal.diet,
            msrsrec.sample.animal.feeding_status,
            msrsrec.sample.animal.treatment.name,
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

    def queryset_to_rows_iterator(self, qs):
        """Takes a queryset of PeakGroup records and returns a list of lists of column data.

        Args:
            qs (QuerySet[PeakGroup])
        Exceptions:
            None
        Returns:
            (List[str])
        """
        seen = {}
        pgrec: PeakGroup
        for pgrec in qs.all():
            msrsrec: MSRunSample
            for msrsrec in pgrec.msrun_sample.sample.msrun_samples.all():
                if msrsrec.id not in seen.keys() and msrsrec.ms_data_file is not None:
                    yield self.msrun_sample_rec_to_row(msrsrec)
                    seen[msrsrec.id] = True


class PeakDataToMzxmlTSV(RecordToMzxmlTSV):
    """This class defines how to convert a PeakDataFormat class's queryset record to a list of lists (1 PeakData record
    can link to 0 or more mzXML files)."""

    # This format ID matches PeakDataFormat.id
    format_id = "pdtemplate"

    def queryset_to_rows_iterator(self, qs):
        """Takes a queryset of PeakData records and returns a list of lists of column data.

        Args:
            qs (QuerySet[PeakData])
        Exceptions:
            None
        Returns:
            rows (List[List[str]])
        """
        seen = {}
        pdrec: PeakData
        for pdrec in qs.all():
            msrsrec: MSRunSample
            for msrsrec in pdrec.peak_group.msrun_sample.sample.msrun_samples.all():
                if msrsrec.ms_data_file is not None and msrsrec.id not in seen.keys():
                    yield self.msrun_sample_rec_to_row(msrsrec)
                    seen[msrsrec.id] = True


class AdvancedSearchDownloadMzxmlTSVView(AdvancedSearchDownloadView):
    """This is a secondary download view for the advanced search page.

    This view is for a subset of SearchGroup formats (those that include mz data files in their results:
    PeakGroupsFormat and PeakDataFormat).  It is for streaming a download of the mzXML metadata in a TSV format.
    """

    header_template = "DataRepo/search/downloads/search_metadata.txt"
    content_type = "application/text"

    def __init__(self, qry=None, res=None):
        self.qry = qry

        # Get the date (for the commented metadata header)
        now = datetime.now()
        self.date_str = now.strftime(self.date_format)
        self.datestamp_str = now.strftime(self.datestamp_format)

        # Set the metadata header template object
        self.headtmplt = loader.get_template(self.header_template)

        # Get the column headers list
        self.headers = RecordToMzxmlTSV.headers

        self.format_id = None
        self.format_name = None
        self.converter = None
        self.res = None
        self.filename = None

        if qry is not None:
            self.prepare_download(qry=qry, res=res)

    def prepare_download(self, qry=None, res=None):
        if qry is None and self.qry is None:
            raise ValueError("Argument 'qry' cannot be None.")

        if self.qry is None:
            self.qry = deepcopy(qry)

        # Get the search format ID and name
        self.format_id = self.qry["selectedtemplate"]
        self.format_name = self.qry["searches"][self.format_id]["name"]

        # Get the converter object that converts a record from the results queryset to a list we can supply to csv
        self.converter = RecordToMzxmlTSV.get_converter_object(self.format_id)

        # Perform the query and save the queryset
        self.res = res if res is not None else list(self.get_query_results(self.qry))[0]

        # Set the output file name
        self.filename = f"{self.format_name}_{self.datestamp_str}.tsv"

    # See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
    def form_valid(self, form):
        # Get the query object (for the commented metadata header)
        self.qry = self.get_qry(form.cleaned_data)

        self.prepare_download()

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
        for row in self.converter.queryset_to_rows_iterator(self.res):
            yield writer.writerow(row)


class RecordToMzxmlZIP(ABC):
    """This class defines a download format and type.  In this instance, it is a tsv file format that defines a file of
    mzXML metadata.  It is an abstract base class.  Derived classes define how to convert a queryset from one or more
    search Format classes belonging to the SearchGroup class.  Derived classes must set a format_id class attribute that
    matches one of the SearchGroup Format classes, and a queryset_to_files_iterator method that does the conversion of a
    queryset record to a list of tuples (where each tuple is an export path and file object).

    This class and its derived classes are used by AdvancedSearchDownloadMzxmlZIPView to generate an mzXML zip archive
    download file.
    """

    @property
    @abstractmethod
    def format_id(self) -> str:
        """Derived classes must implement this as a class attribute.  The format_id must match one of the ids in the
        Format classes in the SearchGroup class."""
        pass

    @abstractmethod
    def queryset_to_files_iterator(self, qs):
        """Derived classes must implement this as a class attribute."""
        pass

    def msrun_sample_rec_to_file(
        self, msrsrec: MSRunSample
    ) -> Tuple[Optional[str], FieldFile]:
        """Takes an MSRunSample record and returns an export path string and a File object.  The path is generated as:
        date/researcher/instrument/protocol/polarity/mz_min-mz_max/filename.

        Args:
            msrsrec (MSRunSample)
        Exceptions:
            None
        Returns:
            export_path, file_location (str, FieldFile)
        """
        return msrsrec.mzxml_export_path, msrsrec.ms_data_file.file_location

    @classmethod
    def get_converter_object(cls, format_id: str):
        """Takes a format_id and returns the derived class object that comes from the subclass whose format_id matches
        the supplied format_id.

        Args:
            format_id (str)
        Exceptions:
            Http404
        Returns:
            (RecordToMzxmlZIP)
        """
        for derived_class in cls.__subclasses__():
            if format_id == derived_class.format_id:
                return derived_class()
        raise Http404(
            f"Invalid format ID: {format_id}.  Must be one of "
            f"{[c.format_id for c in cls.__subclasses__()]}"
        )


class PeakGroupsToMzxmlZIP(RecordToMzxmlZIP):
    """This class defines how to convert a PeakGroupsFormat class's queryset record to a list of lists (1 PeakGroup
    record can link to 0 or more mzXML files)."""

    # This format ID matches PeakGroupsFormat.id
    format_id = "pgtemplate"

    def queryset_to_files_iterator(self, qs):
        """Takes a queryset of PeakGroup records and returns a list of tuples of file data.

        Args:
            qs (QuerySet[PeakGroup])
        Exceptions:
            None
        Returns:
            rows (Tuple[str, File])
        """
        seen = {}
        pgrec: PeakGroup
        for pgrec in qs.all():
            msrsrec: MSRunSample
            for msrsrec in pgrec.msrun_sample.sample.msrun_samples.all():
                if msrsrec.ms_data_file is not None and msrsrec.id not in seen.keys():
                    yield self.msrun_sample_rec_to_file(msrsrec)
                    seen[msrsrec.id] = True


class PeakDataToMzxmlZIP(RecordToMzxmlZIP):
    """This class defines how to convert a PeakDataFormat class's queryset record to a list of lists (1 PeakData record
    can link to 0 or more mzXML files)."""

    # This format ID matches PeakDataFormat.id
    format_id = "pdtemplate"

    def queryset_to_files_iterator(self, qs):
        """Takes a queryset of PeakData records and returns a list of tuples of file data.

        Args:
            qs (QuerySet[PeakData])
        Exceptions:
            None
        Returns:
            rows (Tuple[str, File])
        """
        seen = {}
        pgrec: PeakData
        for pgrec in qs.all():
            msrsrec: MSRunSample
            for msrsrec in pgrec.peak_group.msrun_sample.sample.msrun_samples.all():
                if msrsrec.ms_data_file is not None and msrsrec.id not in seen.keys():
                    yield self.msrun_sample_rec_to_file(msrsrec)
                    seen[msrsrec.id] = True


class AdvancedSearchDownloadMzxmlZIPView(AdvancedSearchDownloadView):
    """This is a secondary download view for the advanced search page.

    This view is for a subset of SearchGroup formats (those that include mz data files in their results:
    PeakGroupsFormat and PeakDataFormat).  It is for streaming a download of the mzXML files in a zip archive.
    """

    content_type = "application/zip"

    def form_valid(self, form):
        # Get the query object (for the commented metadata header)
        self.qry = self.get_qry(form.cleaned_data)
        # Get the date (for the commented metadata header)
        now = datetime.now()
        self.date_str = now.strftime(self.date_format)
        self.datestamp_str = now.strftime(self.datestamp_format)

        # Get the search format ID and name
        self.format_id = self.qry["selectedtemplate"]
        self.format_name = self.qry["searches"][self.format_id]["name"]

        # Perform the query and save the queryset
        self.res = list(self.get_query_results(self.qry))[0]

        # Set the output file name
        self.filename = f"{self.format_name}_mzxmls_{self.datestamp_str}.zip"
        # Get the object that converts a record from the results queryset to a list of file tuples (containing the
        # export path and a file_location field value from an ArchiveFile record)
        self.converter = RecordToMzxmlZIP.get_converter_object(self.format_id)

        # Generate the content of the metadata file that will accompany the mzXML files in the zip archive.
        self.asdtv = AdvancedSearchDownloadMzxmlTSVView(qry=self.qry, res=self.res)
        # Create a fake buffer object (needed to be able to use the csv package for streaming the tsv)
        pseudo_buffer = Echo()
        metadata_tsv_writer: "_csv._writer" = csv.writer(pseudo_buffer, delimiter="\t")
        metadata_content = "".join(list(self.asdtv.tsv_iterator(metadata_tsv_writer)))

        return StreamingHttpResponse(
            self.mzxml_zip_iterator(metadata_content),
            content_type=self.content_type,
            headers={"Content-Disposition": f"attachment; filename={self.filename}"},
        )

    def mzxml_zip_iterator(self, metadata_content):
        """Builds a zip archive stream that contains all mzXML files from the user's search and includes a metadata file
        describing all of the files.

        Based on: https://stackoverflow.com/a/77515363/2057516

        Args:
            metadata_content (str): The content of the metadata file
        Exceptions:
            None
        Yields:
            files added to the zip archive
        """
        buffer = ZipBuffer()

        # Create a zip file object
        with ZipFile(buffer, "w", ZIP_DEFLATED) as zipf:
            zipf.writestr(self.asdtv.filename, metadata_content)
            yield buffer.take()
            for file_tuple in self.converter.queryset_to_files_iterator(self.res):
                export_path, file_obj = file_tuple
                with file_obj.file.open("r") as fl:
                    zipf.writestr(export_path, fl.read())
                    yield buffer.take()

        yield buffer.end()
