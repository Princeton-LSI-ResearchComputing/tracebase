import base64
from typing import List
from datetime import datetime
from io import BytesIO, StringIO
import pandas as pd
import csv
import _csv

from django.db.models import Model
from django.template import loader

from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import BSTManyRelatedColumn
from DataRepo.views.models.bst.query import BSTListView, QueryMode


class BSTExportedListView(BSTListView):
    download_header_template_name = "models/bst/download_metadata_header.txt"
    download_header_template = (
        loader.get_template(download_header_template_name)
        if download_header_template_name is not None
        else None
    )

    header_time_format = "%Y-%m-%d %H:%M:%S"
    filename_time_format = "%Y.%m.%d.%H.%M.%S"

    excel_format = "Excel"
    tsv_format = "TSV"
    csv_format = "CSV"
    export_formats = {
        tsv_format: {"type": "text", "extension": "tsv"},
        csv_format: {"type": "text", "extension": "csv"},
        excel_format: {"type": "excel", "extension": "xlsx"},
    }

    # URL Parameter names
    export_param_name = "export"

    # Context variable names
    export_formats_var_name = "export_formats"
    timestamp_var_name = "timestamp"
    export_format_var_name = "export_format"
    export_type_var_name = "export_type"
    export_data_var_name = "export_data"
    export_filename_var_name = "export_filename"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        now = datetime.now()
        self.fileheader_timestamp = now.strftime(self.header_time_format)
        self.filename_timestamp = now.strftime(self.filename_time_format)

    def get_context_data(self, **kwargs):
        context = super().get_context_data()

        export_data, export_filename = self.get_export_data()
        context.update(
            {
                self.export_data_var_name: export_data,
                self.export_filename_var_name: export_filename,
                self.export_format_var_name: self.export_format,
            }
        )
        return context

    def init_interface(self):
        super().init_interface()

        export_format = self.get_param(self.export_param_name)
        if export_format is not None:
            if export_format not in self.export_formats.keys():
                warning = (
                    f"Invalid export type encountered: '{export_format}'.  "
                    f"Must be one of {list(self.export_formats.keys())}.  "
                    "Aborting download."
                )
                self.warnings.append(warning)
            else:
                self.export_format = export_format

    def get_export_data(self):
        """Turns the queryset into a base64 encoded string and a filename.

        Args:
            format (str) {excel, csv, tsv}
        Exceptions:
            NotImplementedError when the format is unrecognized.
        Returns:
            export_data (str): Base64 encoded string.
            (str): The filename
        """
        file_extension = self.export_formats[self.export_format]["extension"]
        file_type = self.export_formats[self.export_format]["type"]
        if self.export_format == self.excel_format:
            byte_stream = BytesIO()
            self.get_excel_streamer(byte_stream)
            export_data = base64.b64encode(byte_stream.read()).decode("utf-8")
        else:
            buffer = StringIO()
            if self.export_format == self.csv_format:
                # This fills the buffer
                self.get_text_streamer(buffer, delim=",")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            elif self.export_format == self.tsv_format:
                # This fills the buffer
                self.get_text_streamer(buffer, delim="\t")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            else:
                raise NotImplementedError(f"Export format {self.export_format} not supported.")

        return export_data, f"{type(self.model).__name__}.{self.filename_timestamp}.{file_extension}"

    def get_excel_streamer(self, byte_stream: BytesIO):
        """Returns an xlsxwriter for an excel file created from the self.dfs_dict.

        The created writer will output to the supplied stream_obj.

        Args:
            stream_obj (BytesIO)
        Exceptions:
            None
        Returns:
            xlsx_writer (xlsxwriter)
        """
        xlsx_writer = pd.ExcelWriter(  # pylint: disable=abstract-class-instantiated
            byte_stream, engine="xlsxwriter"
        )

        header_context = self.get_header_context()
        sheet = self.model_title_plural
        columns = self.row_headers()

        xlsx_writer.book.set_properties(
            {
                "title": sheet,
                "author": "Robert Leach",
                "company": "Princeton University",
                "comments": self.download_header_template.render(header_context),
            }
        )

        # Build the dict by iterating over the row lists
        qs_dict_by_index = dict((i, []) for i in range(len(columns)))
        for row in self.rows_iterator(headers=False):
            for i, val in enumerate(row):
                qs_dict_by_index[i].append(str(val))

        export_dict = {}
        # Now convert the indexes to the headers
        for i, col in enumerate(columns):
            export_dict[col] = qs_dict_by_index[i]

        # Create a dataframe and add it as an excel object to an xlsx_writer sheet
        pd.DataFrame.from_dict(export_dict).to_excel(
            excel_writer=xlsx_writer,
            sheet_name=sheet,
            columns=columns,
            index=False,
        )
        xlsx_writer.sheets[sheet].autofit()

        xlsx_writer.close()
        # Rewind the buffer so that when it is read(), you won't get an error about opening a zero-length file in Excel
        byte_stream.seek(0)

        return byte_stream

    def get_text_streamer(self, buffer: StringIO, delim: str = "\t"):
        """Fills a supplied buffer with string-cast delimited values from the rows_iterator using a csv writer.

        Args:
            buffer (StringIO)
            delim (str) [\t]: Column delimiter
        Exceptions:
            None
        Returns:
            buffer (StringIO)
        """
        writer: "_csv._writer" = csv.writer(buffer, delimiter=delim)

        # Commented metadata header containing download date and info
        buffer.write(self.download_header_template.render(self.get_header_context()))

        for row in self.rows_iterator():
            writer.writerow([str(c) for c in row])

        return buffer

    def get_header_context(self):
        """Returns a context dict with metadata about the queryset used in the header template.

        Args:
            None
        Exceptions:
            None
        Returns:
            (dict)
        """
        return {
            self.title_var_name: (
                self.model_title_plural if self.title is None else self.title
            ),
            self.timestamp_var_name: self.fileheader_timestamp,
            self.total_var_name: self.total,
            self.search_cookie_name: self.search_term,
            self.columns_var_name: self.columns,
            self.sortcol_cookie_name: self.sort_col,
            self.asc_cookie_name: self.asc,
        }

    def row_headers(self):
        return [col.header for col in self.columns.values() if col.exported]

    def rows_iterator(self, headers=True):
        """Takes a queryset of records and returns a list of lists of column data.  Note that delimited many-related
        values are converted to strings, but everything else in the returned list of lists is the original type.

        Args:
            headers (bool): Whether to include the header row.
        Exceptions:
            None
        Returns:
            (List[list])
        """
        if headers:
            yield self.row_headers()
        rec: Model
        for rec in self.get_queryset():
            yield self.rec_to_row(rec)

    def rec_to_row(self, rec: Model) -> List[str]:
        """Takes a Model record and returns a list of values for a file.

        Args:
            rec (Model)
        Exceptions:
            None
        Returns:
            (List[str])
        """
        return [
            self.get_column_val(rec, col)
            for col in self.columns.values()
            if col.exported
        ]

    def get_column_val(self, rec: Model, col: BSTBaseColumn):
        """Given a model record, i.e. row-data, e.g. from a queryset, and a column, return the column value.

        NOTE: While this supports many-related columns, it is more efficient to call get_many_related_rec_val directly.

        Args:
            rec (Model)
            col (BSTBaseColumn)
        Exceptions:
            ValueError when the BSTColumn is not labeled as many-related.
        Returns:
            (str): Column value or values (if many-related).
        """
        if isinstance(col, BSTManyRelatedColumn) and self.query_mode == QueryMode.subquery:
            return col.delim.join(
                [str(val) for val in self.get_many_related_column_val_by_subquery(rec, col)]
            )
        else:
            if self.query_mode == QueryMode.iterate:
                return str(self.get_column_val_by_iteration(rec, col))
            elif self.query_mode != QueryMode.subquery:
                raise NotImplementedError(f"QueryMode {self.query_mode} not implemented.")
            else:
                raise NotImplementedError(f"QueryMode {self.query_mode} not implemented for {type(col).__name__}.")
