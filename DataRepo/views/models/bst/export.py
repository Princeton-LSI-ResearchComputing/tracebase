import base64
from typing import List
from datetime import datetime
from io import BytesIO, StringIO
import pandas as pd
import csv
import _csv

from django.db.models import Model
from django.template import loader

from DataRepo.views.models.bst.column.annotation import BSTAnnotColumn
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.models.bst.column.many_related_field import BSTManyRelatedColumn
from DataRepo.views.models.bst.column.related_field import BSTRelatedColumn
from DataRepo.views.models.bst.query import BSTListView, QueryMode


class BSTExportedListView(BSTListView):
    export_script_names = ["js/browser_download.js", "js/bst/exporter.js"]

    download_header_template_name = "models/bst/download_metadata_header.txt"
    download_header_template = (
        loader.get_template(download_header_template_name)
        if download_header_template_name is not None
        else None
    )

    header_time_format = "%Y-%m-%d %H:%M:%S"
    filename_time_format = "%Y.%m.%d.%H.%M.%S"

    excel_filetype_name = "Excel"
    tsv_filetype_name = "TSV"
    csv_filetype_name = "CSV"
    export_types = {
        tsv_filetype_name: {"stream_type": "text", "extension": "tsv", "file_type": "text"},
        csv_filetype_name: {"stream_type": "text", "extension": "csv", "file_type": "text"},
        excel_filetype_name: {"stream_type": "binary", "extension": "xlsx", "file_type": "excel"},
    }

    # URL Parameter names
    export_param_name = "export"

    # Context variable names
    export_enabled_var_name = "export_enabled"
    timestamp_var_name = "timestamp"
    export_type_var_name = "export_type"
    export_filename_var_name = "export_filename"
    export_data_var_name = "export_data"
    export_types_var_name = "export_types"
    not_exported_var_name = "not_exported"
    export_filetype_var_name = "file_type"

    def __init__(self, export_enabled = True, **kwargs):
        super().__init__(**kwargs)
        now = datetime.now()
        self.fileheader_timestamp = now.strftime(self.header_time_format)
        self.filename_timestamp = now.strftime(self.filename_time_format)
        self.export_enabled = export_enabled

        # A derived class could set the export script name to None, so check it before adding it to the client interface
        if self.export_script_names:
            for script in reversed(self.export_script_names):
                self.javascripts.insert(0, script)

    def get_context_data(self, **kwargs):
        context = super().get_context_data()

        export_data = None
        export_filename = None
        file_type = None
        export_filters = [
            column.name
            for column in self.columns.values()
            if column.filterable and column.filterer.initial
        ]
        if self.export_type:
            export_data, export_filename, file_type = self.get_export_data()

        export_types = list(self.export_types.keys())

        # not_exported is for BST builtin export functionality.  It is data for a fallback method when the javascript
        # customButtonsFunction's btnExportAll is not defined.
        not_exported = [column.name for column in self.columns.values() if not column.exported]

        context.update(
            {
                self.export_enabled_var_name: self.export_enabled,
                self.export_types_var_name: export_types,
                self.export_type_var_name: self.export_type,
                self.export_data_var_name: export_data,
                "export_data_var_name": self.export_data_var_name,
                self.export_filename_var_name: export_filename,
                self.export_type_var_name: self.export_type,
                "export_param_name": self.export_param_name,
                self.not_exported_var_name: not_exported,
                self.export_filetype_var_name: file_type,
                "export_filetype_var_name": self.export_filetype_var_name,
                "export_filters": export_filters,
            }
        )
        return context

    def get(self, request, *args, **kwargs):
        self.init_interface()
        return super().get(request, *args, **kwargs)

    def init_interface(self):
        super().init_interface()

        self.export_type = self.get_param(self.export_param_name)
        if self.export_type is not None:
            if self.export_type not in self.export_types.keys():
                warning = (
                    f"Invalid export type encountered: '{self.export_type}'.  "
                    f"Must be one of {list(self.export_types.keys())}.  "
                    "Aborting download."
                )
                self.warnings.append(warning)

    def get_export_data(self):
        """Turns the queryset into a base64 encoded string and a filename.

        Args:
            type (str) {excel, csv, tsv}
        Exceptions:
            NotImplementedError when the type is unrecognized.
        Returns:
            export_data (str): Base64 encoded string.
            (str): The filename
            data_type (str) {text, excel}: This is the value used by the browserDownloadBase64 javascript method to set
                the data attribute that instructs the browser how to create the file.
        """
        if self.export_type not in self.export_types.keys():
            raise NotImplementedError(
                f"Export type {self.export_type} not supported.  "
                f"Must be one of {list(self.export_types.keys())}."
            )

        file_extension = self.export_types[self.export_type]["extension"]
        stream_type = self.export_types[self.export_type]["stream_type"]
        data_type = self.export_types[self.export_type]["file_type"]

        if stream_type == "binary":
            byte_stream = BytesIO()
            if self.export_type == self.excel_filetype_name:
                self.get_excel_streamer(byte_stream)
                export_data = base64.b64encode(byte_stream.read()).decode("utf-8")
        elif stream_type == "text":
            buffer = StringIO()
            if self.export_type == self.csv_filetype_name:
                # This fills the buffer
                self.get_text_streamer(buffer, delim=",")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
            elif self.export_type == self.tsv_filetype_name:
                # This fills the buffer
                self.get_text_streamer(buffer, delim="\t")
                # This consumes the buffer
                export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
                # export_data = base64.b64encode(buffer.getvalue().encode('utf-8')).decode("utf-8")
        else:
            supported_types = []
            for typ_dct in self.export_types.values():
                if typ_dct["stream_type"] not in supported_types:
                    supported_types.append(typ_dct["stream_type"])
            raise NotImplementedError(f"File type {stream_type} not supported.  Must be one of {supported_types}.")

        return export_data, f"{self.model.__name__}.{self.filename_timestamp}.{file_extension}", data_type

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
        # Determine the method that will be used to retrieve the column value
        if self.query_mode == QueryMode.subquery:
            method = self.get_many_related_column_val_by_subquery
        elif self.query_mode == QueryMode.iterate:
            method = self.get_column_val_by_iteration
        else:
            raise NotImplementedError(f"QueryMode {self.query_mode} not implemented.")

        # Many-related columns are handled a bit differently.  Their values must be joined using the column's delimiter.
        if isinstance(col, BSTManyRelatedColumn):
            return col.delim.join(
                [
                    # If the value is a foreign key/model object, get the display field defined in the column object.
                    getattr(val, col.display_field_name)
                    if (
                        isinstance(val, Model)
                        and col.display_field_name
                        and col.display_field_path != col.display_field_name
                    )
                    # Else just return the stringified value
                    else str(val)
                    for val in method(rec, col)
                ]
            )
        else:
            # NOTE: QueryMode.subquery is only for BSTManyRelatedColumn objects, so here, we only call
            # get_column_val_by_iteration
            val = self.get_column_val_by_iteration(rec, col)
            if (
                isinstance(val, Model)
                and isinstance(col, BSTRelatedColumn)
                and col.display_field_name
                and col.display_field_path != col.display_field_name
            ):
                # If the value is a foreign key/model object (managed by a BSTRelatedColumn), get the display field
                # defined in the column object.
                return getattr(val, col.display_field_name)
            elif (
                isinstance(val, Model)
                and isinstance(col, BSTAnnotColumn)
                and col.is_fk
            ):
                # If the value is a foreign key/model object (managed by a BSTAnnotColumn), convert the ID the
                # annotation generates to a model object.  We will stringify the object so that it makes some sense
                # to the user, since the integer value of the foreign key is meaningless to the user.  NOTE: There
                # does not yet exist a display field name for annotations that return foreign keys.
                # TODO: Add a display_field_name to BSTAnnotColumn for annotations that return foreign keys
                # TODO: Handle the case where an annotation returns a many-related list of values (not yet
                # supported: so add a raise/exception until it **is** supported)
                return str(col.get_model_object(val))
            else:
                return str(val)
