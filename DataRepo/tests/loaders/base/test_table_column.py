from collections import namedtuple

from django.db.models import AutoField, CharField, Model
from django.test.utils import isolate_apps

from DataRepo.loaders.base.table_column import (
    ColumnHeader,
    ColumnReference,
    ColumnValue,
    TableColumn,
    make_title,
)
from DataRepo.loaders.base.table_loader import TableLoader
from DataRepo.tests.tracebase_test_case import TracebaseTestCase
from DataRepo.utils.exceptions import (
    ConditionallyRequiredOptions,
    MutuallyExclusiveOptions,
)


# Class (Model) used for testing
class TestTableColumnModel(Model):
    id = AutoField(primary_key=True)
    name = CharField(unique=True, help_text="This be the name.")
    choice = CharField(choices=[("1", "1"), ("2", "2")])

    # Necessary for temporary models
    class Meta:
        app_label = "loader"


class TestLoader(TableLoader):
    DataSheetName = "test"
    DataTableHeaders = namedtuple("DataTableHeaders", ["NAME", "CHOICE"])
    DataHeaders = DataTableHeaders(NAME="NOT Loader Name Header", CHOICE="Choice")
    DataRequiredHeaders = ["NAME"]
    DataRequiredValues = DataRequiredHeaders
    DataUniqueColumnConstraints = [["NAME"]]
    FieldToDataHeaderKey = {
        TestTableColumnModel.__name__: {"name": "NAME", "choice": "CHOICE"}
    }
    DataColumnMetadata = DataTableHeaders(
        NAME=TableColumn.init_flat(name="Loader Name Header"),
        CHOICE=TableColumn.init_flat(field=TestTableColumnModel.choice),
    )
    Models = [TestTableColumnModel]

    def load_data(self):
        return None


class ColumnReferenceTests(TracebaseTestCase):
    def test_ColumnReference(self):
        # Test no args
        with self.assertRaises(ConditionallyRequiredOptions):
            ColumnReference()

        # Basic case, no loader, success
        cr = ColumnReference(header="Name", sheet="Sheet1")
        self.assertEqual("Name", cr.header)
        self.assertEqual("Sheet1", cr.sheet)

        # Test loader arg only
        with self.assertRaises(ConditionallyRequiredOptions):
            ColumnReference(loader_class=TestLoader)

        # test loader with header name case
        crl = ColumnReference(loader_class=TestLoader, header="Loader Name Header")
        self.assertEqual("Loader Name Header", crl.header)
        self.assertEqual("test", crl.sheet)

        # test loader with header key case
        crlk = ColumnReference(loader_class=TestLoader, loader_header_key="NAME")
        self.assertEqual("Loader Name Header", crlk.header)
        self.assertEqual("test", crlk.sheet)

        # Test mutex errors
        with self.assertRaises(MutuallyExclusiveOptions):
            ColumnReference(loader_class=TestLoader, sheet="Sheet1")
        with self.assertRaises(MutuallyExclusiveOptions):
            ColumnReference(
                loader_class=TestLoader, header="Name", loader_header_key="NAME"
            )


class ColumnHeaderTests(TracebaseTestCase):
    def test_ColumnHeader(self):
        chn = ColumnHeader(name="Test header")
        self.assertEqual("Test header", chn.name)
        chf = ColumnHeader(field=TestTableColumnModel.name)
        self.assertEqual("TestTableColumnModel Name", chf.name)
        chfn = ColumnHeader(
            field=TestTableColumnModel.name, include_model_in_header=False
        )
        self.assertEqual("Name", chfn.name)

    def test_ColumnHeader_comment(self):
        chf = ColumnHeader(field=TestTableColumnModel.name)
        self.assertEqual("This be the name.\n\nMust be unique.", chf.comment)


class ColumnValueTests(TracebaseTestCase):
    def test_ColumnValue(self):
        cv = ColumnValue()
        self.assertTrue(cv.required)
        cvf = ColumnValue(field=TestTableColumnModel.choice)
        self.assertEqual([("1", "1"), ("2", "2")], cvf.static_choices)


@isolate_apps("DataRepo.tests.apps.loader")
class TableColumnTests(TracebaseTestCase):
    def test_TableColumn(self):
        tch = TableColumn(
            header=ColumnHeader(name="Test header"),
        )
        self.assertTrue(isinstance(tch, TableColumn))
        tcf = TableColumn(
            field=TestTableColumnModel.name,
        )
        self.assertTrue(isinstance(tcf, TableColumn))
        tchr = TableColumn(
            header=ColumnHeader(name="Test header"),
            readonly=True,
        )
        self.assertTrue(isinstance(tchr, TableColumn))
        with self.assertRaises(ConditionallyRequiredOptions):
            TableColumn()

    def test_init_flat(self):
        tcn = TableColumn.init_flat(
            field=TestTableColumnModel.choice, name="Test header"
        )
        self.assertTrue(isinstance(tcn, TableColumn))
        self.assertEqual("Test header", tcn.header.name)
        self.assertEqual([("1", "1"), ("2", "2")], tcn.value.static_choices)


class TableColumnUtilityMethodsTests(TracebaseTestCase):
    def test_make_title(self):
        self.assertEqual("CamelCase Test", make_title("CamelCase", "test"))
        self.assertEqual("Uppercase Test", make_title("UPPERCASE", "test"))
        self.assertEqual("Lowercasetest", make_title("lowercasetest"))
        self.assertEqual("This Is a Test", make_title("this is a test"))
