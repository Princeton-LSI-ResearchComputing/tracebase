from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.models.protocol import Protocol
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.file_utils import is_excel


class Command(LoadTableCommand):
    help = "Loads data from a protocol table into the database"
    loader_class = ProtocolsLoader
    default_sheet = "Treatments"

    # default XLXS template headers
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"
    TREATMENTS_CATEGORY_VALUE = Protocol.ANIMAL_TREATMENT

    @LoadTableCommand.handler
    def handle(self, *args, **options):

        # Different headers and defaults if an excel file is provided
        if is_excel(self.get_infile()):
            headers = self.get_headers(
                custom_default_header_data={
                    ProtocolsLoader.NAME_KEY: self.TREATMENTS_NAME_HEADER,
                    ProtocolsLoader.CAT_KEY: None,
                    ProtocolsLoader.DESC_KEY: self.TREATMENTS_DESC_HEADER,
                }
            )

            defaults = self.get_defaults(
                {ProtocolsLoader.CAT_KEY: self.TREATMENTS_CATEGORY_VALUE}
            )
        else:
            headers = self.get_headers()
            defaults = self.get_defaults()

        # LoadTableCommand uses the loader to track stats per model
        self.set_loader(
            ProtocolsLoader(
                self.get_dataframe(),
                headers=headers,
                defaults=defaults,
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
