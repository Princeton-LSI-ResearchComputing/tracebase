from DataRepo.management.commands.load_table import LoadFromTableCommand
from DataRepo.models.protocol import Protocol
from DataRepo.utils import ProtocolsLoader
from DataRepo.utils.file_utils import is_excel


class Command(LoadFromTableCommand):
    """Command to load the Protocol model from a table-like file."""

    help = "Loads data from a protocol table into the database"
    loader_class = ProtocolsLoader
    default_sheet = "Treatments"

    # default XLXS template headers
    TREATMENTS_NAME_HEADER = "Animal Treatment"
    TREATMENTS_DESC_HEADER = "Treatment Description"

    def handle(self, *args, **options):
        """Code to run when the command is called from the command line.

        This code is automatically wrapped by LoadFromTableCommand._handler, which handles:
            - DryRun Exceptions
            - Contextualization of exceptions to the associated input in the file
            - Atomic transactions with optionally deferred rollback
            - Header and data type validation
            - Unique file constraints

        Args:
            None

        Raises:
            Nothing (See LoadFromTableCommand._handler for exceptions in the wrapper)

        Returns:
            Nothing
        """
        # Different headers and defaults if an excel file is provided
        if is_excel(self.get_infile()):
            # An excel sheet is assumed to be for treatment protocols
            headers = self.get_headers(
                custom_default_header_data={
                    ProtocolsLoader.NAME_KEY: self.TREATMENTS_NAME_HEADER,
                    ProtocolsLoader.CAT_KEY: None,
                    ProtocolsLoader.DESC_KEY: self.TREATMENTS_DESC_HEADER,
                }
            )
        else:
            headers = self.get_headers()

        # LoadTableCommand uses the loader to track stats per model
        self.set_loader(
            ProtocolsLoader(
                self.get_dataframe(),
                headers=headers,
                defaults=self.get_defaults(),  # category default = animal_treatment
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
