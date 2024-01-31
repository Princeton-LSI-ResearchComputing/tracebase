from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils import TissuesLoader


class Command(LoadTableCommand):
    help = "Loads data from a tissue table into the database"
    loader_class = TissuesLoader
    default_sheet = "Tissues"

    @LoadTableCommand.handler
    def handle(self, *args, **options):

        self.set_loader(
            TissuesLoader(
                tissues=self.get_dataframe(),
                headers=self.get_headers(),
                defaults=self.get_defaults(),
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
