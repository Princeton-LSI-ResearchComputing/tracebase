from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils import StudyTableLoader


class Command(LoadTableCommand):
    help = "Loads data from a study table (e.g. study code, name, and description) into the database."
    loader_class = StudyTableLoader
    default_sheet = "Study"

    @LoadTableCommand.handler
    def handle(self, *args, **options):
        self.set_loader(
            StudyTableLoader(
                # Data
                self.get_dataframe(),
                headers=self.get_headers(),
                defaults=self.get_defaults(),
                # Modes
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                # Used for error-reporting
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
