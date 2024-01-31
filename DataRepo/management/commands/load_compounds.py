from DataRepo.management.commands.load_table import LoadTableCommand
from DataRepo.utils import CompoundsLoader


class Command(LoadTableCommand):
    help = "Loads data from a compound table into the database"
    loader_class = CompoundsLoader
    default_sheet = "Compounds"

    def add_arguments(self, parser):
        super().add_arguments(parser)

        parser.add_argument(
            "--synonym-separator",
            type=str,
            help="Character separating multiple synonyms in 'Synonyms' column (default '%(default)s')",
            default=";",
            required=False,
        )

    @LoadTableCommand.handler
    def handle(self, *args, **options):
        self.set_loader(
            CompoundsLoader(
                self.get_dataframe(),
                headers=self.get_headers(),
                defaults=self.get_defaults(),
                synonym_separator=options["synonym_separator"],
                dry_run=self.get_dry_run(),
                defer_rollback=self.get_defer_rollback(),
                sheet=self.get_sheet(),
                file=self.get_infile(),
            )
        )

        self.load_data()
