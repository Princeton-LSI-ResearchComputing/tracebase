from csv import DictReader

from django.core.management import BaseCommand

from DataRepo.models import Compound


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from a compound list into the database"

    def add_arguments(self, parser):
        parser.add_argument("compound_list_filename", type=str)

    def handle(self, *args, **options):
        print("Loading compound data")
        for row in DictReader(
            open(options["compound_list_filename"]),
            dialect="excel-tab",
        ):

            compound, created = Compound.objects.get_or_create(name=row["Name"])
            if created:
                print(f"Created new record: Compound:{compound}")
            compound.formula = row["Formula"]
            compound.hmdb_id = row["HMDB_ID"]
            compound.save()
