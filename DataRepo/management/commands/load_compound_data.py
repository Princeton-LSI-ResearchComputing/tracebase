from csv import DictReader

from django.core.management import BaseCommand

from DataRepo.models import Compound

ALREDY_LOADED_ERROR_MESSAGE = """
If you need to reload the compound data from the TSV file,
you must delete the data from the Postgres database.
Then, run `python manage.py migrate` for a new empty
database with tables"""


class Command(BaseCommand):
    # Show this when the user types help
    help = "Loads data from TraceBase Augmented Compopund List from jcm.tsv into our Compound mode"

    def handle(self, *args, **options):
        if Compound.objects.exists():
            print("Compound data already loaded...exiting.")
            print(ALREDY_LOADED_ERROR_MESSAGE)
            return
        print("Loading compound data")
        for row in DictReader(
            open("./TraceBase Augmented Compopund List from jcm.tsv"),
            dialect="excel-tab",
        ):
            cpd = Compound()
            cpd.name = row["Name"]
            cpd.formula = row["Formula"]
            cpd.hmdb_id = row["HMDB_ID"]
            cpd.save()
