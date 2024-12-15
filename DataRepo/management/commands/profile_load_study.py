import cProfile

from django.core.management import BaseCommand, call_command

from DataRepo.management.commands.load_study import Command as StudyCommand


class Command(BaseCommand):
    # Show this when the user types help
    help = "Profiles the StudyLoader"

    def add_arguments(self, parser):
        sc = StudyCommand()
        sc.add_arguments(parser)

    def handle(self, **options):
        p = cProfile.Profile()
        p.runcall(call_command, "load_study", **options)
        p.print_stats()
