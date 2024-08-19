from django.core.management import BaseCommand
from django.db import transaction
from django.forms import model_to_dict

from DataRepo.models import MaintainedModel, MSRunSample, PeakGroup
from DataRepo.models.archive_file import ArchiveFile
from DataRepo.models.hier_cached_model import (
    delete_all_caches,
    disable_caching_updates,
    enable_caching_updates,
)
from DataRepo.utils.exceptions import DryRun


class Command(BaseCommand):
    # Show this when the user types help
    help = "This is a one-off script to migrate the rabinowitz data and remove multiple representations."

    # This holds the peak group names and the peak annotation files that identify which peak groups are to be deleted
    # among the subset of peak groups that are duplicated in the same sequence and sample.  (In other words, this isn't
    # *totally* identifying.  This represents JUST enough information after having found multiple representations.)
    todelete = {
        "3-Ureidopropionic acid": [
            "exp048a_BetaalaHist_pos_highmz_cor_ion_counts.csv",
            "exp048a_Carn_pos_highmz_corrected.xlsx",
        ],
        "carnosine": ["exp048a_Carn_pos_highmz_corrected.xlsx"],
        "creatine": ["exp048a_BetaalaHist_negative_cor_ion_counts.csv"],
        "cytidine": [
            "exp048a_BetaalaHist_negative_cor_ion_counts.csv",
            "exp048a_Carn_neg_corrected.xlsx",
        ],
        "thymidine": [
            "exp048a_BetaalaHist_negative_cor_ion_counts.csv",
            "exp048a_Carn_neg_corrected.xlsx",
        ],
        "arginine": ["exp027f4_free_plasma and tissues_negative_corrected.xlsx"],
        "lysine": ["exp027f4_free_plasma and tissues_negative_corrected.xlsx"],
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="If supplied, nothing will be saved to the database.",
        )

    def handle(self, *args, **options):
        try:
            self.clean_reps(options["dry_run"])
        except DryRun:
            print("Dry run completed successfully")

    @transaction.atomic
    @MaintainedModel.defer_autoupdates(
        pre_mass_update_func=disable_caching_updates,
        post_mass_update_func=enable_caching_updates,
    )
    def clean_reps(self, dry_run):
        disable_caching_updates()
        dupes = self.find_multiple_representations()
        msrs_peakgroup_migrations = self.delete_duplicate_peak_groups(dupes)
        self.migrate_peakgroups(msrs_peakgroup_migrations)
        enable_caching_updates()
        self.check_fake_mzxmls()
        if dry_run:
            raise DryRun()
        delete_all_caches()

    def find_multiple_representations(self):
        """Finds all of the compunds(/peak groups) that came from more than 1 file and the same sequence and sample.

        We know from manual curation that we will get the compounds and peak annotation files defined in cls.todelete.
        There are 2 each time.  One will be deleted and the other will take all the remaining non-duplicate peak groups
        from the one that had the deleted duplicate."""

        # Get all the peak groups
        pgs = PeakGroup.objects.all()

        # Record all peak groups for the same compound, sample, and sequence
        pgdict = {}
        for pg in pgs:
            pgk = f"{pg.name} {pg.msrun_sample.sample} {pg.msrun_sample.msrun_sequence}"
            if pgk not in pgdict.keys():
                pgdict[pgk] = []
            pgdict[pgk].append(pg)

        # Filter for just those compounds that are in multiple peak groups
        dupes = []
        for dpg_list in [dpg_list for dpg_list in pgdict.values() if len(dpg_list) > 1]:
            dupes.append(dpg_list)

        # Return the list of lists of duplicate peak groups
        return dupes

    @transaction.atomic
    def delete_duplicate_peak_groups(self, dupes):
        """This deletes the peak groups from cls.todelete, among the multiple representations.

        Note, there are 2 peak groups in every list of duplicates.  This was manually determined.

        We go through each pair and delete one.  The other will become the placeholder record for all remaining peak
        groups after the multiple representation deletion, and a dict of the MSRunSample IDs is returned.  The key is
        the ID of the one that will be deleted and the value is the ID of the one that will be taking on its sublings'
        peak groups.

        Originally, these groups were separated by polarity, but without an mzXML file, the new schema requirement is
        that there can only be 1 placeholder record, hence the moving of the peak groups.
        """
        msrs_peakgroup_migrations = {}
        for dupe_peakgroup_list in dupes:

            msrs_id_to_delete = None
            msrs_id_to_keep = None

            for dupe_peakgroup in dupe_peakgroup_list:
                if (
                    dupe_peakgroup.name in self.todelete.keys()
                    and dupe_peakgroup.peak_annotation_file.filename
                    in self.todelete[dupe_peakgroup.name]
                ):
                    msrs_id_to_delete = dupe_peakgroup.msrun_sample.id
                    print(
                        f"Deleting duplicate PeakGroup {model_to_dict(dupe_peakgroup)}"
                    )
                    dupe_peakgroup.delete()  # Deleting duplicate compound PeakGroup
                else:
                    print(
                        f"Keeping 1 representative PeakGroup {model_to_dict(dupe_peakgroup)}"
                    )
                    msrs_id_to_keep = dupe_peakgroup.msrun_sample.id

            msrs_peakgroup_migrations[msrs_id_to_delete] = msrs_id_to_keep

        return msrs_peakgroup_migrations

    @transaction.atomic
    def migrate_peakgroups(self, msrs_peakgroup_migrations):
        """This moves peak groups from one arbitrary MSRunSample record to another (because only 1 placeholder record is
        allowed), then deleted the emptied MSRunSample record."""

        for msrs_giver_id, msrs_taker_id in msrs_peakgroup_migrations.items():

            # Get the MSRunSample records involved
            msrs_giver = MSRunSample.objects.get(id=msrs_giver_id)
            msrs_taker = MSRunSample.objects.get(id=msrs_taker_id)

            pg: PeakGroup
            for pg in msrs_giver.peak_groups.all():
                print(
                    f"Moving remaining PeakGroup:\n\t{model_to_dict(pg)}\n"
                    f"to MSRunSample Placeholder:\n\t{model_to_dict(msrs_taker)}\n"
                )
                pg.msrun_sample = (
                    msrs_taker  # Moving PeakGroup to new Placeholder MSRunSample
                )
                pg.save()

            print(
                f"Deleting duplicate MSRunSample Placeholder:\n\t{model_to_dict(msrs_giver)}\n"
            )
            msrs_giver.delete()  # Deleting MSRunSample record that was emptied of PeakGroups

            print(
                f"Deleting Fake ArchiveFile:\n\t{model_to_dict(msrs_taker.ms_data_file)}\n"
            )
            msrs_taker.ms_data_file.delete()  # Deleting fake mzXML ArchiveFile record

            print(
                f"Saving 1 representative MSRunSample Placeholder:\n\t{model_to_dict(msrs_giver)}\n"
            )
            msrs_taker.ms_data_file = (
                None  # Deleting fake mzXML from MSRunSample record
            )
            msrs_taker.save()

    def check_fake_mzxmls(self):
        """Raise if there are any overlooked Fake mzXML files.  Fake files contain the substring "Dupe"."""
        fake_msrs = MSRunSample.objects.filter(ms_data_file__filename__icontains="Dupe")
        msg = f"{fake_msrs.count()} MSRunSample records with fake mzXML files remain.\n"
        for msrs in fake_msrs.all():
            msg += f"\t{model_to_dict(msrs)}\n"

        fake_mzxmls = ArchiveFile.objects.filter(filename__icontains="Dupe")
        msg += (
            f"{fake_mzxmls.count()} ArchiveFile records with fake mzXML files remain."
        )
        for mzxml in fake_mzxmls.all():
            msg += f"\t{model_to_dict(mzxml)}\n"

        if fake_msrs.count() > 0 or fake_mzxmls.count() > 0:
            raise ValueError(msg)

        print(msg)
