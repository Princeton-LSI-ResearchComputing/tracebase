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

    # Regarding no_autoupdates: The call to PeakGroup .delete() causes an auto-update to be buffered for the parent
    # record (MSRunSample), however we are later deleting all of those records manually, so we do not want those to
    # auto-update, as calling save on those later will raise an exception, because the records will not exist.  So since
    # we are manually handling these records, and will touch all of the MSRunSample records anyway, which will buffer
    # autoupdates, we are disabling auto-updates for the PeakGroup deletions in this method (only).
    @transaction.atomic
    @MaintainedModel.no_autoupdates()
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

            dupe_peakgroup: PeakGroup
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

            # Get the MSRunSample records involved.  note, an earlier iteration could have already handled this and
            # deleted the one due for deletion.
            msrs_giver = MSRunSample.objects.filter(id=msrs_giver_id).first()
            msrs_taker = MSRunSample.objects.filter(id=msrs_taker_id).first()

            if (
                msrs_giver is not None
                and msrs_giver.ms_data_file is not None
                and "Dupe" in msrs_giver.ms_data_file.checksum
                and "Dupe" in msrs_giver.ms_data_file.filename
            ):

                # Save the ArchiveFile record (because it won't delete when the record is referenced from
                # MSRunSample)
                fake_mzxml: ArchiveFile = msrs_giver.ms_data_file

                # If the neighbor MSRunSample record also has a fake mzXML, migrate the PeakGroups
                if (
                    msrs_taker is not None
                    and msrs_taker.ms_data_file is not None
                    and "Dupe" in msrs_taker.ms_data_file.checksum
                    and "Dupe" in msrs_taker.ms_data_file.filename
                ):
                    pg: PeakGroup
                    for pg in msrs_giver.peak_groups.all():
                        print(
                            f"Moving remaining PeakGroup:\n\t{model_to_dict(pg)}\n"
                            f"to MSRunSample Placeholder:\n\t{model_to_dict(msrs_taker)}\n"
                        )
                        # Moving PeakGroup to new Placeholder MSRunSample
                        pg.msrun_sample = msrs_taker
                        pg.save()

                    print(
                        f"Deleting duplicate MSRunSample Placeholder:\n\t{model_to_dict(msrs_giver)}\n"
                    )
                    msrs_giver.delete()
                else:
                    # Otherwise, just remove the fake mzXML and keep the peak groups where they are

                    print(
                        f"Saving representative MSRunSample Placeholder:\n\t{model_to_dict(msrs_giver)}\n"
                    )
                    # Remove fake mzXML from MSRunSample record
                    msrs_giver.ms_data_file = None
                    msrs_giver.save()

                print(f"Deleting Fake ArchiveFile:\n\t{model_to_dict(fake_mzxml)}\n")
                fake_mzxml.delete()

            elif msrs_giver is not None and msrs_giver.ms_data_file is not None:
                # The mzXML file was probably added after-the-fact, recently.  Lance DID add some.
                # Added these checks due to the dry-run output showing real mzXML files.
                print(
                    "Keeping MSRunSample and not moving its PeakGroups because it has a real mzXML file:\n"
                    f"\t{model_to_dict(msrs_giver)}\n"
                )

            # If the taker MSRunSample record has a fake mzXML file, remove it.
            if (
                msrs_taker is not None
                and msrs_taker.ms_data_file is not None
                and "Dupe" in msrs_taker.ms_data_file.checksum
                and "Dupe" in msrs_taker.ms_data_file.filename
            ):
                # Save the ArchiveFile record (because it won't delete when the record is referenced from MSRunSample)
                fake_mzxml = msrs_taker.ms_data_file

                print(
                    f"Saving representative MSRunSample Placeholder:\n\t{model_to_dict(msrs_taker)}\n"
                )
                # Remove fake mzXML from MSRunSample record
                msrs_taker.ms_data_file = None
                msrs_taker.save()

                print(f"Deleting Fake ArchiveFile:\n\t{model_to_dict(fake_mzxml)}\n")
                fake_mzxml.delete()
            elif msrs_taker is not None and msrs_taker.ms_data_file is not None:
                # The mzXML file was probably added after-the-fact, recently.  Lance DID add some.
                # Added these checks due to the dry-run output showing real mzXML files.
                print(
                    "Keeping MSRunSample and not taking the other's PeakGroups because this MSRunSample has a real "
                    f"mzXML file:\n\t{model_to_dict(msrs_taker)}\n"
                )

    def check_fake_mzxmls(self):
        """Raise if there are any overlooked Fake mzXML files.  Fake files contain the substring "Dupe"."""
        fake_msrs = MSRunSample.objects.filter(ms_data_file__filename__icontains="Dupe")
        msg = f"{fake_msrs.count()} MSRunSample records with fake mzXML files remain.\n"
        for msrs in fake_msrs.all():
            msg += f"\t{model_to_dict(msrs)}\n"

        fake_mzxmls = ArchiveFile.objects.filter(filename__icontains="Dupe")
        msg += (
            f"{fake_mzxmls.count()} ArchiveFile records with fake mzXML files remain.\n"
        )
        for mzxml in fake_mzxmls.all():
            msg += f"\t{model_to_dict(mzxml)}\n"

        if fake_msrs.count() > 0 or fake_mzxmls.count() > 0:
            raise ValueError(msg)

        print(msg)
