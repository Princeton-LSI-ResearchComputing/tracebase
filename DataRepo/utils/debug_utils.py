from django.core.exceptions import MultipleObjectsReturned
from django.db.models import Count, F, Q
from django.forms.models import model_to_dict

from DataRepo.loaders import MSRunsLoader
from DataRepo.models import (
    MaintainedModel,
    MSRunSample,
    PeakData,
    PeakGroup,
    Sample,
)
from DataRepo.models.hier_cached_model import (
    disable_caching_updates,
    enable_caching_updates,
)


@MaintainedModel.no_autoupdates()
def fix_peak_group_names():
    """Fixes peak group names that did not have spaces stripped between delimiters.

    When a peak contains 2 indistinguishable compounts, e.g. "citrate" and "isocitrate", the peak group name is composed
    of the multiple compound synonym names delimited by "/".  If the user entered for example " citrate / isocitrate ",
    the extra spaces were supposed to have been removed so that it is entered as "citrate/isocitrate", but there was a
    bug for a long time that did not remove the internal spaces, such that the peak group name became "cotrate /
    isocitrate".

    This issue is further complicated by the fact that if a load was ever re-run after the bug-fix, a duplictae record
    will have been created with the extra spaces removed so that a record would exist both with and without the spaces.
    This migration will fix the name and if it is identified as a duplicate, the record will be deleted.

    This method removes those extra spaces.
    """

    # If there's an exception, disabling caching updates only persists for the duration of the
    disable_caching_updates()

    renames = []
    dupe_deletions = []

    for pg in PeakGroup.objects.filter(name__icontains=" / "):
        pg.name = str(pg.name).replace(" / ", "/")
        try:
            pg.full_clean()
            pg.save()
            renames.append(pg)
        except Exception as e:
            _delete_duplicate_peak_group(pg, e)
            dupe_deletions.append(pg)

    enable_caching_updates()

    print(
        f"{len(renames)} PeakGroup names fixed.\n"
        f"{len(dupe_deletions)} PeakGroups deleted due to existing duplicates after rename."
    )


def _delete_duplicate_peak_group(pg, exc):
    """Deletes a PeakGroup after confirming it's a duplicate."""

    if "Peak group with this Name and Msrun sample already exists." in str(exc):
        existing_pg = PeakGroup.objects.get(name=pg.name, msrun_sample=pg.msrun_sample)
        if (
            existing_pg.msrun_sample == pg.msrun_sample
            and existing_pg.formula == pg.formula
            and existing_pg.peak_annotation_file == pg.peak_annotation_file
            and set(existing_pg.compounds.all()) == set(pg.compounds.all())
            and (
                set(pgl.element for pgl in existing_pg.labels.all())
                == set(pgl.element for pgl in pg.labels.all())
            )
        ):
            pg.delete()
        else:
            raise exc
    else:
        raise exc


@MaintainedModel.no_autoupdates()
def fix_sample_names():
    """Fixes sample names by removing scan labels.

    Sample records should represent unique biological samples, but samples used to be loaded verbatim from the peak
    annotation files, of which there can be multiple, corresponding to a scan type.  The scan type (e.g. "pos", "neg",
    "scan2") were appended to the sample name given by the researcher to name the file names of the different scans
    unique.  These scan labels are now automatically removed in the current loading process and MSRunSample records from
    different scans are linked to the same sample.

    This method renames the samples and when a duplicate in encountered, its MSRunSample records are linked to the other
    existing (previously renamed) sample record.

    This method uses the following filters for identifying sample names with scan labels:

    Sample.objects.filter(
        Q(name__endswith="scan1")
        | Q(name__endswith="scan2")
        | Q(name__endswith="scan3")
        | Q(name__iendswith="pos")
        | Q(name__iendswith="neg")
    )

    At the time of creating this method, there have been a max of 3 scans as a practical limit.  Sometimes, scan labels
    can be embedded internally in a sample name, but no instances currently exist and that possibility is ignored by
    this migration.

    See MSRunsLoader.DEFAULT_SCAN_LABEL_PATTERNS for the current versions of "scan", "pos", and "neg".
    """

    # If there's an exception, disabling caching updates only persists for the duration of the
    disable_caching_updates()

    num_samples_renamed = 0
    num_samples_deleted = 0
    tot_num_msrs_transferred = 0
    tot_num_msrs_created = 0
    tot_num_msrs_deleted = 0
    tot_num_pgs_transferred = 0
    tot_num_pgs_deleted = 0

    for rename_sample_rec in Sample.objects.filter(
        Q(name__endswith="scan1")  # pylint: disable=unsupported-binary-operation
        | Q(name__endswith="scan2")  # pylint: disable=unsupported-binary-operation
        | Q(name__endswith="scan3")  # pylint: disable=unsupported-binary-operation
        | Q(name__iendswith="pos")  # pylint: disable=unsupported-binary-operation
        | Q(name__iendswith="neg")  # pylint: disable=unsupported-binary-operation
    ):
        old_sample_name = rename_sample_rec.name
        # Remove the scan label(s) from the sample name
        new_sample_name = MSRunsLoader.guess_sample_name(rename_sample_rec.name)
        if new_sample_name == rename_sample_rec.name:
            raise ValueError(
                f"No scan label could be removed from sample '{rename_sample_rec.name}'."
            )

        # Rename the sample record (this might end up being a duplicate if the study was ever loaded again to add more
        # data)
        rename_sample_rec.name = new_sample_name

        # Now attemot to validate and save the renamed sample record
        try:
            rename_sample_rec.full_clean()
            rename_sample_rec.save()
            num_samples_renamed += 1
        except Exception as e:
            if "Sample with this Name already exists." in str(e):
                # Obtain the existing sample record that already has this name
                existing_sample_rec = Sample.objects.get(name=new_sample_name)

                # If these are the same samples
                if (
                    existing_sample_rec.name == rename_sample_rec.name
                    and existing_sample_rec.date == rename_sample_rec.date
                    and existing_sample_rec.researcher == rename_sample_rec.researcher
                    and existing_sample_rec.animal == rename_sample_rec.animal
                    and existing_sample_rec.tissue == rename_sample_rec.tissue
                    and existing_sample_rec.time_collected
                    == rename_sample_rec.time_collected
                ):
                    (
                        num_msrs_transferred,
                        num_msrs_created,
                        num_msrs_deleted,
                        num_pgs_transferred,
                        num_pgs_deleted,
                    ) = _merge_msrun_samples(existing_sample_rec, rename_sample_rec)
                    rename_sample_rec.delete()
                    tot_num_msrs_transferred += num_msrs_transferred
                    tot_num_msrs_created += num_msrs_created
                    tot_num_msrs_deleted += num_msrs_deleted
                    tot_num_pgs_transferred += num_pgs_transferred
                    tot_num_pgs_deleted += num_pgs_deleted
                    num_samples_deleted += 1
                else:
                    raise ValueError(
                        f"Attempt to rename '{old_sample_name}' to '{new_sample_name}' (to remove the scan label) "
                        "failed because a **different** record already exists by that name."
                    )
            else:
                raise e

    enable_caching_updates()

    print(
        f"{num_samples_renamed} Sample records renamed\n"
        f"{num_samples_deleted} Sample duplicate records deleted after rename\n"
        f"{tot_num_msrs_transferred} MSRunSample records transferred to existing same-named duplicate sample\n"
        f"{tot_num_msrs_created} MSRunSample records created and linked to existing same-named duplicate sample\n"
        f"{tot_num_msrs_deleted} MSRunSample records deleted after Sample rename\n"
        f"{tot_num_pgs_transferred} PeakGroup records transferred to MSRunSample records linked to an existing Sample\n"
        f"{tot_num_pgs_deleted} PeakGroup duplicate records deleted after Sample rename"
    )


def _merge_msrun_samples(keep_sample_rec, delete_sample_rec):
    """Takes 2 sample records (both pre-existing and both the same biological sample: one selected to be kept and the
    other to be treated as "new" (but will be deleted)) and merges their MSRunSample records.  Both the "existing" and
    "new" MSRunSample records can have mzXML files and/or be a placeholder (i.e. not have an mzXML file associated with
    them) and both can have PeakGroup records linked to them.  In every case, the PeakGroup records will need to be
    consolidated in one placeholder MSRunSample record.
    """

    num_msrs_transferred = 0
    num_msrs_created = 0
    num_msrs_deleted = 0

    if keep_sample_rec.msrun_samples.filter(ms_data_file__isnull=True).exists():
        # The existing Sample record keeps its existing placeholder MSRunSample record
        keep_placeholder_msrs = keep_sample_rec.msrun_samples.get(
            ms_data_file__isnull=True
        )
    elif delete_sample_rec.msrun_samples.filter(ms_data_file__isnull=True).exists():
        try:
            # The existing Sample record gets a new placeholder record from the delete MSRunSample record
            keep_placeholder_msrs = delete_sample_rec.msrun_samples.get(
                ms_data_file__isnull=True
            )
            keep_placeholder_msrs.sample = keep_sample_rec
            keep_placeholder_msrs.full_clean()
            keep_placeholder_msrs.save()
            num_msrs_transferred += 1
        except MultipleObjectsReturned as mor:
            placeholder_recs = [
                str(model_to_dict(r))
                + " with "
                + str(r.peak_groups.count())
                + " peak groups"
                for r in delete_sample_rec.msrun_samples.filter(
                    ms_data_file__isnull=True
                )
            ]
            print(
                f"Sample to delete {model_to_dict(delete_sample_rec)} (keep: {model_to_dict(keep_sample_rec)}) has "
                f"multiple placeholder MSRunSample records: {placeholder_recs}"
            )
            raise mor
    elif (
        keep_sample_rec.msrun_samples.filter(ms_data_file__isnull=False).exists()
        and delete_sample_rec.msrun_samples.filter(ms_data_file__isnull=False).exists()
    ):
        # The existing Sample record gets a new placeholder record created from scratch
        any_msrs = keep_sample_rec.msrun_samples.filter(
            ms_data_file__isnull=False
        ).first()
        keep_placeholder_msrs = MSRunSample.objects.create(
            msrun_sequence=any_msrs.msrun_sequence,
            sample=any_msrs.sample,
        )
        keep_placeholder_msrs.full_clean()
        keep_placeholder_msrs.save()
        num_msrs_created += 1
    elif keep_sample_rec.msrun_samples.filter(ms_data_file__isnull=False).exists():
        # The existing Sample record keeps its existing concrete MSRunSample record & we're done.  There's nothing to
        # do because the delete sample record has no MSRunSample records.
        return 0, 0, 0, 0, 0
    elif delete_sample_rec.msrun_samples.filter(ms_data_file__isnull=False).exists():
        # The existing Sample record gets the delete sample record's concrete MSRunSample record & we're done.  There's
        # nothing else to do because there should be only 1 concrete record when a placeholder does not exist, based on
        # current business rules, and any PeakGroups that link to it do not need to be transferred (because the
        # MSRunSample record was transferred).
        # NOTE: If this gets a MultipleObjectsReturned error, something is wrong with the state of the DB and this
        # migration isn't intended to fix that.
        keep_placeholder_msrs = delete_sample_rec.msrun_samples.get(
            ms_data_file__isnull=False
        )
        keep_placeholder_msrs.sample = keep_sample_rec
        keep_placeholder_msrs.full_clean()
        keep_placeholder_msrs.save()
        return 0, 1, 0, 0, 0
    else:
        # There are no MSRunSample records (to keep or delete), so there's nothing to do.
        return 0, 0, 0, 0, 0

    # Now transfer PeakGroup records from the delete sample record's MSRunSample records to the keep sample record's
    # Placeholder MSRunSample record.
    tot_num_pgs_transferred = 0
    tot_num_pgs_deleted = 0
    for delete_msrs in delete_sample_rec.msrun_samples.all():
        if keep_placeholder_msrs.id != delete_msrs.id:
            num_transferred, num_deleted = _transfer_peak_groups_to_placeholder(
                keep_placeholder_msrs, delete_msrs
            )
            tot_num_pgs_transferred += num_transferred
            tot_num_pgs_deleted += num_deleted
            delete_msrs.delete()
            num_msrs_deleted += 1

    return (
        num_msrs_transferred,
        num_msrs_created,
        num_msrs_deleted,
        tot_num_pgs_transferred,
        tot_num_pgs_deleted,
    )


def _transfer_peak_groups_to_placeholder(placeholder_msrs, source_msrs):
    num_transferred = 0
    num_deleted = 0

    for peak_group in source_msrs.peak_groups.all():
        peak_group.msrun_sample = placeholder_msrs
        try:
            peak_group.full_clean()
            peak_group.save()
            num_transferred += 1
        except Exception as e:
            _delete_duplicate_peak_group(peak_group, e)
            num_deleted += 1

    placeholder_msrs.full_clean()
    placeholder_msrs.save()

    return num_transferred, num_deleted


@MaintainedModel.defer_autoupdates(
    pre_mass_update_func=disable_caching_updates,
    post_mass_update_func=enable_caching_updates,
)
def delete_duplicate_peak_groups():
    """Deletes duplicate PeakGroup records that exist linked to concrete MSRunSample records.

    The MSRunsLoader, when presented with additional data, is supposed to move PeakGroup records that were linked to a
    concrete MSRunSample record (i.e. MSRunSample records that link to an mzXML file) to a new placeholder MSRunSample
    record (i.e. an MSRunSample record that has no mzXML file linked) when peaks from another file are loaded, but
    apparently it is not doing that completely.  It is leaving some PeakGroup records un-deleted after the move (/copy).

    This method finds these duplicate PeakGroup records, confirms that one of the duplicates is linked to a placeholder
    MSRunSample record, and deletes those linked to concrete MSRunSample records.
    """

    # If there's an exception, disabling caching updates only persists for the duration of the run
    disable_caching_updates()

    pg_dupes = (
        PeakGroup.objects.values("name", "msrun_sample__sample__id")
        .annotate(pgcount=Count("id"))
        .filter(pgcount__gt=1)
    )

    num_peakgroups_deleted = 0
    seen = {}

    for pg in PeakGroup.objects.filter(
        name__in=[d["name"] for d in pg_dupes],
        msrun_sample__sample__id__in=[d["msrun_sample__sample__id"] for d in pg_dupes],
    ):
        if pg.id in seen.keys():
            continue

        # If one of the duplicates is a placeholder MSRunSample record
        if PeakGroup.objects.filter(
            name=pg.name,
            msrun_sample__sample__id=pg.msrun_sample.sample.id,
            msrun_sample__ms_data_file__isnull=True,
        ).exists():

            seen[
                PeakGroup.objects.get(
                    name=pg.name,
                    msrun_sample__sample__id=pg.msrun_sample.sample.id,
                    msrun_sample__ms_data_file__isnull=True,
                ).id
            ] = 1

            # Delete the others
            for concrete_pg in PeakGroup.objects.filter(
                name=pg.name,
                msrun_sample__sample__id=pg.msrun_sample.sample.id,
                msrun_sample__ms_data_file__isnull=False,
            ):
                seen[concrete_pg.id] = 1
                concrete_pg.delete()
                num_peakgroups_deleted += 1
        else:
            raise ValueError(
                f"Multiple representations of peakgroup '{pg.name}' with no placeholder, belonging to sample "
                f"'{pg.msrun_sample.sample}' exists.  (This should not be possible if the loading code was used.)"
            )

    enable_caching_updates()

    print(f"{num_peakgroups_deleted} PeakGroup duplicate records deleted")


def count_peakdata_dupes():
    """This identifies potential duplicate PeakData records."""
    delete_duplicate_peakdata(debug=True)


@MaintainedModel.defer_autoupdates(
    pre_mass_update_func=disable_caching_updates,
    post_mass_update_func=enable_caching_updates,
)
def delete_duplicate_peakdata(debug=True):
    """This migration locates old PeakData records that are incorrect duplicates of newly loaded records.

    At some unidentified point in the past (probably in the old loader code), some PeakData records containing either
    null or 0.0 values for the following PeakData Fields:
        raw_abundance
        med_mz
        med_rt
    were created.

    This affected multiple studies.  Probably, when data was re-loaded to *add* data, a bug that created these
    incorrect records was fixed in the new loading code, which resulted in the correct records being *added* to the
    database.  These records didn't show up in other duplicate searches because the records had different (/correct)
    values for the described fields.

    This function idenifies the pseudo-duplicate records and deletes the incorrect records.

    Note that this deletes the PeakDataLabel records as well.

    The thing that makes this duplication search hard to deal with is that the relationship between
    PeakData:PeakDataLabel is 1:many.  The join would have to be done a variable number of times based on the number of
    labeled elements in the tracer(s) in order to identify true duplicates.
    """

    # If there's an exception, disabling caching updates only persists for the duration of the run
    disable_caching_updates()

    # This identifies potential duplicates.  When looping, there may be records that are not actually duplicated based
    # on the way the actual records are retrieved (using a series of `__in` conditions).
    potential_dupes = (
        PeakData.objects.values(
            "peak_group__id",
            "corrected_abundance",
            "labels__element",
            "labels__count",
            "labels__mass_number",
        )
        .annotate(pdcount=Count("id"))
        .filter(pdcount__gt=1)
    )

    # This counts the number of unique records that are potentially duplicated
    tot = potential_dupes.count()

    # This is so we can iterate over each duplicate record (this could be up to twice the size)
    individual_potential_dupes = PeakData.objects.filter(
        peak_group__id__in=[d["peak_group__id"] for d in potential_dupes],
        corrected_abundance__in=[d["corrected_abundance"] for d in potential_dupes],
        labels__element__in=[d["labels__element"] for d in potential_dupes],
        labels__count__in=[d["labels__count"] for d in potential_dupes],
        labels__mass_number__in=[d["labels__mass_number"] for d in potential_dupes],
    )
    realtot = individual_potential_dupes.count()

    # These track some stats to make sense of the numbers
    seen = {}
    num_peakdata_deletions = 0
    checked = 0
    skipped = 0
    nodupes = 0
    none_or_zero_dupes = 0
    legit_diffs = 0
    spaces = " " * 30

    print(f"{tot} ({realtot}) potential duplicates")

    # Order the records by descending med_mz (so that the deleted record will be the 0.0 med_mz record), but note also
    # that in some cases, med_mz is null, so .desc(nulls_last=True) is used in the sort too.
    for idx, pd in enumerate(
        individual_potential_dupes.order_by(F("med_mz").desc(nulls_last=True))
    ):
        print(f"CHECKING {idx}/{realtot}: {model_to_dict(pd)}{spaces}", end="\r")
        checked += 1

        # Any time we delete a duplicate, that duplicate record in the outer loop will need to be skipped (because it
        # was already dealt with)
        if pd.id in seen.keys():
            skipped += 1
            continue

        # Obtain this case's duplicate records.  This is imprecise.  Multiple distinct PeakData records in a PeakGroup
        # can have a corrected abundance of 0, for example.  We can't limit based on label values in a single query
        # because of the 1:M relationship.  So, we do that below...
        recs = PeakData.objects.filter(
            peak_group__id=pd.peak_group.id,
            corrected_abundance=pd.corrected_abundance,
        )

        # If this is not actually a duplicate, skip it.  This can happen when the legitimate corrected abundance is 0
        # for multiple labeling states.
        if recs.count() < 2:
            nodupes += 1
            continue

        checked += 1
        matching_recs = []
        labels_q = Q()
        for potential_rec in recs:
            # Don't delete one of the duplicates - the first one, which should be the non-null/non-zero (/"correct") one
            if pd.id == potential_rec.id:
                continue

            # Don't equate records where they both have non-zero and non-null med_mz values that differ.  There can be
            # multiple of these given.  E.g. multiple label states with a corrected abundance of 0.
            if (
                (
                    pd.med_mz is not None
                    and potential_rec.med_mz is not None
                    and potential_rec.med_mz != 0.0
                    and pd.med_mz != potential_rec.med_mz
                )
                or (
                    pd.med_rt is not None
                    and potential_rec.med_rt is not None
                    and potential_rec.med_rt != 0.0
                    and pd.med_rt != potential_rec.med_rt
                )
                or (
                    pd.raw_abundance is not None
                    and potential_rec.raw_abundance is not None
                    and potential_rec.raw_abundance != 0.0
                    and pd.raw_abundance != potential_rec.raw_abundance
                )
            ):
                # These records legitimately differ
                legit_diffs += 1
                continue
            elif (
                (
                    pd.med_mz is not None
                    and (potential_rec.med_mz is None or potential_rec.med_mz == 0.0)
                )
                or (
                    pd.med_rt is not None
                    and (potential_rec.med_rt is None or potential_rec.med_rt == 0.0)
                )
                or (
                    pd.raw_abundance is not None
                    and (
                        potential_rec.raw_abundance is None
                        or potential_rec.raw_abundance == 0.0
                    )
                )
            ):
                # Count the duplicates that differ by med_mz, med_rt, or raw_abundance incorrectly being none/zero.
                # We're not treating these as different.  Legacy data had Nones and newly loaded data does not.
                none_or_zero_dupes += 1

            # Now confirm the potential duplicate by matching the labels (their element, count, and mass_number)
            for label in pd.labels.all():
                labels_q |= Q(
                    peak_data=potential_rec,
                    element=label.element,
                    count=label.count,
                    mass_number=label.mass_number,
                )
            matching_labels = potential_rec.labels.filter(labels_q)

            # This uses a frozenset to avoid the "unhashable" error
            if potential_rec.labels.count() == pd.labels.count() and set(
                [
                    frozenset(d.items())
                    for d in potential_rec.labels.values(
                        "element", "count", "mass_number"
                    )
                ]
            ) == set(
                [
                    frozenset(d.items())
                    for d in matching_labels.values("element", "count", "mass_number")
                ]
            ):
                # Mark this duplicate as seen, so that we can skip it in the outer loop.
                seen[potential_rec.id] = 1
                matching_recs.append(potential_rec)

        if len(matching_recs) > 0:
            print(
                f"DUPLICATE: {model_to_dict(pd)} WITH MATCHES "
                f"{len(matching_recs)}: {[model_to_dict(r) for r in matching_recs]} ({pd.labels.count()} LABELS: "
                f"{[model_to_dict(r) for r in pd.labels.all()]}){spaces}"
            )
            num_peakdata_deletions += len(matching_recs)

            if debug is False:
                dupe_pd: PeakData
                for dupe_pd in matching_recs:
                    print(
                        f"DELETING PeakData {dupe_pd.id}: {model_to_dict(dupe_pd)} KEEPING: {model_to_dict(pd)}{spaces}"
                    )
                    dupe_pd.delete()

    if debug:
        print(
            f"\n{num_peakdata_deletions} duplicate PeakData records (and their associated PeakDataLabel records)\n"
            f"\tCHECKED: {checked}\n"
            f"\tSKIPPED: {skipped}\n"
            f"\tSEEN: {len(seen.keys())}\n"
            f"\tNO SIMPLE DUPE: {nodupes}\n"
            f"\tDIFFERED BY med_mz, med_rt, or raw_abundance None VALUES: {none_or_zero_dupes}\n"
            f"\tLEGITIMATE DIFFERENCES: {legit_diffs}"
        )
    else:
        print(
            f"DELETED {num_peakdata_deletions} duplicate PeakData records (and their associated PeakDataLabel records)"
            f"{spaces}\n"
            f"TOTAL: {realtot} CHECKED: {checked} SKIPPED: {skipped} SEEN: {len(seen.keys())} NO SIMPLE DUPE: "
            f"{nodupes} EQUATED PeakData RECS THAT DIFFERED BY med_mz, med_rt, or raw_abundance None VALUES: "
            f"{none_or_zero_dupes} LEGITIMATE DIFFERENCES: {legit_diffs}{spaces}"
        )

    enable_caching_updates()
