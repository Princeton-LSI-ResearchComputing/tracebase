import re

from django.db import migrations

# Matches 25mins, 25min, 25minutes, 25minute, 25-mins, ...
RUNLEN_PAT = re.compile(r"(?P<mins_digits>\d+)[^0-9a-zA-Z]?(?:min|minute)s?")
HILIC_PAT = re.compile(r"(?i)(?:hilic|lc-ms|^default$)")
REVPHASE_PAT = re.compile(r"(?i)reverse")
RP_LIPID_PAT = re.compile(r"(?i)lipid")
RP_IONPAIR_PAT = re.compile(r"(?i)\bion\b")


def msrunprotocol_name_to_lcmethod_rec(protocol_name, method_map, default_rec):
    # If the name matched a parsable run length
    mins = None
    time_match = re.search(RUNLEN_PAT, protocol_name)
    if time_match is not None:
        # Get the number of minutes for the run length
        mins = int(time_match.group("mins_digits").strip())

    for map_dict in method_map:
        match = True

        # If the minutes are present but do not match, set match = False & break
        if mins is not None and mins != map_dict["MINS_IF_PRESENT"]:
            match = False
            break

        # If any of the matching patterns do not match, set match = False & break
        for match_pat in map_dict["MATCHING_PATS"]:
            if re.search(match_pat, protocol_name) is None:
                match = False
                break
        if match is False:
            break

        # If any of the unmatching patterns do match, set match = False & break
        for non_match_pat in map_dict["UNMATCHING_PATS"]:
            if re.search(non_match_pat, protocol_name) is not None:
                match = False
                break

        # If we get here with match = True, return the corresponding record
        if match is True:
            print(f"{protocol_name} -> {map_dict['LC_REC'].name}")
            return map_dict["LC_REC"]

    # If we get here, return the default record
    print(f"{protocol_name} -> {default_rec.name}")
    return default_rec


def msrunprotocol_to_lcmethod(apps, _):
    # We retrieve the models using an "apps" registry, which contains historical versions of all of the models, because
    # the current version may be newer than this migration expects.
    MSRun = apps.get_model("DataRepo", "MSRun")
    LCMethod = apps.get_model("DataRepo", "LCMethod")

    msr_cnt = MSRun.objects.count()
    lcm_cnt = LCMethod.objects.count()

    if msr_cnt > 0 and lcm_cnt == 0:
        raise MissingLCMethodFixtures(msr_cnt)
    elif msr_cnt > 0 and lcm_cnt > 0:
        method_map = [
            {
                # polar-HILIC-25-min
                "MATCHING_PATS": [HILIC_PAT],
                "UNMATCHING_PATS": [RP_LIPID_PAT, RP_IONPAIR_PAT, REVPHASE_PAT],
                "MINS_IF_PRESENT": 25,
                "LC_REC": LCMethod.objects.get(name__exact="polar-HILIC-25-min"),
            },
            {
                # polar-reversed-phase-25-min
                "MATCHING_PATS": [REVPHASE_PAT],
                "UNMATCHING_PATS": [RP_LIPID_PAT, RP_IONPAIR_PAT, HILIC_PAT],
                "MINS_IF_PRESENT": 25,
                "LC_REC": LCMethod.objects.get(
                    name__exact="polar-reversed-phase-25-min"
                ),
            },
            {
                # lipid-reversed-phase-25-min
                "MATCHING_PATS": [REVPHASE_PAT, RP_LIPID_PAT],
                "UNMATCHING_PATS": [RP_IONPAIR_PAT, HILIC_PAT],
                "MINS_IF_PRESENT": 25,
                "LC_REC": LCMethod.objects.get(
                    name__exact="lipid-reversed-phase-25-min"
                ),
            },
            {
                # polar-reversed-phase-ion-pairing-25-min
                "MATCHING_PATS": [REVPHASE_PAT, RP_IONPAIR_PAT],
                "UNMATCHING_PATS": [RP_LIPID_PAT, HILIC_PAT],
                "MINS_IF_PRESENT": 25,
                "LC_REC": LCMethod.objects.get(
                    name__exact="polar-reversed-phase-ion-pairing-25-min"
                ),
            },
        ]
        # When none of the above match:
        default_lc_method = LCMethod.objects.get(name__exact="unknown")

        for msrun_rec in MSRun.objects.all():
            protocol_name = str(msrun_rec.protocol)
            lcm_rec = msrunprotocol_name_to_lcmethod_rec(
                protocol_name, method_map, default_lc_method
            )

            msrun_rec.lc_method = lcm_rec
            msrun_rec.save()
    # Else - nothing to migrate


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0015_msrun_lc_method"),
    ]

    operations = [
        migrations.RunPython(msrunprotocol_to_lcmethod),
    ]


class MissingLCMethodFixtures(Exception):
    def __init__(self, msr_cnt):
        message = (
            f"There are {msr_cnt} MSRun records with links to Protocol records, but 0 LCMethod records.  LCMethod "
            "records should be loaded using DataRepo/fixtures/lc_methods.yaml before migrations can work."
        )
        super().__init__(message)
