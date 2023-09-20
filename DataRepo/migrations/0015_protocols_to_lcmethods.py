import re
from datetime import timedelta

from django.db import migrations

# Matches 25mins, 25min, 25minutes, 25minute, 25-mins, ...
RUNLEN_PAT = re.compile(
    r"(?P<mins_digits>\d+)[^0-9a-zA-Z]?(?:min|minute)s?"
)
HILIC_PAT = re.compile(
    r"(?i)(?:hilic|lc-ms|^default$)"
)
REVPHASE_PAT = re.compile(
    r"(?i)reverse"
)
RP_LIPID_PAT = re.compile(
    r"(?i)lipid"
)
RP_IONPAIR_PAT = re.compile(
    r"(?i)\bion\b"
)


def msrunprotocol_name_to_lcmethod_name_type_and_runlength(msrp_name):
    # Defaults
    name = None
    type = "unknown"
    runlen = None
    mins = None

    # If the name matched a parsable run length
    time_match = re.search(RUNLEN_PAT, msrp_name)
    if time_match is not None:
        # Get the number of minutes for the run length
        mins = time_match.group("mins_digits").strip()
        runlen = timedelta(minutes=float(mins))

    hilic_match = re.search(HILIC_PAT, msrp_name)
    reverse_match = re.search(REVPHASE_PAT, msrp_name)

    if hilic_match is not None:
        type = "polar-HILIC"
    elif reverse_match is not None:
        lipid_match = re.search(RP_LIPID_PAT, msrp_name)
        ion_match = re.search(RP_IONPAIR_PAT, msrp_name)

        if lipid_match is not None:
            type = "lipid-reversed-phase"
        elif ion_match is not None:
            type = "polar-reversed-phase-ion-pairing"
        else:
            type = "polar-reversed-phase"

    name = type

    if mins is not None:
        name += f"-{mins}-mins"

    return name, type, runlen


def msrunprotocol_to_lcmethod(apps, _):
    # We retrieve the models using an "apps" registry, which contains historical versions of all of the models, because
    # the current version may be newer than this migration expects.
    Protocol = apps.get_model("DataRepo", "Protocol")
    LCMethod = apps.get_model("DataRepo", "LCMethod")

    for msrun_protocol in Protocol.objects.filter(category__exact="msrun_protocol"):
        name = "unknown"
        type = "unknown"
        runlen = None

        if msrun_protocol.name is not None:
            name, type, runlen = msrunprotocol_name_to_lcmethod_name_type_and_runlength(
                msrun_protocol.name
            )

        # We will ignore description
        lc_rec = LCMethod.objects.get(
            name=name,
            type=type,
            run_length=runlen,
        )

        # TODO: There's nothing to do for issue #703.  The records are already created. What needs to change is the
        # links, but the links aren't created until #704.  Once #704 is done, we can set the links to the corresponding
        # LCMethod record.


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0014_lcmethod_fixture_update"),
    ]

    operations = [
        migrations.RunPython(msrunprotocol_to_lcmethod),
    ]
