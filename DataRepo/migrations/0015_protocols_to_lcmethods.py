import re
from datetime import timedelta

from django.db import migrations

# Matches 25mins, 25min, 25minutes, 25minute, 25-mins, ...
RUNLEN_PAT = re.compile(
    r"(?P<delim1>[^0-9a-zA-Z]?)(?P<mins_digits>\d+)[^0-9a-zA-Z]?(?:min|minute)s?(?P<delim2>[^0-9a-zA-Z]?)"
)


def msrunprotocol_name_to_lcmethod_type_and_runlength(msrp_name):
    # Defaults
    type = msrp_name
    runlen = None

    # Parse the msrun protocol name for the run length
    match = re.search(RUNLEN_PAT, msrp_name)

    # If the name matched a parsable run length
    if match is not None:
        # Re-use the delimiter used already in the msrp_name string
        delim = "_"
        delim1 = match.group("delim1")
        delim2 = match.group("delim2")
        if delim1 is not None and len(delim1) > 0:
            delim = delim1
        elif delim2 is not None and len(delim2) > 0:
            delim = delim2

        # Get the number of minutes for the run length
        mins = match.group("mins_digits").strip()
        runlen = timedelta(minutes=float(mins))

        # Remove the run length to create the type
        type = re.sub(RUNLEN_PAT, delim, msrp_name).strip(delim)

    return type, runlen


def msrunprotocol_to_lcmethod(apps, _):
    # We retrieve the models using an "apps" registry, which contains historical versions of all of the models, because
    # the current version may be newer than this migration expects.
    Protocol = apps.get_model("DataRepo", "Protocol")
    LCMethod = apps.get_model("DataRepo", "LCMethod")

    for msrun_protocol in Protocol.objects.filter(category__exact="msrun_protocol"):
        type = "unknown"
        runlen = None

        if msrun_protocol.name is not None:
            type, runlen = msrunprotocol_name_to_lcmethod_type_and_runlength(
                msrun_protocol.name
            )

        lc_rec = LCMethod.objects.create(
            name=msrun_protocol.name,
            description=msrun_protocol.description,
            type=type,
            run_length=runlen,
        )

        lc_rec.save()


class Migration(migrations.Migration):
    dependencies = [
        ("DataRepo", "0014_lcmethod_fixture_update"),
    ]

    operations = [
        migrations.RunPython(msrunprotocol_to_lcmethod),
    ]
