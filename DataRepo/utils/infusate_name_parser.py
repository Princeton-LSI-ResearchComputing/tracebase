import re
from itertools import zip_longest
from typing import List, Optional, Tuple, TypedDict

from DataRepo.models.element_label import ElementLabel

KNOWN_ISOTOPES = "".join(ElementLabel.labeled_elements_list())

# infusate with a name have the tracer(s) grouped in braces
INFUSATE_ENCODING_PATTERN = re.compile(
    r"^(?:(?P<infusate_name>[^\{\}]*?)\s*\{)?(?P<tracers_string>[^\{\}]*?)\}?$"
)
TRACERS_ENCODING_JOIN = ";"
TRACER_ENCODING_PATTERN = re.compile(
    r"^(?P<compound_name>.*?)-\[(?P<isotopes>[^\[\]]+)\]$"
)
TRACER_WITH_CONC_ENCODING_PATTERN = re.compile(
    r"^(?P<compound_name>.*?)-\[(?P<isotopes>[^\[\]]+)\]\[(?P<concentration>[^\[\]]+)\]$"
)
ISOTOPE_ENCODING_JOIN = ","
ISOTOPE_ENCODING_PATTERN = re.compile(
    r"(?P<all>(?:(?P<positions>[0-9][0-9,]*)-)?(?P<mass_number>[0-9]+)(?P<element>["
    + KNOWN_ISOTOPES
    + r"]{1,2})(?P<count>[0-9]+))($|,)"
)
CONCENTRATIONS_DELIMITER = ";"


class IsotopeData(TypedDict):
    element: str
    mass_number: int
    count: int
    positions: Optional[List[int]]


class TracerData(TypedDict):
    unparsed_string: str
    compound_name: str
    isotopes: List[IsotopeData]


class InfusateTracerData(TypedDict):
    tracer: TracerData
    concentration: Optional[float]


class InfusateData(TypedDict):
    unparsed_string: str
    infusate_name: Optional[str]
    tracers: List[InfusateTracerData]


def parse_group_and_tracer_names(
    infusate_string: str,
) -> Tuple[Optional[str], List[str]]:
    tracer_group_name = None
    tracer_names = []
    match = re.search(INFUSATE_ENCODING_PATTERN, infusate_string)

    if match:
        short_name = match.group("infusate_name")
        if short_name is not None and short_name.strip() != "":
            tracer_group_name = short_name.strip()
        tracer_names = split_encoded_tracers_string(
            match.group("tracers_string").strip()
        )
    else:
        raise InfusateParsingError(
            f"Unable to parse infusate string: [{infusate_string}]"
        )

    return tracer_group_name, tracer_names


def parse_infusate_name(
    infusate_string: str, concentrations: List[int]
) -> InfusateData:
    """
    Takes a complex infusate, coded as a string, and parses it into its optional
    name, lists of tracer(s) and compounds.

    Args:
        infusate_string (string): A string representation of an infusate
        concentrations (:obj:`list` of :obj:`int`): A list of tracer
            concentrations, there must be one per tracer.

    Returns:
        An InfusateData object built using the parsed values

    Raises:
        InfusateParsingError: If unable to properly parse the infusate_string
            and list of concentrations.
    """

    # defaults
    # assume the string lacks the optional name, and it is all tracer encodings
    infusate_string = infusate_string.strip()
    parsed_data: InfusateData = {
        "unparsed_string": infusate_string,
        "infusate_name": None,
        "tracers": list(),
    }

    parsed_data["infusate_name"], tracer_strings = parse_group_and_tracer_names(
        infusate_string
    )

    # If concentrations were supplied, there must be one per tracer
    if len(tracer_strings) != len(concentrations):
        raise InfusateParsingError(
            f"Unable to match {len(tracer_strings)} tracers to {len(concentrations)} concentration values:\n"
            f"\tTracers: {tracer_strings}\n"
            f"\tConcentration values: {concentrations}"
        )
    for tracer_string, concentration in zip_longest(tracer_strings, concentrations):
        infusate_tracer: InfusateTracerData = {
            "tracer": parse_tracer_string(tracer_string),
            "concentration": concentration,
        }
        parsed_data["tracers"].append(infusate_tracer)

    return parsed_data


# TODO: The infusate name (and tracer name, for that matter) employs a significant digits mechanism to make
# concentrations appear nice (instead of, due to floating point precision issues, like "100.0000000000001").  Since the
# names are now used to associate infusates in the infusates sheet with animals using the name, that name has to be
# loaded prior to running the animals loader, and it can differ from what the user entered (e.g. 148.88 ends up putting
# 149 in the name) can potentially cause lookups to fail.  This has been worked around by using a fallback mechanism
# that uses the exact number to query the actual data.  That mechanism could be reliable (I haven't looked in detail at
# the `get_infusate method before writing this TODO), but what would be better is having the means to use the entered
# data in the column to format a name to be used in lookups in the exact same way names are constructed by the model.
# We might also consider increasing the number of significant digits, so that every number manually entered is included
# (aside from trailing zeros), because we could potentially end up in a situation where an entered number (148.5 vs
# 148.9) would end up retrieving the wrong infusate, or end up creating an integrity error.
def parse_infusate_name_with_concs(infusate_string: str) -> InfusateData:
    """Takes a complex infusate, coded as a string, and parses it into its optional tracer group name and lists of
    tracers with concentrations and compounds.

    Args:
        infusate_string (string): A string representation of an infusate
    Exceptions:
        InfusateParsingError: If unable to properly parse the infusate_string and list of concentrations.
    Returns:
        parsed_data (InfusateData): An InfusateData object built using the parsed values.
    """

    # defaults
    # assume the string lacks the optional name, and it is all tracer encodings
    infusate_string = infusate_string.strip()
    parsed_data: InfusateData = {
        "unparsed_string": infusate_string,
        "infusate_name": None,
        "tracers": list(),
    }

    parsed_data["infusate_name"], tracer_strings = parse_group_and_tracer_names(
        infusate_string
    )

    for tracer_string in tracer_strings:
        tracer_data, concentration = parse_tracer_with_conc_string(tracer_string)
        infusate_tracer: InfusateTracerData = {
            "tracer": tracer_data,
            "concentration": concentration,
        }
        parsed_data["tracers"].append(infusate_tracer)

    return parsed_data


def split_encoded_tracers_string(tracers_string: str) -> List[str]:
    tracers = tracers_string.split(TRACERS_ENCODING_JOIN)
    return tracers


def parse_tracer_string(tracer: str) -> TracerData:
    tracer_data: TracerData = {
        "unparsed_string": tracer,
        "compound_name": "",
        "isotopes": list(),
    }
    match = re.search(TRACER_ENCODING_PATTERN, tracer)
    if match:
        tracer_data["compound_name"] = match.group("compound_name").strip()
        tracer_data["isotopes"] = parse_isotope_string(match.group("isotopes").strip())
    else:
        raise TracerParsingError(f'Encoded tracer "{tracer}" cannot be parsed.')

    # Compound names are very permissive, but we should at least make sure a malformed isotope specification didn't
    # bleed into the compound pattern (like you would get if the wrong delimiter was used
    # - see test_malformed_tracer_parsing_with_improper_delimiter)
    imatch = re.search(ISOTOPE_ENCODING_PATTERN, tracer_data["compound_name"])
    if imatch:
        raise TracerParsingError(
            f'Encoded tracer "{tracer}" cannot be parsed.  A compound name cannot contain an isotope encoding string.'
        )

    return tracer_data


def parse_tracer_with_conc_string(tracer_string: str) -> Tuple[TracerData, float]:
    """Takes a complex tracer, coded as a string, containing a concentration, and parses it into its isotope string and
    concentration.

    Args:
        tracer_string (string): A string representation of an infusate
    Exceptions:
        InfusateParsingError: If unable to properly parse the infusate_string and list of concentrations.
    Returns:
        tracer_data (TracerData)
        concentration (float)
    """
    tracer_data: TracerData = {
        "unparsed_string": tracer_string,
        "compound_name": "",
        "isotopes": list(),
    }
    concentration = 0.0
    match = re.search(TRACER_WITH_CONC_ENCODING_PATTERN, tracer_string)
    if match:
        tracer_data["compound_name"] = match.group("compound_name").strip()
        tracer_data["isotopes"] = parse_isotope_string(match.group("isotopes").strip())
        concentration = float(match.group("concentration").strip())
    else:
        raise TracerParsingError(f'Encoded tracer "{tracer_string}" cannot be parsed.')

    # Compound names are very permissive, but we should at least make sure a malformed isotope specification didn't
    # bleed into the compound pattern (like you would get if the wrong delimiter was used
    # - see test_malformed_tracer_parsing_with_improper_delimiter)
    imatch = re.search(ISOTOPE_ENCODING_PATTERN, tracer_data["compound_name"])
    if imatch:
        raise TracerParsingError(
            f'Encoded tracer "{tracer_string}" cannot be parsed.  A compound name cannot contain an isotope encoding '
            "string."
        )

    return tracer_data, concentration


def parse_isotope_string(isotopes_string: str) -> List[IsotopeData]:
    if not isotopes_string:
        raise IsotopeParsingError("parse_isotope_string requires a defined string.")

    isotope_data = list()
    isotopes = re.findall(ISOTOPE_ENCODING_PATTERN, isotopes_string)
    if len(isotopes) < 1:
        raise IsotopeParsingError(
            f"Encoded isotopes: [{isotopes_string}] cannot be parsed."
        )

    parsed_string = None
    for isotope in ISOTOPE_ENCODING_PATTERN.finditer(isotopes_string):
        mass_number = int(isotope.group("mass_number"))
        element = isotope.group("element")
        count = int(isotope.group("count"))
        positions = None
        if isotope.group("positions"):
            positions_str = isotope.group("positions")
            positions = [int(x) for x in positions_str.split(",")]

        if parsed_string is None:
            parsed_string = isotope.group("all")
        else:
            parsed_string += ISOTOPE_ENCODING_JOIN + isotope.group("all")

        isotope_data.append(
            IsotopeData(
                element=element,
                mass_number=mass_number,
                count=count,
                positions=positions,
            )
        )

    if parsed_string != isotopes_string:
        raise IsotopeParsingError(
            f"One or more encoded isotopes in [{isotopes_string}] could not be parsed. Only the following were "
            f"parsed: [{parsed_string}]."
        )

    return isotope_data


def parse_tracer_concentrations(tracer_concs_str: str) -> List[float]:
    try:
        if tracer_concs_str is None:
            tracer_concs = None
        else:
            # Not sure how the split results in a float, but my guess is that it's something in excel, thus
            # if there do exist comma-delimited items, this should actually work
            tracer_concs = [
                float(x.strip())
                for x in tracer_concs_str.split(CONCENTRATIONS_DELIMITER)
            ]
    except AttributeError as ae:
        if "object has no attribute 'split'" in str(ae):
            tracer_concs = [float(tracer_concs_str)]
        else:
            raise ae
    return tracer_concs


class ParsingError(Exception):
    pass


class InfusateParsingError(ParsingError):
    pass


class TracerParsingError(ParsingError):
    pass


class IsotopeParsingError(ParsingError):
    pass
