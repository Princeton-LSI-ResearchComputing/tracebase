import re
from typing import List, Optional, TypedDict

from DataRepo.models.tracer_labeled_class import TracerLabeledClass

KNOWN_ISOTOPES = "".join(TracerLabeledClass.tracer_labeled_elements_list())

# infusate with a name have the tracer(s) grouped in braces
INFUSATE_ENCODING_PATTERN = re.compile(
    r"^(?:(?P<infusate_name>[^\{\}]*?)\s*\{)?(?P<tracers_string>[^\{\}]*?)\}?$"
)
TRACERS_ENCODING_JOIN = ";"
TRACER_ENCODING_PATTERN = re.compile(
    r"^(?P<compound_name>.*?)-\[(?P<isotopes>[^\[\]]+)\]$"
)
ISOTOPE_ENCODING_JOIN = ","
ISOTOPE_ENCODING_PATTERN = re.compile(
    r"(?P<all>(?:(?P<labeled_positions>[0-9,]+)-)?(?P<mass_number>[0-9]+)(?P<labeled_element>["
    + KNOWN_ISOTOPES
    + r"]{1,2})(?P<labeled_count>[0-9]+))"
)


class IsotopeData(TypedDict):
    labeled_element: str
    mass_number: int
    labeled_count: int
    labeled_positions: Optional[List[int]]


class TracerData(TypedDict):
    unparsed_string: str
    compound_name: str
    isotopes: List[IsotopeData]


class InfusateData(TypedDict):
    unparsed_string: str
    infusate_name: Optional[str]
    tracers: List[TracerData]


def parse_infusate_name(infusate_string: str) -> InfusateData:
    """
    Takes a complex infusate, coded as a string, and parses it into its optional
    name, lists of tracer(s) and compounds.
    """

    # defaults
    # assume the string lacks the optional name, and it is all tracer encodings
    infusate_string = infusate_string.strip()
    parsed_data: InfusateData = {
        "unparsed_string": infusate_string,
        "infusate_name": None,
        "tracers": list(),
    }

    match = re.search(INFUSATE_ENCODING_PATTERN, infusate_string)

    if match:
        short_name = match.group("infusate_name")
        if short_name is not None and short_name.strip() != "":
            parsed_data["infusate_name"] = short_name.strip()
        tracer_strings = split_encoded_tracers_string(
            match.group("tracers_string").strip()
        )
    else:
        raise InfusateParsingError(
            f"Unable to parse infusate string: [{infusate_string}]"
        )

    for tracer_string in tracer_strings:
        parsed_data["tracers"].append(parse_tracer_string(tracer_string))

    return parsed_data


def split_encoded_tracers_string(tracers_string: str) -> List[str]:
    tracers = tracers_string.split(TRACERS_ENCODING_JOIN)
    return tracers


def parse_tracer_string(tracer: str, parse_one=False) -> TracerData:

    tracer_data: TracerData = {
        "unparsed_string": tracer,
        "compound_name": "",
        "isotopes": list(),
    }

    match = re.search(TRACER_ENCODING_PATTERN, tracer)
    if match:
        if parse_one and (match.start != 0 or match.end != len(tracer)):
            raise TracerParsingError(f'Encoded tracer "{tracer}" cannot be parsed.')
        tracer_data["compound_name"] = match.group("compound_name").strip()
        tracer_data["isotopes"] = parse_isotope_string(match.group("isotopes").strip())
    else:
        raise TracerParsingError(f'Encoded tracer "{tracer}" cannot be parsed.')

    # Compound names are very premissive, but we should at least make sure a malformed isotope specification didn't
    # bleed into the compound pattern (like you would get if the wrong delimiter was used
    # - see test_malformed_tracer_parsing_with_improper_delimiter)
    imatch = re.search(ISOTOPE_ENCODING_PATTERN, tracer_data["compound_name"])
    if imatch:
        raise TracerParsingError(
            f'Encoded tracer "{tracer}" cannot be parsed.  A compound name cannot contain an isotope encoding string.'
        )

    return tracer_data


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
        labeled_element = isotope.group("labeled_element")
        labeled_count = int(isotope.group("labeled_count"))
        labeled_positions = None
        if isotope.group("labeled_positions"):
            positions_str = isotope.group("labeled_positions")
            labeled_positions = [int(x) for x in positions_str.split(",")]

        if parsed_string is None:
            parsed_string = isotope.group("all")
        else:
            parsed_string += "," + isotope.group("all")

        isotope_data.append(
            IsotopeData(
                labeled_element=labeled_element,
                mass_number=mass_number,
                labeled_count=labeled_count,
                labeled_positions=labeled_positions,
            )
        )

    if parsed_string != isotopes_string:
        raise IsotopeParsingError(
            f"One or more encoded isotopes in [{isotopes_string}] could not be parsed. Only the following were "
            f"parsed: [{parsed_string}]."
        )

    return isotope_data


class ParsingError(Exception):
    pass


class InfusateParsingError(ParsingError):
    pass


class TracerParsingError(ParsingError):
    pass


class IsotopeParsingError(ParsingError):
    pass
