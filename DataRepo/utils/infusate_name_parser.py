import re
from typing import List, Optional, TypedDict

KNOWN_ISOTOPES = "CNHOS"

# infusate with a name have the tracer(s) grouped in braces
INFUSATE_ENCODING_PATTERN = re.compile(
    r"^(?P<infusate_name>[^\{\}]*?)\s*\{(?P<tracers_string>[^\{\}]*?)\}$"
)
TRACERS_ENCODING_JOIN = ";"
TRACER_ENCODING_PATTERN = re.compile(
    r"^(?P<compound_name>[^\[\]][\w,\-]+)(?:\-\[(?P<isotopes>[^\[\]][0-9"
    + KNOWN_ISOTOPES
    + r",\-]+)\])$"
)
ISOTOPE_ENCODING_JOIN = ","
ISOTOPE_ENCODING_PATTERN = re.compile(
    r"(?:(?P<labeled_positions>[0-9,]+)-){0,1}(?P<labeled_element>[0-9]+[^\[\]]["
    + KNOWN_ISOTOPES
    + r"])(?P<labeled_count>[0-9+])"
)
# only allow digits, brackets, dashes, commas, and  isotope symbols
ISOTOPE_DISALLOWED_CHARACTERS = re.compile(r"[^\d\[\]\-," + KNOWN_ISOTOPES + "]")


class IsotopeData(TypedDict):
    labeled_element: str
    element: str
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
        parsed_data["infusate_name"] = match.group("infusate_name").strip()
        tracer_strings = split_encoded_tracers_string(
            match.group("tracers_string").strip()
        )
    else:
        tracer_strings = [infusate_string]

    for tracer_string in tracer_strings:
        parsed_data["tracers"].append(parse_tracer_string(tracer_string))

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

    return tracer_data


def parse_isotope_string(isotopes_string: str) -> List[IsotopeData]:

    if not isotopes_string:
        raise IsotopeParsingError("parse_isotope_string requires a defined string.")

    rejected_match = re.search(ISOTOPE_DISALLOWED_CHARACTERS, isotopes_string)
    if rejected_match:
        raise IsotopeParsingError(
            f'Encoded isotopes "{isotopes_string}" contains disallowed characters.'
        )

    isotope_data = list()
    isotopes = re.findall(ISOTOPE_ENCODING_PATTERN, isotopes_string)
    if len(isotopes) < 1:
        raise IsotopeParsingError(f'Encoded isotopes "{isotopes}" cannot be parsed.')
    recomposited_isotopes = ""
    first_time = True
    for isotope in ISOTOPE_ENCODING_PATTERN.finditer(isotopes_string):
        labeled_element = isotope.group("labeled_element")
        if labeled_element:
            match = re.search(
                r"(?P<mass_number>[\d]+)(?P<element>[" + KNOWN_ISOTOPES + "]{1})",
                labeled_element,
            )
            if match:
                mass_number = int(match.group("mass_number"))
                element = match.group("element")
        labeled_count = int(isotope.group("labeled_count"))

        recomposited_isotope = labeled_element + str(labeled_count)
        if isotope.group("labeled_positions"):
            positions_str = isotope.group("labeled_positions")
            recomposited_isotope = f"{positions_str}-{recomposited_isotope}"
            labeled_positions = [int(x) for x in positions_str.split(",")]
        else:
            labeled_positions = None

        if first_time:
            recomposited_isotopes = recomposited_isotope
            first_time = False
        else:
            recomposited_isotopes = recomposited_isotopes + "," + recomposited_isotope
        isotope_data.append(
            IsotopeData(
                labeled_element=labeled_element,
                element=element,
                mass_number=mass_number,
                labeled_count=labeled_count,
                labeled_positions=labeled_positions,
            )
        )

    if recomposited_isotopes != isotopes_string:
        raise IsotopeParsingError(
            f'Encoded isotopes "{isotopes_string}" cannot be completely interpreted {recomposited_isotopes}.'
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
