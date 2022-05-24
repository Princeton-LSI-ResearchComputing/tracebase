import re
from typing import Optional, TypedDict

# infusate with a name have the tracer(s) grouped in braces
INFUSATE_ENCODING_PATTERN = r"(.*)\s*\{(.*)\}"
TRACERS_ENCODING_JOIN = ";"
TRACER_ENCODING_PATTERN = r"(.*)\-\[([0-9CNHOS,\-]*)\]"


class ParsedInfusate(TypedDict):
    original_infusate: str
    infusate_name: Optional[str]
    tracer_names: list
    compound_names: list
    isotope_labels: list


class ParsedTracer(TypedDict):
    compound_names: list
    isotope_labels: list


def parse_infusate_name(infusate_string: str) -> ParsedInfusate:
    """
    Takes a complex infusate, coded as a string, and parses it into its optional
    name, lists of tracer(s) and compounds.
    """

    # defaults
    # assume the string lacks the optional name, and it is all tracer encodings
    parsed_data: ParsedInfusate = {
        "original_infusate": infusate_string,
        "infusate_name": None,
        "tracer_names": split_encoded_tracers_string(infusate_string),
        "compound_names": list(),
        "isotope_labels": list(),
    }

    match = re.search(INFUSATE_ENCODING_PATTERN, infusate_string)

    if match:
        parsed_data["infusate_name"] = match.group(1).strip()
        tracers_string = match.group(2).strip()
        # over-write the defaults
        tracer_names = split_encoded_tracers_string(tracers_string)
        parsed_data["tracer_names"] = tracer_names

    tracer_data = parse_tracer_strings(parsed_data["tracer_names"])

    parsed_data["compound_names"] = tracer_data["compound_names"]
    parsed_data["isotope_labels"] = tracer_data["isotope_labels"]

    return parsed_data


def split_encoded_tracers_string(tracers_string: str) -> list:
    tracers = tracers_string.split(TRACERS_ENCODING_JOIN)
    return tracers


def parse_tracer_strings(tracers: list[str]) -> ParsedTracer:

    tracers_data: ParsedTracer = {"compound_names": list(), "isotope_labels": list()}

    for tracer in tracers:
        match = re.search(TRACER_ENCODING_PATTERN, tracer)
        if match:
            compound = match.group(1).strip()
            labeling = match.group(2).strip()
            tracers_data["compound_names"].append(compound)
            tracers_data["isotope_labels"].append(labeling)
        else:
            raise Exception(f'Encoded tracer "{tracer}" cannot be parsed.')

    return tracers_data
