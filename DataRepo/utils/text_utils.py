import re
import textwrap
from typing import Optional, Union


def autowrap(text: str, default_width: int = 80, **kwargs):
    """Given text (containing hard-returns), this method uses textwrap.wrap() to wrap long lines to a max length as
    determined by textwrap.wrap's 'width' argument.  The default for that argument is 70, but this method sets it to a
    default of 80.  It returns the wrapped text string, the number of lines in the result, and the length of the longest
    line.

    This is intended to be used to determine text box dimensions (after applying font and line sizes).

    Args:
        text (string): The text/content to be wrapped.
        default_width (integer): The number of characters allowed on a line (including white space) is kwargs["width"]
            is not set.
        kwargs (dict): Keyword arguments supplied directly to textwrap.wrap()
    Exceptions:
        None
    Returns:
        wrapped (string): The input text with hard-returns inserted in lines that exceed kwargs["width"].
        nlines (int): The number of lines in the outout (not including a line after the last trailing hard return).
        max_width (int): The number of characters on the longest line.  Note, this does not account for variable width
            fonts.  If the intention is to use this to compute text box width, use a fixed-witdh font, or parse the
            lines in {wrapped} to determine the longest line.
    """
    wrapped = ""
    nlines = 0
    max_width = 0

    if text == "":
        # Avoid issues with removing a non-existent character
        return wrapped, nlines, max_width

    if kwargs.get("width") is None:
        kwargs["width"] = default_width

    for line in text.split("\n"):
        if line == "":
            # textwrap.wrap() returns an empty list when line is ""
            nlines += 1
            wrapped += "\n"
            continue

        for wrappedline in textwrap.wrap(line, **kwargs):
            nlines += 1
            if len(wrappedline) > max_width:
                max_width = len(wrappedline)
            wrapped += f"{wrappedline}\n"

    return wrapped[:-1], nlines, max_width


def sigfig(num: Union[int, float], figures=3) -> str:
    """Return the supplied num with the significant number of figures/digits.

    Examples:
        sigfig(123.45678, 4)  # 123.5
        sigfig(12345678, 4)  # 12350000
        sigfig(1234312, 4)  # 12340000
        sigfig(12, 4)  # 12
    Args:
        num (Union[int, float]): A number with any number of digits/figures.  May have a sign and optionally be in
            scientific notation.
        figures (int): A number of significant digits in num
    Exceptions:
        None
    Returns:
        (str): Formatted number containing only significant figures/digits
    """
    # TODO: Add support for trailing significant zeroes (with the python g format specifier removes)
    return f"{num:.{figures}g}"


def get_num_parts(num):
    """Breaks up a number into sign, number, and exponent.  sign and exponent are optional.

    Examples:
        get_num_parts("+.122e33")  # "+", ".122", "33"
        get_num_parts("0.122")  # "", ".122", None
        get_num_parts("-000.122e+33")  # "-", "000.122", "+33"
        get_num_parts(-.122E33)  # "-", "1.22", "+32"  # python applies its own rules to scientific notation
    Args:
        num (Union[str, int, float]): A number with any number of digits/figures.  May have a sign (+/-) and optionally
            be in scientific notation (e.g. 1.0e-10).
    Exceptions:
        None
    Returns:
        sign (str): "", "+", or "-"
        num_str (str)
        exp (Optional[str]): Does not include the "e".  None, if no exponent.
    """
    e_str = str(num).lower()
    if "e" in e_str:
        num_str, exp = e_str.split("e")
    else:
        num_str = e_str
        exp = None

    sign = ""
    if "+" in num_str:
        sign = "+"
        num_str = num_str.lstrip("+")
    elif "-" in num_str:
        sign = "-"
        num_str = num_str.lstrip("-")

    return sign, num_str, exp


def sigfigfloor(num, figures: int = 3) -> Union[float, int]:
    """Truncate a number to the number of significant digits (i.e. do not round)

    Examples:
        sigfigfloor(0.0012345678, 4)  # 0.001234
        sigfigfloor(123.45678, 4)  # 123.4
        sigfigfloor(12345678, 4)  # 12340000
        sigfigfloor(123.45678, 4)  # 123.4
        sigfigfloor(12345678, 4)  # 12340000
        sigfigfloor(12, 4)  # 12
        sigfigfloor(-0.0012345678, 4)  # -0.001235
        sigfigfloor(-123.45678, 4)  # -123.5
        sigfigfloor(-12345678, 4)  # -12350000
        sigfigfloor(-123.45678, 4)  # -123.5
        sigfigfloor(-12345678, 4)  # -12350000
        sigfigfloor(-12, 4)  # -12.01  # The last of 4 significant digits is incremented
    Args:
        num (Union[str, float, int]): A number with any number of digits/figures.  May have a sign and optionally be in
            scientific notation.
        figures (int): A number of significant digits in num
    Exceptions:
        Raises:
            ValueError
        Buffers:
            None
    Returns:
        floored (Union[float, int])
    """
    if figures < 1:
        raise ValueError(f"'figures' must be greater than 0.  '{figures}' supplied.")

    if str(num).startswith("-"):
        return -1 * sigfigceil(str(num).lstrip("-"), figures=figures)

    _, num_str, exp = get_num_parts(num)

    if "." in num_str:
        whl_part, dec_part = num_str.split(".")
        if whl_part == "" or int(whl_part) == 0:
            # The number is less than 1
            if int(dec_part) == 0:
                floored_str = "0"
            else:
                # The number is greater than 0
                dec_sd_part = dec_part.lstrip("0")
                leader_len = len(dec_part) - len(dec_sd_part)
                dec_sd_part = dec_sd_part[:figures]
                floored_str = whl_part + "." + ("0" * leader_len) + dec_sd_part
        else:
            whl_part = whl_part.lstrip("0")
            if len(whl_part) == figures:
                floored_str = whl_part
            elif len(whl_part) > figures:
                trailer_len = len(whl_part) - figures
                floored_str = whl_part[:figures] + ("0" * trailer_len)
            else:
                dec_figures = figures - len(whl_part)
                floored_str = whl_part + "." + dec_part[:dec_figures]
    else:
        whl_part = num_str.lstrip("0")
        if len(whl_part) == figures:
            floored_str = whl_part
        elif len(whl_part) > figures:
            trailer_len = len(whl_part) - figures
            floored_str = whl_part[:figures] + ("0" * trailer_len)
        else:
            floored_str = whl_part

    if floored_str == "":
        floored_str = "0"

    if exp is None:
        if "." in floored_str:
            return float(floored_str)
        else:
            return int(floored_str)
    else:
        cased_e = "e"
        if "E" in str(num):
            cased_e = "E"
        if "." in floored_str:
            return float(floored_str + cased_e + exp)
        else:
            return int(floored_str + cased_e + exp)


def sigfigceil(num, figures: int = 3) -> Union[float, int]:
    """Truncate a number to the number of significant digits (i.e. do not round), then add a 1 in the last place of the
    significant digits in the supplied num.

    Examples:
        sigfigceil(0.0012345678, 4)  # 0.001235
        sigfigceil(123.45678, 4)  # 123.5
        sigfigceil(12345678, 4)  # 12350000
        sigfigceil(0.0012, 4)  # 0.001201
        sigfigceil(120, 4)  # 120.1
        sigfigceil(12000000, 4)  # 12010000
        sigfigceil(-0.0012345678, 4)  # -0.001234
        sigfigceil(-123.45678, 4)  # -123.4
        sigfigceil(-12345678, 4)  # -12340000
        sigfigceil(-0.0012, 4)  # -0.0012
        sigfigceil(-120, 4)  # -120
        sigfigceil(-12000000, 4)  # -12000000
        TODO: sigfigceil("0.0000000", 4)  # planned: 0.0000001 current: 1.
            A description of support for significant zeros (which python's format specifier (g) removes...
            Sig digs should count from left-most non-zero, rightward.  If all are 0, count is from right-most, leftward.
            There should also be a type consideration.  If the type is a string, leading zeroes in a decimal could be
            considered significant if there are trailing zeroes and fewer non-zero digits than the number of significant
            digits, because scientists write as many trailing zeroes as are significant.
    Args:
        num (Union[str, float, int]): A number with any number of digits/figures.  May have a sign and optionally be in
            scientific notation.
        figures (int): A number of significant digits in num
    Exceptions:
        Raises:
            ValueError
        Buffers:
            None
    Returns:
        ceiled (Union[float, int])
    """
    # TODO: In a value like 0.000, if there is 1 significant digit, the last 0 is considered to be the significant
    # digit, so ceil should return 0.001, but right now, it returns 1.  (When there are non-zeroes in the number, the
    # first significant digit is the highest [left-most] such digit [which is what is currently supported].)

    if figures < 1:
        raise ValueError(f"'figures' must be greater than 0.  '{figures}' supplied.")

    if str(num).startswith("-"):
        return -1 * sigfigfloor(str(num).lstrip("-"), figures=figures)

    floored = sigfigfloor(num, figures)

    _, num_str, exp = get_num_parts(floored)

    if "." in num_str:
        whl_part, dec_part = num_str.split(".")
    else:
        whl_part = num_str
        dec_part = ""

    whl_part = whl_part.lstrip("0")
    dec_part = dec_part.rstrip("0")

    if len(whl_part) >= figures:
        add_one = int("1" + ("0" * (len(whl_part) - figures)))
        ceiled_str = str(int(whl_part) + add_one)
    elif len(whl_part) > 0:
        dec_figures = figures - len(whl_part)
        dec_sd_part = dec_part
        if len(dec_sd_part) <= dec_figures:
            dec_sd_part += "0" * (dec_figures - len(dec_sd_part))

        if int(dec_sd_part) == 0:
            dec_sd_part = ("0" * (dec_figures - 1)) + "1"
        else:
            dec_sd_part = str(int(dec_sd_part) + 1)

        if len(dec_sd_part) > dec_figures:
            dec_sd_part = dec_sd_part[1:]
            whl_part = str(int(whl_part) + 1)

        ceiled_str = whl_part + "." + dec_sd_part
    else:
        # Remove leading zeroes
        dec_sd_part = dec_part.lstrip("0")
        # Determine the number of leading zeroes
        dec_leader_len = len(dec_part) - len(dec_sd_part)
        if len(dec_sd_part) <= figures:
            dec_sd_part += "0" * (figures - len(dec_sd_part))

        # Add 1 (treated as int)
        if int(dec_sd_part) == 0:
            # We know that whl_part is 0. When the entire value is 0, the significant digits start with the "0" in the
            # whl_part
            if figures == 1:
                whl_part = "1"
                dec_sd_part = "0"
            else:
                dec_sd_part = ("0" * (figures - 2)) + "1"
        else:
            # The significant digits start with the first non-0 number in the decimal digits
            dec_sd_part = str(int(dec_sd_part) + 1)

        if len(dec_sd_part) > figures:
            dec_leader_len -= 1
            if dec_leader_len < 0:
                dec_sd_part = dec_sd_part[1:]
                if whl_part == "":
                    whl_part = "1"
                else:
                    whl_part = str(int(whl_part) + 1)

        ceiled_str = whl_part + "." + ("0" * dec_leader_len) + dec_sd_part

    if exp is None:
        if "." in ceiled_str:
            return float(ceiled_str)
        else:
            return int(ceiled_str)
    else:
        cased_e = "e"
        if "E" in str(floored):
            cased_e = "E"
        if "." in ceiled_str:
            return float(ceiled_str + cased_e + exp)
        else:
            return int(ceiled_str + cased_e + exp)


def iswhole(num):
    """Returns true if the number is a whole number (i.e. has no decimal value).

    Examples:
        iswhole(0)  # True
        iswhole(0.1)  # False
        iswhole(1.0)  # True
        iswhole(1.0e0)  # True
        iswhole(1.0e1)  # True
        iswhole(10e-2)  # False
        iswhole(0001e-1)  # False
        iswhole(10)  # True
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
            scientific notation.
    Exceptions:
        None
    Returns:
        (bool)
    """
    _, num_str, exp = get_num_parts(num)
    if exp is None:
        if "." not in num_str:
            return True
        whl, dec = num_str.split(".")
        return int(dec) == 0
    exp = exp.lstrip("+")
    if "." in num_str:
        whl, dec = num_str.split(".")
    else:
        whl = num_str
        dec = ""
    whl = whl.lstrip("0")
    if int(exp) < 0:
        if len(whl) == abs(int(exp)):
            dec = whl + dec
        elif len(whl) < abs(int(exp)):
            dec = ("0" * (abs(int(exp)) - len(whl))) + whl + dec
        else:
            start = len(whl) - abs(int(exp))
            dec = whl[start:] + dec
    else:
        if len(dec) == abs(int(exp)):
            dec = ""
        elif len(dec) < abs(int(exp)):
            dec = ""
        else:
            start = int(exp)
            dec = dec[start:]
    return dec == "" or int(dec) == 0


def getsigfig(num):
    """Returns the number of significant figures.  Ignores trailing whole number zeroes if there is no decimal.  Ignores
    leading whole number zeroes.  Does not ignore trailing decimal zeroes (i.e. assumes significant zeroes).  Ignores
    leading decimal zeroes is the whole number part is equal to 0.

    WARNING: Python doesn't preserve or respect significant zeroes, so you must supply string versions of numbers to get
    the correct number of significant digits.  E.g.

        getsigfig(1.000e-7)    # 1  # WRONG, because python represents 1.000e-7 as 1e-7
        getsigfig(1e7)         # 9  # WRONG, because python represents 1e7 as 10000000.0

    Supply a string instead:

        getsigfig("1.000e-7")  # 4
        getsigfig("1e7")       # 1

    Examples:
        getsigfig(0.00012345)  # 5
        getsigfig(0.000123450)  # 6
        getsigfig(1.00012345)  # 9
        getsigfig(1.000123450)  # 10
        getsigfig(10.00)  # 4
        getsigfig(0010)  # 1
        getsigfig(0010.)  # 2
        getsigfig(1.000e7)  # 4
        getsigfig("1.000e-7")  # 4
        getsigfig(1000000)  # 1
        getsigfig("1e7")  # 1
        getsigfig(1.0e7)  # 2
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
            scientific notation.
    Exceptions:
        None
    Returns:
        (int)
    """
    _, num_str, _ = get_num_parts(num)
    whl, dec = num_str.split(".") if "." in num_str else (num_str, "")
    whl = whl.lstrip("0")
    if dec == "" and "." not in num_str:
        return len(whl.rstrip("0"))
    if whl == "" or int(whl) == 0:
        return len(dec.lstrip("0"))
    return len(whl) + len(dec)


def sigfigfilter(
    num: Union[int, float],
    fieldname: str,
    figures: int = 3,
    query: Optional[dict] = None,
    update=False,
) -> dict:
    """Returns and/or updates a dict that can be used as a django ORM lookup filter to find numbers exactly matching the
    given num, when converted to the given number of significant figures.

    Specifically, it returns a dict like:

        If num >= 0:

            {
                f"{fieldname}__gte": sigfigfloor(num),
                f"{fieldname}__lt": sigfigceil(num),
            }

        If num < 0:

            {
                f"{fieldname}__gt": sigfigfloor(num),
                f"{fieldname}__lte": sigfigceil(num),
            }

    Examples:
        sigfigfilter(1.444, "field")  # {"field__gte": 1.44, "field__lt": 1.45}
        sigfigfilter(-1.444, "field")  # {"field__gt": -1.45, "field__lte": -1.44}
        sigfigfilter(1.445, "field")  # {"field__gte": 1.44, "field__lt": 1.45}
        sigfigfilter(-1.445, "field")  # {"field__gt": -1.45, "field__lte": -1.44}
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
            scientific notation.
        fieldname (str): The name of a django model field (or field path, e.g. "foreignkey__fieldname")
        figures (int): A number of significant figures/digits to be applied in the django ORM search for num.  I.e. if
            num is 3.3333, and we're looking for values in the database that match within 3 significant figures, then we
            want to find any number greater than or equal to 3.33 and less than 3.34.
        query (Optional[dict]): An existing query dict that will be supplied to a django ORM queryset "filter", e.g. the
            `query` dict in `Model.objects.filter(**query)`.
        update (bool): Supply True if you would like to overwrite pre-existing keys in `query` without error.
    Exceptions:
        ValueError
    Returns:
        query_dict (dict)
    """
    if fieldname == "":
        raise ValueError("fieldname must not be an empty string.")

    if num < 0:
        lower_bound_key = f"{fieldname}__gt"
        upper_bound_key = f"{fieldname}__lte"
    else:
        lower_bound_key = f"{fieldname}__gte"
        upper_bound_key = f"{fieldname}__lt"

    if query is not None:
        query_dict = query

        if update is False:
            existing_keys = []
            if lower_bound_key in query_dict.keys():
                existing_keys.append(lower_bound_key)
            if upper_bound_key in query_dict.keys():
                existing_keys.append(upper_bound_key)
            if len(existing_keys) > 0:
                raise ValueError(
                    f"The supplied query dict {query} must not contain the key(s) {existing_keys} when update is "
                    "False."
                )
    else:
        query_dict = {}

    floor = sigfigfloor(num, figures)
    ceil = sigfigceil(num, figures)

    lesser_value, greater_value = (floor, ceil) if floor <= ceil else (ceil, floor)

    query_dict[lower_bound_key] = lesser_value
    query_dict[upper_bound_key] = greater_value

    return query_dict


def camel_to_title(string: str, delim: str = " "):
    """Example: camel_to_title('MSRunSample') -> 'MS Run Sample'"""
    return delim.join(re.split(r"(?=[A-Z][a-z])", string))


def underscored_to_title(string: str, delim: str = " "):
    """Example: underscored_to_title('this_is_a__function_tEST') -> 'This is a Function tEST'"""
    # See: https://prowritingaid.com/list-of-words-not-capitalized-in-titles
    subsequent_lowers = [
        "a",
        "and",
        "as",
        "at",
        "but",
        "by",
        "down",
        "for",
        "from",
        "if",
        "in",
        "into",
        "like",
        "near",
        "nor",
        "of",
        "off", 
        "on",
        "once",
        "onto",
        "or",
        "over",
        "past",
        "so",
        "than",
        "that",
        "to",
        "upon",
        "when",
        "with",
        "yet",
        "is",  # My exception
    ]
    words: str = re.split(r"(?:_+)", string)
    return delim.join([w.title() if (i == 0 or w.islower()) and w not in subsequent_lowers else w for i, w in enumerate(words)])
