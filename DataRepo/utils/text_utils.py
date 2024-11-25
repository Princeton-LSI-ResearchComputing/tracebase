import textwrap
from typing import Union


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


def is_number(val):
    try:
        float(str(val))
    except Exception:
        return False
    return True


def sigfig(num, figures=3) -> str:
    """Return the supplied num with the significant number of figures/digits."""
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
        sigfigfloor(123.45678, 4)  # 123.4
        sigfigfloor(12345678, 4)  # 12340000
        sigfigfloor(12, 4)  # 12.01  # The last of 4 significant digits is incremented
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
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
        raise ValueError(f"'figures' must be grater than 0.  '{figures}' supplied.")

    sign, num_str, exp = get_num_parts(num)

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
            return float(sign + floored_str)
        else:
            return int(sign + floored_str)
    else:
        cased_e = "e"
        if "E" in str(num):
            cased_e = "E"
        if "." in floored_str:
            return float(sign + floored_str + cased_e + exp)
        else:
            return int(sign + floored_str + cased_e + exp)


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
        TODO: sigfigceil("0.0000000", 4)  # planned: 0.0000001 current: 1.
            Sig digs should count from left-most non-zero, rightward.  If all are 0, count is from right-most, leftward.
            There should also be a type consideration.  If the type is a string, leading zeroes in a decimal could be
            considered significant if there are trailing zeroes and fewer non-zero digits than the number of significant
            digits, because scientists write as many trailing zeroes as are significant.
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
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
        raise ValueError(f"'figures' must be grater than 0.  '{figures}' supplied.")

    floored = sigfigfloor(num, figures)

    sign, num_str, exp = get_num_parts(floored)

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
            return float(sign + ceiled_str)
        else:
            return int(sign + ceiled_str)
    else:
        cased_e = "e"
        if "E" in str(floored):
            cased_e = "E"
        if "." in ceiled_str:
            return float(sign + ceiled_str + cased_e + exp)
        else:
            return int(sign + ceiled_str + cased_e + exp)


def sigfigrange(num, figures: int = 3) -> tuple:
    """Takes a number and a number of significant figures and returns a tuple, representing a range, that can be used
    in a greater than or equal to and less than query for numbers matching num.

    Example:
        start, stop = sigfigrange(123.45678, 4)
        # start = 123.4
        # stop = 123.5
        testnum = 123.456
        if testnum >= start and testnum < stop:
            # Match! - The numbers are assumed to be the same and only differ by precision
    Args:
        num (Any): a number with any number of digits/figures.  May be str, int, float with sign and optionally in
            scientific notation.
        figures (int): A number of significant digits in num
    Exceptions:
        None
    Returns:
        range (Tuple[float, float])
    """
    return sigfigfloor(num, figures), sigfigceil(num, figures)
