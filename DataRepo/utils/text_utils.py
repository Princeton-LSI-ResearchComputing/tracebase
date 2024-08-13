import textwrap


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
