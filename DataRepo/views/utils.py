from urllib.parse import unquote

from django.core.paginator import PageNotAnInteger, EmptyPage, Paginator

def get_cookie(request, cookie_name, cookie_default=None):
    val = request.COOKIES.get(cookie_name, "")
    # A cookie value of an empty string should trigger the default value to be applied
    if val == "" and val != cookie_default:
        return cookie_default
    try:
        # If this is not an encoded string, an error can be thrown.  A regular string can look like an encoded string,
        # in which case, the return value will be invalid, but those cases will eventually flush out.
        return unquote(val)
    except Exception as e:
        print(
            f"WARNING: Encountered unencoded cookie: '{cookie_name}' = '{val}'.  {type(e).__name__}: {e}"
        )
        return val


# See https://docs.djangoproject.com/en/5.1/howto/outputting-csv/#streaming-large-csv-files
class Echo:
    """A class that implements just the write method of a file-like interface.

    This is intended for use by a csv writer.
    """

    def write(self, value):
        return value


class ZipBuffer:
    """A class that implements a zip-file-like interface.

    This is intended for use by a zipfile writer.
    """

    def __init__(self):
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)
        return len(data)

    def flush(self):
        pass

    def take(self):
        buf = self.buf
        self.buf = bytearray()
        return bytes(buf)

    def end(self):
        buf = self.buf
        self.buf = None
        return bytes(buf)


class GracefulPaginator(Paginator):
    """This derived class of Paginator prevents page not found errors by defaulting to page 1 when the page is either
    out of range or not a number."""
    # See: https://forum.djangoproject.com/t/letting-listview-gracefully-handle-out-of-range-page-numbers/23037/4
    def page(self, num):
        try:
            num = self.validate_number(num)
        except PageNotAnInteger:
            num = 1
        except EmptyPage:
            num = self.num_pages
        return super().page(num)


def reduceuntil(function, untilfunction, sequence, initial=None):
    """Like functools.reduce, but with a condition function that stops the reduction early if a condition is met.

    Example:
        input_list = [2, 2, 2, 2, 2, 3, 4, 5, 6, 7]
        max_unique_len = 2
        reduceuntil(
            lambda ulst, val: ulst + [val] if val not in ulst else ulst,
            lambda val: len(val) >= max_unique_len,
            input_list,
            [],
        )
        # Output: [2, 3]
    Args:
        function (Callable): See functools.reduce (same)
        untilfunction (Callable): Takes the accumulating result and returns a bool that should be True if the reduction
            should stop and False if it should keep going.
        sequence (Iterable): See functools.reduce (same)
        initial (Any): See functools.reduce (same)
    Exceptions:
        TypeError - when initial is invalid and needed.
    Returns:
        value (Any): The final accumulated output of function
    """

    it = iter(sequence)

    if initial is None:
        try:
            value = next(it)
        except StopIteration:
            raise TypeError(
                "reduceuntil() of empty sequence with no initial value"
            ) from None
    else:
        value = initial

    for element in it:
        value = function(value, element)
        if untilfunction(value):
            break

    return value
