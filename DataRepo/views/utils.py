from typing import Optional
from urllib.parse import unquote

from django.core.paginator import EmptyPage, PageNotAnInteger, Paginator


def get_cookie_dict(request, prefix: Optional[str] = None, exclude_empties=True):
    matching_cookies = {}
    fullname: str
    for fullname, val in request.COOKIES.items():
        if (
            prefix is None or (fullname.startswith(prefix) and prefix != fullname)
        ) and (not exclude_empties or val != ""):
            name = fullname if prefix is None else fullname.replace(prefix, "", 1)
            val = get_cookie(request, fullname)
            matching_cookies[name] = val
    return matching_cookies


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
    """This derived class of Paginator prevents page not found errors by defaulting to page 1 when the page is not a
    number or the last lage if it is out of range."""

    # See: https://forum.djangoproject.com/t/letting-listview-gracefully-handle-out-of-range-page-numbers/23037/4
    def page(self, num):
        try:
            num = self.validate_number(num)
        except PageNotAnInteger:
            num = 1
        except EmptyPage:
            num = self.num_pages
        return super().page(num)
