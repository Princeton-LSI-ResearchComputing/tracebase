from typing import Optional
from urllib.parse import unquote
from warnings import warn

from django.conf import settings
from django.core.paginator import EmptyPage, Paginator
from django.http import HttpRequest

from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.utils.text_utils import iswhole


def get_cookie_dict(
    request: HttpRequest,
    prefix: Optional[str] = None,
    exclude_empties=True,
    preserve_prefix=False,
):
    """Get cookies from the request object (optionally matching a prefix).

    Args:
        request (HttpRequest)
        prefix (Optional[str]): Get cookies whose name starts with this value.
        exclude_empties (bool) [True]: Do not include cookies whose value is an empty string.
        preserve_prefix (bool): Keep the prefix in the dict keys.
    Exceptions:
        None
    Retuns:
        matching_cookies (dict)
    """
    matching_cookies = {}
    fullname: str
    for fullname, val in request.COOKIES.items():
        if (
            prefix is None or (fullname.startswith(prefix) and prefix != fullname)
        ) and (not exclude_empties or val != ""):
            if prefix is None or preserve_prefix:
                name = fullname
            else:
                name = fullname.replace(prefix, "", 1)
            val = get_cookie(request, fullname)
            matching_cookies[name] = val
    return matching_cookies


def get_cookie(request: HttpRequest, cookie_name: str, cookie_default=None):
    """Get a cookie from the request object.

    Args:
        request (HttpRequest)
        cookie_name (str)
        cookie_default (Optional[Any])
    Exceptions:
        None
    Retuns:
        val (str)
    """
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


def delete_cookie(request: HttpRequest, cookie_name: str):
    """Delete a cookie from the request object and return its value.

    Args:
        request (HttpRequest)
        cookie_name (str)
    Exceptions:
        None
    Retuns:
        val (str)
    """
    val = None
    if cookie_name in request.COOKIES.keys():
        val = get_cookie(request, cookie_name)
        del request.COOKIES[cookie_name]
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
    number or the last page if it is out of range."""

    # See: https://forum.djangoproject.com/t/letting-listview-gracefully-handle-out-of-range-page-numbers/23037/4
    def page(self, num):
        if (isinstance(num, int) and num > 1) or (iswhole(num) and str(num) != "1"):
            try:
                num = self.validate_number(num)
            except EmptyPage:
                if settings.DEBUG:
                    warn(
                        f"Page {num} is empty.  Gracefully falling back to last page: {self.num_pages}.",
                        DeveloperWarning,
                    )
                num = self.num_pages
        else:
            if settings.DEBUG and (not iswhole(num) or str(num) != "1"):
                warn(
                    f"Page {num} not an integer.  Gracefully falling back to 1.",
                    DeveloperWarning,
                )
            num = 1
        return super().page(num)
