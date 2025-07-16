from typing import Dict, Optional, Union
from warnings import warn

from django.conf import settings
from django.templatetags.static import static
from django.utils.safestring import mark_safe
from django.views.generic import ListView

from DataRepo.utils.exceptions import DeveloperWarning
from DataRepo.views.models.bst.column.base import BSTBaseColumn
from DataRepo.views.utils import get_cookie, get_cookie_dict


class BSTClientInterface(ListView):
    """This is a server-side interface to the Bootstrap Table javascript and cookies in the client's browser.  It
    encapsulates the javascript and cookie structure/functions and is tightly integrated with BSTListView (which is
    intended to inherit from this class).  This is not intended to be used on its own.
    """

    script_name = "DataRepo/static/js/bst/base.js"

    def __init__(self, **kwargs):
        """An extension of the ListView constructor intended to initialize the javascript and cookie interface.  It
        facillitates communication between the browser and the view.

        Args:
            kwargs (dict): Keyword args passed to ListView's constructor.
        Exceptions:
            None
        Returns:
            None
        """

        super().__init__(**kwargs)

        self.cookie_prefix = f"{self.__class__.__name__}-"
        self.cookie_warnings = []
        self.cookie_resets = []
        self.clear_cookies = False

    @property
    def script(self) -> str:
        """Returns an HTML script tag whose source points to self.script_name.

        Example:
            # BSTClientInterface.get_context_data
                context["bst_object"] = self
            # Template
                {{ bst_object.script }}
            # Template result (assuming settings.STATIC_URL = "static/")
                <script src='static/js/bst/base.js'></script>
        """
        return mark_safe(f"<script src='{static(self.script_name)}'></script>")

    def get_param(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieves a URL parameter.

        Args:
            name (str)
            default (str)
        Exceptions:
            None
        Returns:
            (Optional[str]): The param value for the supplied name obtained from self.request or the default if the
                parameter was not found (or was an empty string).
        """
        if hasattr(self, "request"):
            val = self.request.GET.get(name, default)
            return val if val is not None and val != "" else default
        return default

    def get_cookie_name(self, name: str) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            name (str)
        Exceptions:
            None
        Returns:
            (str)
        """
        return f"{self.cookie_prefix}{name}"

    def get_cookie(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieves a cookie using a prepended view name.

        Args:
            name (str)
            default (str)
        Exceptions:
            None
        Returns:
            (Optional[str]): The cookie value for the supplied name (with the view_name prepended) obtained from
                self.request or the default if the cookie was not found (or was an empty string).
        """
        # If a cookie reset occurred, it means one or more of the cookies is problematic, so just return the default.
        if hasattr(self, "request") and not self.clear_cookies:
            return get_cookie(self.request, self.get_cookie_name(name), default)
        return default

    def get_boolean_cookie(self, name: str, default: bool = False) -> bool:
        """Retrieves a cookie using a prepended view name.

        Assumptions:
            1. Any string starting with T or F (case insensitive) is True or False, respectively.
        Args:
            name (str)
            default (str) [""]
        Exceptions:
            None
        Returns:
            (str): The cookie value for the supplied name (with the view_name prepended) obtained from self.request or
                the default if the cookie was not found (or was an empty string).
        """
        # If a cookie reset occurred, it means one or more of the cookies is problematic, so just return the default.
        boolstr = self.get_cookie(name)
        if boolstr == "" or boolstr is None:
            return default
        elif boolstr.lower().startswith("t"):
            return True
        elif boolstr.lower().startswith("f"):
            return False

        cookie_name = self.get_cookie_name(name)
        if cookie_name not in self.cookie_resets:
            self.cookie_resets.append(cookie_name)
            warning = f"Invalid '{name}' value encountered: '{boolstr}'.  Clearing cookie '{cookie_name}'."
            self.cookie_warnings.append(warning)
            if settings.DEBUG:
                warn(warning, DeveloperWarning)

        return default

    def get_column_cookie_name(
        self, column: Union[BSTBaseColumn, str], name: str
    ) -> str:
        """Retrieves a cookie name using a prepended view name.

        Args:
            column (Union[BSTColumn, str]): The name of the BST column or the column object
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            None
        Returns:
            (str)
        """
        if isinstance(column, str):
            return f"{self.cookie_prefix}{name}-{column}"
        return f"{self.cookie_prefix}{name}-{column.name}"

    def get_column_cookie_dict(self, name: str) -> dict:
        """Retrieves a dict of values where the column names are the keys and the values are the value of the cookie
        name for the current view.

        Example:
            get_column_cookie_dict("visible")
            # output example: {"column1": True, "column2": False}
        Args:
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            ValueError when there is no request
        Returns:
            (dict): A dict of cookie values keyed on column names
        """
        if not hasattr(self, "request") or self.clear_cookies:
            # This is to avoid exceptions when testing in the shell.  That's when there's no request.
            return {}
        return get_cookie_dict(self.request, prefix=f"{self.cookie_prefix}{name}-")

    def get_boolean_column_cookie_dict(self, name: str) -> Dict[str, bool]:
        """Retrieves a dict of values where the column names are the keys and the values are the boolean version of the
        value of the cookie for the current view.

        Example:
            get_column_cookie_dict("visible")
            # output example: {"column1": True, "column2": False}
        Args:
            name (str): The name of the cookie variable specific to the column
        Exceptions:
            ValueError when there is no request
        Returns:
            (dict)
        """
        boolstrs_dict: Dict[str, str] = self.get_column_cookie_dict(name)
        bools_dict: Dict[str, bool] = {}
        for colname, boolstr in boolstrs_dict.items():
            if boolstr.lower().startswith("t"):
                bools_dict[colname] = True
            elif boolstr.lower().startswith("f"):
                bools_dict[colname] = False
            else:
                cookie_name = self.get_column_cookie_name(colname, name)
                if cookie_name not in self.cookie_resets:
                    self.cookie_resets.append(cookie_name)
                    # TODO: Change the column name to the header
                    warning = (
                        f"Invalid '{name}' cookie value encountered for column '{colname}': '{boolstr}'.  "
                        f"Clearing cookie '{cookie_name}'."
                    )
                    self.cookie_warnings.append(warning)
                    if settings.DEBUG:
                        warn(warning, DeveloperWarning)
        return bools_dict

    def get_column_cookie(
        self, column: BSTBaseColumn, name: str, default: Optional[str] = None
    ) -> Optional[str]:
        """Retrieves a cookie using a prepended view name.

        Args:
            column (str): The name of the BST column
            name (str): The name of the cookie variable specific to the column
            default (str) [""]
        Exceptions:
            None
        Returns:
            (Optional[str]): The cookie value for the supplied name (with the view_name prepended) obtained from
                self.request or the default if the cookie was not found (or was an empty string).
        """
        if hasattr(self, "request"):
            return get_cookie(
                self.request, self.get_column_cookie_name(column, name), default
            )
        return default
