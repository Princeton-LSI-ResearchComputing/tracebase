from urllib.parse import unquote

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
        print(f"WARNING: Encountered unencoded cookie: '{cookie_name}' = '{val}'.  {type(e).__name__}: {e}")
        return val
