def get_cookie(request, cookie_name, cookie_default=None):
    val = request.COOKIES.get(cookie_name, "")
    # A cookie value of an empty string should trigger the default value to be applied
    if val == "" and val != cookie_default:
        val = cookie_default
    return val
