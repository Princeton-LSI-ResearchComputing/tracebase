def get_cookie(request, cookie_name, cookie_default):
    return request.COOKIES.get(cookie_name, cookie_default)
