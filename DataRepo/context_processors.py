from django.conf import settings


def globals(_):
    return {
        "DEBUG": settings.DEBUG,
        "READONLY": settings.READONLY,
    }
