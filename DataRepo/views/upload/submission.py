from django.conf import settings
from django.shortcuts import render


def upload(request):
    context = {
        "data_submission_email": settings.DATA_SUBMISSION_EMAIL,
        "data_submission_url": settings.DATA_SUBMISSION_URL,
    }
    return render(request, "upload.html", context)
