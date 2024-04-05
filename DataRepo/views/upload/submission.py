from django.conf import settings
from django.shortcuts import render


def upload(request):
    context = {
        "submission_feedback_url": settings.FEEDBACK_URL,
        "submission_form_url": settings.SUBMISSION_FORM_URL,
        "submission_doc_url": settings.SUBMISSION_DOC_URL,
        "submission_doc_name": settings.SUBMISSION_DOC_NAME,
        "submission_drive_type": settings.SUBMISSION_DRIVE_TYPE,
        "submission_drive_folder": settings.SUBMISSION_DRIVE_FOLDER,
    }
    return render(request, "upload.html", context)
