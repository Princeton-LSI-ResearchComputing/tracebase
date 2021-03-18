from django.conf import settings
from django.db import models


class Compound(models.Model):
    # Class variables
    HMDB_CPD_URL = settings.HMDB_CPD_URL

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    formula = models.CharField(max_length=256)

    # ID to serve as an external link to record using HMDB_CPD_URL
    hmdb_id = models.CharField(max_length=11, unique=True)
