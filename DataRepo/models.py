from django.db import models


class Compound(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    formula = models.CharField(max_length=256)

    # ID to serve as an external link to record in https://hmdb.ca
    hmdb_id = models.CharField(max_length=11, blank=True)
