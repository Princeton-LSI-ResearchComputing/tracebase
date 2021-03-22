from django.db import models


class Compound(models.Model):
    # Class variables
    HMDB_CPD_URL = "https://hmdb.ca/metabolites"

    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    formula = models.CharField(max_length=256)

    # ID to serve as an external link to record using HMDB_CPD_URL
    hmdb_id = models.CharField(max_length=11, unique=True)

    @property
    def hmdb_url(self):
        "Returns the url to the compound's hmdb record"
        return f"{self.HMDB_CPD_URL}/{self.hmdb_id}"

class Protocol(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    description = models.TextField(blank=True)

class Sample(models.Model):
    ##
    ## Placeholder to allow creation of class MSRun
    ##

    # Instance / model fields
    id = models.AutoField(primary_key=True)

class MSRun(models.Model):
    # Instance / model fields
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=256, unique=True)
    date = models.DateField(auto_now=False, auto_now_add=True, editable=True)
    # Don't allow a Protocol to be deleted if an MSRun links to it
    protocol_id = models.ForeignKey(Protocol,on_delete=models.RESTRICT)
    # Don't allow a Sample to be deleted if an MSRun links to it
    sample_id = models.ForeignKey(Sample,on_delete=models.RESTRICT)
