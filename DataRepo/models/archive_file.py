from django.db import models
from django.template.defaultfilters import slugify


class DataTypeManager(models.Manager):
    def get_by_natural_key(self, code):
        """Allows Django to get objects by a natural key instead of the primary key"""
        return self.get(code=code)


class DataType(models.Model):
    """Possible values for ArchiveFile data types"""

    id = models.AutoField(primary_key=True)

    code = models.CharField(
        max_length=32,
        unique=True,
        help_text="Short code for types of data stored in ArchiveFile",
    )
    name = models.CharField(
        max_length=256, unique=True, help_text="Human readable name of the data type"
    )
    description = models.CharField(
        max_length=2048, null=True, blank=True, help_text="Description of the data type"
    )

    objects = DataTypeManager()

    def natural_key(self):
        """Django can use the natural_key() method to serialize any foreign
        key reference to objects of the type that defines the method.

        Must return a tuple."""
        return (self.code,)


class DataFormatManager(models.Manager):
    def get_by_natural_key(self, code):
        """Allows Django to get objects by a natural key instead of the primary key"""
        return self.get(code=code)


class DataFormat(models.Model):
    """Possible values for ArchiveFile data formats"""

    id = models.AutoField(primary_key=True)

    code = models.CharField(
        max_length=32,
        unique=True,
        help_text="Short code of data formats stored in ArchiveFile",
    )
    name = models.CharField(
        max_length=256, unique=True, help_text="Human readable name of the data format"
    )
    description = models.CharField(
        max_length=2048,
        null=True,
        blank=True,
        help_text="Description of the data format",
    )

    objects = DataFormatManager()

    def natural_key(self):
        """Django can use the natural_key() method to serialize any foreign
        key reference to objects of the type that defines the method.

        Must return a tuple."""
        return (self.code,)


def data_type_path(instance, filename):
    date_folder = instance.imported_timestamp.strftime("%Y-%m")
    data_type_folder = slugify(instance.data_type.code)
    filename_clean = slugify(filename)
    return f"archive_files/{date_folder}/{data_type_folder}/{filename_clean}"


class ArchiveFile(models.Model):
    """Store the file location, checksum, datatype, and format of files."""

    # Instance / model fields
    id = models.AutoField(primary_key=True)

    filename = models.CharField(
        max_length=512,
        help_text="The user specified file name",
    )

    checksum = models.CharField(
        max_length=256,
        unique=True,
        help_text="The sha1 checksum",
    )

    imported_timestamp = models.DateTimeField(
        auto_now_add=True,
        help_text="The timestamp for when the source datafile was imported.",
    )

    # Store the file https://www.youtube.com/watch?v=O5YkEFLXcRg
    # https://docs.djangoproject.com/en/3.2/topics/files/
    file_location = models.FileField(
        upload_to=data_type_path,
        null=True,
        blank=True,
        help_text="The path of the archived file on the filesystem",
    )

    data_type = models.ForeignKey(DataType, on_delete=models.PROTECT)

    data_format = models.ForeignKey(DataFormat, on_delete=models.PROTECT)
