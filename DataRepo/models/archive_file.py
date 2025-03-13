from __future__ import annotations

import hashlib
import os
from pathlib import Path

from django.core.files import File
from django.db import ProgrammingError, models, transaction
from django.db.models.signals import post_delete
from django.dispatch import receiver
from django.forms import model_to_dict
from django.utils.text import get_valid_filename

from DataRepo.models.utilities import exists_in_db


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

    def __str__(self):
        return self.name


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

    def __str__(self):
        return self.name


def data_type_path(instance, filename):
    date_folder = instance.imported_timestamp.strftime("%Y-%m")
    data_type_folder = get_valid_filename(instance.data_type.code)
    filename_clean = get_valid_filename(filename)
    return f"archive_files/{date_folder}/{data_type_folder}/{filename_clean}"


class ArchiveFileQuerySet(models.QuerySet):
    @transaction.atomic
    def get_or_create(self, is_binary=None, **kwargs) -> tuple[ArchiveFile, bool]:
        """An override of get_or_create (if provided a Path object or a path string for file_location) can avoid unique
        constraint violations due to the fact that Django appends a short random hash string to file names at the end of
        the file_location to ensure uniqueness without worrying about the path.

        Also provides the following conveniences:
        - Accepts either a file path string, Path object, or File object for file_location.
          - If file_location is a File object, there are no special features. It just calls super.get_or_create.
          - Otherwise, it has the features described below...
        - Checks or fills in the checksum (if a valid file_location is provided).
        - Fills in the filename (if None).
        - If the data_type is a string, it retrieves the DataType record using that string as DataType.code.
        - If the data_format is a string, it retrieves the DataFormat record using that string as DataFormat.code.
        - Performs a full_clean if created.

        Args:
            kwargs (dict): The field names (keys) and values.
                file_location (str|Path|File): Required.
                filename (Optional[str]): Can be derived from file_location.
                checksum (Optional[str]): Can be derived from file_location.
                data_type (str|DataType): Required.
                data_format (str|DataFormat): Required.
            is_binary (boolean): An optional way to indicate if the file is binary or not, overriding the automatic
                guess/determination.
        Exceptions:
            FileNotFoundError: if file_location is supplied and its file does not exist.
            ValueError: if both file_location and checksum are None or unsupplied.
            ValueError: if the computed checksum does not match the supplied checksum.
            TypeError: if file_location is not (str|Path|File).
            DataType.DoesNotExist: if data_type is a string that doesn't exist as a DataType.code in the DB.
            DataFormat.DoesNotExist: if data_format is a string that doesn't exist as a DataFormat.code in the DB.
        Returns:
            archivefile_rec (ArchiveFile)
            created (boolean)
        """

        file_location = kwargs.pop("file_location", None)

        # Fill in the data_type, using the provided value as the code if it is a string
        data_type = kwargs.get("data_type")
        if data_type is not None and isinstance(data_type, str):
            kwargs["data_type"] = DataType.objects.get(code=data_type)

        # Fill in the data_format, using the provided value as the code if it is a string
        data_format = kwargs.get("data_format")
        if data_format is not None and isinstance(data_format, str):
            kwargs["data_format"] = DataFormat.objects.get(code=data_format)

        if file_location is None:
            if kwargs.get("checksum", None) is None:
                raise ValueError(
                    "A checksum is required if the supplied file path is not an existing file."
                )
            return super().get_or_create(**kwargs)

        if isinstance(file_location, File):
            # No special features if this is already a Django File object
            kwargs["file_location"] = file_location
            return super().get_or_create(**kwargs)
        elif isinstance(file_location, Path):
            path_obj = file_location
        elif isinstance(file_location, str):
            path_obj = Path(file_location)
        else:
            raise TypeError(
                f"file_location must be either a Path object or string, not {type(file_location).__name__}."
            )

        # Compute the filename if not provided, but allow it to differ from the name of the file_location
        if not path_obj.is_file():
            raise FileNotFoundError(f"No such file: {str(path_obj)}")

        if kwargs.get("filename") is None:
            kwargs["filename"] = path_obj.name

        # Compute and/or check the checksum.
        supplied_checksum = kwargs.get("checksum", None)
        computed_checksum = ArchiveFile.hash_file(path_obj)
        if supplied_checksum is not None and computed_checksum != supplied_checksum:
            raise ValueError(
                f"The supplied checksum [{supplied_checksum}] does not match the computed checksum "
                f"[{computed_checksum}]."
            )
        elif supplied_checksum is None:
            kwargs["checksum"] = computed_checksum

        # When an ArchiveFile record is get_or_create'd, and you expect a `get` to occur, the handling of the
        # `file_location` value results in an unexpected outcome.  Instead of `get`ting the record, since the
        # path and name is the same, the Django code appends a short random hash value to the file name before the
        # file extension.  This results in the `get_or_create` method trying to "create" a record, because one
        # of the field values differ.  This then results in a unique constraint violation, because the hash must
        # be unique.  So to work around this, we will perform a `get_or_create` *without* the `file_location`
        # value, and instead add the file after, only *if* the record was created...
        archivefile_rec, created = super().get_or_create(**kwargs)
        if created or archivefile_rec.file_location is None:
            # Create a File object
            mode = "r"
            if (is_binary is not None and is_binary) or ArchiveFile.file_is_binary(
                file_location
            ):
                mode = "rb"
            with path_obj.open(mode=mode) as file_handle:
                tmp_file_location = File(file_handle, name=kwargs["filename"])

                if created:
                    archivefile_rec.file_location = tmp_file_location
                    archivefile_rec.full_clean()
                    archivefile_rec.save()
                elif archivefile_rec.file_location is None:
                    # Re-do the get_or_create WITH the file_location (since we know a record exists WITHOUT a value for
                    # file_location) in order to generate the expected/usual exception about a unique-constraint
                    # violation
                    kwargs["file_location"] = tmp_file_location
                    archivefile_rec, created = super().get_or_create(**kwargs)
                    if created:
                        archivefile_rec.full_clean()

        return archivefile_rec, created


class ArchiveFile(models.Model):
    """Store the file location, checksum, datatype, and format of files."""

    objects: ArchiveFileQuerySet = ArchiveFileQuerySet().as_manager()
    detail_name = "archive_file_detail"

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

    class Meta:
        verbose_name = "Archive File"
        verbose_name_plural = "Archive Files"
        ordering = [
            "filename",
            "data_type__name",
            "data_format__name",
            "imported_timestamp",
        ]

    def __str__(self):
        return f"{self.filename} ({self.checksum})"

    @classmethod
    def file_is_binary(cls, filepath):
        """Guesses whether a file is binary or text.  Partially based on:
        https://stackoverflow.com/a/7392391/2057516
        Args:
            filepath (string): The path to a file.
        Exceptions:
            None
        Returns:
            is_binary (boolean)
        """
        textchars = bytearray(
            {7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)) - {0x7F}
        )
        file_sample = None
        is_binary = False
        try:
            with open(filepath, "rb") as fl:
                file_sample = fl.read(1024)
            is_binary = bool(file_sample.translate(None, textchars))
        except Exception:
            # Fall back to guessing by extension
            supported_binary_exts = ["xlsx", "xls"]
            _, ext = os.path.splitext(filepath)
            if ext in supported_binary_exts:
                is_binary = True
        return is_binary

    @classmethod
    def hash_file(cls, path_obj: File):
        """Determine the SHA-1 hash of a file.  Note, it does not matter if the file is binary or not.
        Args:
            path_obj (Path)
        Exceptions:
            None
        Returns:
            hex (string): the hex representation of digest
        """
        hash_obj = hashlib.sha1()

        with path_obj.open("rb") as file_handle:
            chunk = file_handle.read(1024)
            hash_obj.update(chunk)
            while chunk != b"":
                chunk = file_handle.read(1024)
                hash_obj.update(chunk)

        return hash_obj.hexdigest()

    def get_absolute_url(self):
        """Get the URL to the detail page.
        See: https://docs.djangoproject.com/en/5.1/ref/models/instances/#get-absolute-url
        """
        from django.urls import reverse

        return reverse(self.detail_name, kwargs={"pk": self.pk})


@receiver(post_delete, sender=ArchiveFile)
def post_archive_file_delete_commit(**kwargs):
    """Schedule a call to delete_archive_file upon deletion commit (when we are guaranteed that the transaction won't be
    rolled back).

    The purpose is to delete the file associated with an ArchiveFile record when it is being deleted.

    NOTE: There are scenarios however where files in the archive can be orphaned and not cleaned up.  For example, if
    rec.file_location is changed (i.e. delete is not called).

    References:
        https://stackoverflow.com/a/52703242/2057516
    Args:
        kwargs (dict)
    Exceptions:
        None
    Returns:
        None
    """
    transaction.on_commit(lambda: delete_archive_file(kwargs["instance"]))


def delete_archive_file(deleted_rec: ArchiveFile) -> None:
    """Given a deleted record instance (or a record being deleted during a safe transaction commit), delete the file on
    disk associated with the record.

    References:
        https://stackoverflow.com/a/16041527/2057516
    Args:
        deleted_rec (ArchiveFile)
    Exceptions:
        None
    RTeturns:
        None
    """
    if deleted_rec.file_location and os.path.isfile(deleted_rec.file_location.path):
        if exists_in_db(deleted_rec):
            raise ProgrammingError(
                f"Calling delete_archive_file on existing database record: {model_to_dict(deleted_rec)} is not allowed."
            )
        else:
            os.remove(deleted_rec.file_location.path)
