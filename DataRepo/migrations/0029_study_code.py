# Generated by Django 3.2.20 on 2024-01-08 21:26

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0028_alter_peakgroup_msrunsample_and_msrunsample_polarity'),
    ]

    operations = [
        migrations.AddField(
            model_name='study',
            name='code',
            field=models.CharField(blank=True, help_text='A 2 to 6 character unique readable alphanumeric code for the study, to be used as a prefix for animal names, sample names, etc if necessary, to make them unique.', max_length=6, null=True, unique=True, validators=[django.core.validators.MinLengthValidator(2), django.core.validators.RegexValidator('^[0-9a-zA-Z]+$', 'Only alphanumeric characters are allowed.')]),
        ),
    ]
