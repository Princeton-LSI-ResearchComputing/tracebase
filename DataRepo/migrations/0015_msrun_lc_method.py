# Generated by Django 3.2.20 on 2023-09-20 19:11

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0014_lcmethod_fixture_update'),
    ]

    operations = [
        migrations.AddField(
            model_name='msrun',
            name='lc_method',
            field=models.ForeignKey(help_text='The liquid chromatography protocol that was used for this mass spectrometer run.', null=True, blank=True, on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.lcmethod'),
        ),
    ]
