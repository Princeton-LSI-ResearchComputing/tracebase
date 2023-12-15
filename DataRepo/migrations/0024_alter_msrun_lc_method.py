# Generated by Django 3.2.5 on 2023-09-22 21:29

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0023_alter_msrunsample_ms_data_files'),
    ]

    operations = [
        # Even though every MSRun record in the database is assured to have a value for MSRun.lc_method, we must set a default so that AlterField will allow us to set null=False and blank=False
        migrations.AlterField(
            model_name='msrun',
            name='lc_method',
            field=models.ForeignKey(default=0, help_text='The liquid chromatography protocol that was used for this mass spectrometer run.', on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.lcmethod'),
        ),
        # Now we can remove that default
        migrations.AlterField(
            model_name='msrun',
            name='lc_method',
            field=models.ForeignKey(help_text='The liquid chromatography protocol that was used for this mass spectrometer run.', on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.lcmethod'),
        ),
    ]