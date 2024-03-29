# Generated by Django 3.2.5 on 2022-09-14 18:26

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='animal',
            name='last_serum_sample',
            field=models.ForeignKey(blank=True, db_column='last_serum_sample_id', help_text='Automatically maintained field. Shortcut to the last serum sample.', null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='animals', to='DataRepo.sample'),
        ),
    ]
