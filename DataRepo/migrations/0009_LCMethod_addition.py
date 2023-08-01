# Generated by Django 3.2.20 on 2023-08-01 16:55

import datetime
import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0008_alter_infusate_tracer_uniq_constraint'),
    ]

    operations = [
        migrations.CreateModel(
            name='LCMethod',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('chromatographic_technique', models.CharField(help_text='Laboratory-defined type of the liquid chromatography method.(e.g. HILIC, Reverse Phase)', max_length=256)),
                ('description', models.TextField(help_text='Full text of the liquid chromatography method.')),
                ('run_length', models.DurationField(blank=True, help_text='Time duration to complete the mass spectrometry sequence.', null=True, validators=[django.core.validators.MinValueValidator(datetime.timedelta(0)), django.core.validators.MaxValueValidator(datetime.timedelta(days=1))])),
            ],
            options={
                'verbose_name': 'liquid chromatography method',
                'verbose_name_plural': 'liquid chromatography methods',
                'ordering': ['chromatographic_technique'],
            },
        ),
        migrations.AddConstraint(
            model_name='lcmethod',
            constraint=models.UniqueConstraint(fields=('chromatographic_technique', 'description', 'run_length'), name='datarepo_lcmethod_record_unique'),
        ),
        migrations.AddConstraint(
            model_name='lcmethod',
            constraint=models.CheckConstraint(check=models.Q(('description__length__gt', 0)), name='datarepo_lcmethod_description_not_empty'),
        ),
    ]
