# Generated by Django 3.2.5 on 2021-10-28 20:51

import DataRepo.models
import datetime
import django.core.validators
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Animal',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='A unique name or lab identifier of the source animal for a series of studied samples.', max_length=256, unique=True)),
                ('tracer_labeled_atom', models.CharField(blank=True, choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur')], default='C', help_text='The type of atom that is labeled in the tracer compound (e.g. "C", "H", "O").', max_length=1, null=True)),
                ('tracer_labeled_count', models.PositiveSmallIntegerField(blank=True, help_text='The number of labeled atoms (M+) in the tracer compound supplied to this animal.', null=True, validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(20)])),
                ('tracer_infusion_rate', models.FloatField(blank=True, help_text='The rate of tracer infusion in microliters/min/gram of body weight of the animal (ul/min/g).', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('tracer_infusion_concentration', models.FloatField(blank=True, help_text='The millimolar concentration of the tracer in the solution that was infused (mM).', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('genotype', models.CharField(help_text='The laboratory standardized genotype of the animal.', max_length=256)),
                ('body_weight', models.FloatField(blank=True, help_text='The weight (in grams) of the animal at the time of sample collection.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('age', models.DurationField(blank=True, help_text='The age of the animal at the time of sample collection.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('sex', models.CharField(blank=True, choices=[('F', 'female'), ('M', 'male')], help_text='The sex of the animal ("male" or "female").', max_length=1, null=True)),
                ('diet', models.CharField(blank=True, help_text='The feeding descriptor for the animal [e.g. "LabDiet Rodent 5001"].', max_length=256, null=True)),
                ('feeding_status', models.CharField(blank=True, help_text='The laboratory coded dietary state for the animal, also referred to as "Animal State" (e.g. "fasted").', max_length=256, null=True)),
            ],
            options={
                'verbose_name': 'animal',
                'verbose_name_plural': 'animals',
                'ordering': ['name'],
            },
            bases=(models.Model, DataRepo.models.TracerLabeledClass),
        ),
        migrations.CreateModel(
            name='Compound',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='The compound name that is commonly used in the laboratory (e.g. "glucose", "C16:0", etc.).', max_length=256, unique=True)),
                ('formula', models.CharField(help_text='The molecular formula of the compound (e.g. "C6H12O6", "C16H32O2", etc.).', max_length=256)),
                ('hmdb_id', models.CharField(help_text='A unique identifier for this compound in the Human Metabolome Database (https://hmdb.ca/metabolites).', max_length=11, unique=True, verbose_name='HMDB ID')),
            ],
            options={
                'verbose_name': 'compound',
                'verbose_name_plural': 'compounds',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='MSRun',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('researcher', models.CharField(help_text='The name of the researcher who ran the mass spectrometer.', max_length=256)),
                ('date', models.DateField(help_text='The date that the mass spectrometer was run.')),
            ],
            options={
                'verbose_name': 'mass spectrometry run',
                'verbose_name_plural': 'mass spectrometry runs',
                'ordering': ['date', 'researcher', 'sample__name', 'protocol__name'],
            },
        ),
        migrations.CreateModel(
            name='PeakGroupSet',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('filename', models.CharField(help_text='The unique name of the source-file or dataset containing a researcher-defined set of peak groups and their associated data', max_length=256, unique=True)),
                ('imported_timestamp', models.DateTimeField(auto_now_add=True, help_text='The timestamp for when the source datafile was imported.')),
            ],
            options={
                'verbose_name': 'peak group set',
                'verbose_name_plural': 'peak group sets',
                'ordering': ['filename'],
            },
        ),
        migrations.CreateModel(
            name='Protocol',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='Unique name of the protocol.', max_length=256, unique=True)),
                ('description', models.TextField(blank=True, help_text="Full text of the protocol's methods.")),
                ('category', models.CharField(choices=[('msrun_protocol', 'LC-MS Run Protocol'), ('animal_treatment', 'Animal Treatment')], help_text='Classification of the protocol, e.g. an animal treatment or MSRun procedure.', max_length=256)),
            ],
            options={
                'verbose_name': 'protocol',
                'verbose_name_plural': 'protocols',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Study',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='A succinct name for the study, which is a collection of one or more series of animals and their associated data.', max_length=256, unique=True)),
                ('description', models.TextField(blank=True, help_text='A long form description for the study which may include the experimental design process, citations, and other relevant details.')),
            ],
            options={
                'verbose_name': 'study',
                'verbose_name_plural': 'studies',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Tissue',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='The laboratory standardized name for this tissue type (e.g. "serum", "brain", "liver").', max_length=256, unique=True)),
                ('description', models.TextField(blank=True, help_text='Description of this tissue type.')),
            ],
            options={
                'verbose_name': 'tissue',
                'verbose_name_plural': 'tissues',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Sample',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='The unique name of the biological sample.', max_length=256, unique=True)),
                ('date', models.DateField(default=datetime.date.today, help_text='The date the sample was collected.')),
                ('researcher', models.CharField(help_text='The name of the researcher who prepared the sample (e.g. "Alex Medina").', max_length=256)),
                ('time_collected', models.DurationField(blank=True, help_text='The time, relative to an infusion timepoint, that a sample was extracted from an animal.', null=True, validators=[django.core.validators.MinValueValidator(datetime.timedelta(days=-1)), django.core.validators.MaxValueValidator(datetime.timedelta(days=7))])),
                ('animal', models.ForeignKey(help_text='The source animal from which the sample was extracted.', on_delete=django.db.models.deletion.CASCADE, related_name='samples', to='DataRepo.animal')),
                ('tissue', models.ForeignKey(help_text='The tissue type this sample was taken from.', on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.tissue')),
            ],
            options={
                'verbose_name': 'sample',
                'verbose_name_plural': 'samples',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='PeakGroup',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='The compound or isomer group name (e.g. "citrate/isocitrate", "glucose").', max_length=256)),
                ('formula', models.CharField(help_text='The molecular formula of the compound (e.g. "C6H12O6").', max_length=256)),
                ('compounds', models.ManyToManyField(help_text='The compound(s) that this PeakGroup is presumed to represent.', related_name='peak_groups', to='DataRepo.Compound')),
                ('msrun', models.ForeignKey(help_text='The MS Run this PeakGroup belongs to.', on_delete=django.db.models.deletion.CASCADE, related_name='peak_groups', to='DataRepo.msrun')),
                ('peak_group_set', models.ForeignKey(help_text='The source file this PeakGroup came from.', on_delete=django.db.models.deletion.CASCADE, related_name='peak_groups', to='DataRepo.peakgroupset')),
            ],
            options={
                'verbose_name': 'peak group',
                'verbose_name_plural': 'peak groups',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='PeakData',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('labeled_element', models.CharField(blank=True, choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur')], default='C', help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").', max_length=1, null=True)),
                ('labeled_count', models.PositiveSmallIntegerField(blank=True, help_text='The number of labeled atoms (M+) observed relative to the presumed compound referred to in the peak group.', null=True, validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(20)])),
                ('raw_abundance', models.FloatField(blank=True, help_text='The ion count of this observation.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('corrected_abundance', models.FloatField(help_text='The ion counts corrected for natural abundance of isotopomers.', validators=[django.core.validators.MinValueValidator(0)])),
                ('med_mz', models.FloatField(blank=True, help_text='The median mass/charge value of this measurement.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('med_rt', models.FloatField(blank=True, help_text='The median retention time value of this measurement.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('peak_group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='peak_data', to='DataRepo.peakgroup')),
            ],
            options={
                'verbose_name': 'peak data',
                'verbose_name_plural': 'peak data',
                'ordering': ['peak_group', 'labeled_count'],
            },
            bases=(models.Model, DataRepo.models.TracerLabeledClass),
        ),
        migrations.AddField(
            model_name='msrun',
            name='protocol',
            field=models.ForeignKey(help_text='The protocol that was used for this mass spectrometer run.', limit_choices_to={'category': 'msrun_protocol'}, on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.protocol'),
        ),
        migrations.AddField(
            model_name='msrun',
            name='sample',
            field=models.ForeignKey(help_text='The sample that was run on the mass spectrometer.', on_delete=django.db.models.deletion.RESTRICT, related_name='msruns', to='DataRepo.sample'),
        ),
        migrations.AddField(
            model_name='animal',
            name='studies',
            field=models.ManyToManyField(help_text='The experimental study(ies) the the animal is associated with.', related_name='animals', to='DataRepo.Study'),
        ),
        migrations.AddField(
            model_name='animal',
            name='tracer_compound',
            field=models.ForeignKey(help_text='The compound which was used as the tracer (i.e. infusate). The tracer is the labeled compound that is infused into the animal.', null=True, on_delete=django.db.models.deletion.RESTRICT, to='DataRepo.compound'),
        ),
        migrations.AddField(
            model_name='animal',
            name='treatment',
            field=models.ForeignKey(blank=True, help_text='The laboratory controlled label of the actions taken on an animal.', limit_choices_to={'category': 'animal_treatment'}, null=True, on_delete=django.db.models.deletion.RESTRICT, related_name='animals', to='DataRepo.protocol'),
        ),
        migrations.AddConstraint(
            model_name='peakgroup',
            constraint=models.UniqueConstraint(fields=('name', 'msrun'), name='unique_peakgroup'),
        ),
        migrations.AddConstraint(
            model_name='peakdata',
            constraint=models.UniqueConstraint(fields=('peak_group', 'labeled_element', 'labeled_count'), name='unique_peakdata'),
        ),
        migrations.AddConstraint(
            model_name='msrun',
            constraint=models.UniqueConstraint(fields=('researcher', 'date', 'protocol', 'sample'), name='unique_msrun'),
        ),
    ]
