# Generated by Django 3.2.5 on 2022-09-01 20:54

import DataRepo.models.element_label
import datetime
import django.contrib.postgres.fields
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
                ('infusion_rate', models.FloatField(blank=True, help_text='The rate of infusion of the tracer solution in microliters/min/gram of body weight of the animal (ul/min/g).', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('genotype', models.CharField(help_text='The laboratory standardized genotype of the animal.', max_length=256)),
                ('body_weight', models.FloatField(blank=True, help_text='The weight (in grams) of the animal at the time of sample collection.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('age', models.DurationField(blank=True, help_text='The age of the animal at the time of sample collection.', null=True, validators=[django.core.validators.MinValueValidator(datetime.timedelta(0))])),
                ('sex', models.CharField(blank=True, choices=[('F', 'female'), ('M', 'male')], help_text='The sex of the animal ("male" or "female").', max_length=1, null=True)),
                ('diet', models.CharField(blank=True, help_text='The feeding descriptor for the animal [e.g. "LabDiet Rodent 5001"].', max_length=256, null=True)),
                ('feeding_status', models.CharField(blank=True, help_text='The laboratory coded dietary state for the animal, also referred to as "Animal State" (e.g. "fasted").', max_length=256, null=True)),
            ],
            options={
                'verbose_name': 'animal',
                'verbose_name_plural': 'animals',
                'ordering': ['name'],
            },
            bases=(models.Model, DataRepo.models.element_label.ElementLabel),
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
            name='Infusate',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(editable=False, help_text="A unique name or lab identifier of the infusate 'recipe' containing 1 or more tracer compounds at specific concentrations.", max_length=256, null=True, unique=True)),
                ('tracer_group_name', models.CharField(blank=True, help_text="A short name or lab identifier of refering to a group of tracer compounds, e.g '6eaas'.  There may be multiple infusate records with this group name, each referring to the same tracers at different concentrations.", max_length=20, null=True)),
            ],
            options={
                'verbose_name': 'infusate',
                'verbose_name_plural': 'infusates',
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
            name='PeakData',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('raw_abundance', models.FloatField(blank=True, help_text='The ion count of this observation.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('corrected_abundance', models.FloatField(help_text='The ion counts corrected for natural abundance of isotopomers.', validators=[django.core.validators.MinValueValidator(0)])),
                ('med_mz', models.FloatField(blank=True, help_text='The median mass/charge value of this measurement.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
                ('med_rt', models.FloatField(blank=True, help_text='The median retention time value of this measurement.', null=True, validators=[django.core.validators.MinValueValidator(0)])),
            ],
            options={
                'verbose_name': 'peak data',
                'verbose_name_plural': 'peak data',
                'ordering': ['peak_group', '-corrected_abundance'],
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
            ],
            options={
                'verbose_name': 'peak group',
                'verbose_name_plural': 'peak groups',
                'ordering': ['name'],
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
            name='Tracer',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(editable=False, help_text="A unique name or lab identifier of the tracer, e.g. 'lysine-C14'.", max_length=256, null=True, unique=True)),
                ('compound', models.ForeignKey(on_delete=django.db.models.deletion.RESTRICT, related_name='tracers', to='DataRepo.compound')),
            ],
            options={
                'verbose_name': 'tracer',
                'verbose_name_plural': 'tracers',
                'ordering': ['name'],
            },
            bases=(models.Model, DataRepo.models.element_label.ElementLabel),
        ),
        migrations.CreateModel(
            name='TracerLabel',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(editable=False, help_text='An automatically maintained identifier of a tracer label.', max_length=256, null=True)),
                ('element', models.CharField(choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur'), ('P', 'Phosphorus')], default='C', help_text='The type of atom that is labeled in the tracer compound (e.g. "C", "H", "O").', max_length=1)),
                ('count', models.PositiveSmallIntegerField(help_text='The number of labeled atoms (M+) in the tracer compound supplied to this animal.  Note that the labeled_count must be greater than or equal to the number of labeled_positions.', validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(20)])),
                ('positions', django.contrib.postgres.fields.ArrayField(base_field=models.PositiveSmallIntegerField(validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(20)]), blank=True, default=list, help_text='The known labeled atom positions in the compound.  Note that the number of known labeled positions must be less than or equal to the labeled_count.', null=True, size=None)),
                ('mass_number', models.PositiveSmallIntegerField(help_text="The sum of the number of protons and neutrons of the labeled atom, a.k.a. 'isotope', e.g. Carbon 14.  The number of protons identifies the element that this tracer is an isotope of.  The number of neutrons in the element equals the number of protons, but in an isotope, the number of neutrons will be less than or greater than the number of protons.  Note, this differs from the 'atomic number' which indicates the number of protons only.", validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(120)])),
                ('tracer', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='labels', to='DataRepo.tracer')),
            ],
            options={
                'verbose_name': 'tracer label',
                'verbose_name_plural': 'tracer labels',
                'ordering': ['tracer', 'element', 'mass_number', 'count', 'positions'],
            },
            bases=(models.Model, DataRepo.models.element_label.ElementLabel),
        ),
        migrations.CreateModel(
            name='Sample',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(help_text='The unique name of the biological sample.', max_length=256, unique=True)),
                ('date', models.DateField(default=datetime.date.today, help_text='The date the sample was collected.')),
                ('researcher', models.CharField(help_text='The name of the researcher who prepared the sample (e.g. "Alex Medina").', max_length=256)),
                ('is_serum_sample', models.BooleanField(default=False, help_text='This field indicates whether this sample is a serum sample.')),
                ('time_collected', models.DurationField(blank=True, help_text='The time, relative to an infusion timepoint, that a sample was extracted from an animal.', null=True, validators=[django.core.validators.MinValueValidator(datetime.timedelta(days=-1)), django.core.validators.MaxValueValidator(datetime.timedelta(days=7))])),
                ('animal', models.ForeignKey(help_text='The source animal from which the sample was extracted.', on_delete=django.db.models.deletion.CASCADE, related_name='samples', to='DataRepo.animal')),
                ('tissue', models.ForeignKey(help_text='The tissue type this sample was taken from.', on_delete=django.db.models.deletion.RESTRICT, related_name='samples', to='DataRepo.tissue')),
            ],
            options={
                'verbose_name': 'sample',
                'verbose_name_plural': 'samples',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='PeakGroupLabel',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('element', models.CharField(choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur'), ('P', 'Phosphorus')], default='C', help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").', max_length=1)),
                ('peak_group', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='labels', to='DataRepo.peakgroup')),
            ],
            options={
                'verbose_name': 'labeled element',
                'verbose_name_plural': 'labeled elements',
                'ordering': ['peak_group', 'element'],
            },
        ),
        migrations.AddField(
            model_name='peakgroup',
            name='peak_group_set',
            field=models.ForeignKey(help_text='The source file this PeakGroup came from.', on_delete=django.db.models.deletion.CASCADE, related_name='peak_groups', to='DataRepo.peakgroupset'),
        ),
        migrations.CreateModel(
            name='PeakDataLabel',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('element', models.CharField(choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur'), ('P', 'Phosphorus')], default='C', help_text='The type of element that is labeled in this observation (e.g. "C", "H", "O").', max_length=1)),
                ('count', models.PositiveSmallIntegerField(help_text='The number of labeled atoms (M+) observed relative to the presumed compound referred to in the peak group.', validators=[django.core.validators.MinValueValidator(0), django.core.validators.MaxValueValidator(20)])),
                ('mass_number', models.PositiveSmallIntegerField(help_text="The sum of the number of protons and neutrons of the labeled atom, a.k.a. 'isotope', e.g. Carbon 14.  The number of protons identifies the element that this tracer is an isotope of.  The number of neutrons in the element equals the number of protons, but in an isotope, the number of neutrons will be less than or greater than the number of protons.  Note, this differs from the 'atomic number' which indicates the number of protons only.", validators=[django.core.validators.MinValueValidator(1), django.core.validators.MaxValueValidator(120)])),
                ('peak_data', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='labels', to='DataRepo.peakdata')),
            ],
            options={
                'verbose_name': 'label',
                'verbose_name_plural': 'labels',
                'ordering': ['element', 'count', 'mass_number', 'peak_data'],
            },
            bases=(models.Model, DataRepo.models.element_label.ElementLabel),
        ),
        migrations.AddField(
            model_name='peakdata',
            name='peak_group',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='peak_data', to='DataRepo.peakgroup'),
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
        migrations.CreateModel(
            name='InfusateTracer',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('concentration', models.FloatField(help_text="The millimolar concentration of the tracer in a specific infusate 'recipe' (mM).", validators=[django.core.validators.MinValueValidator(0)])),
                ('infusate', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tracer_links', to='DataRepo.infusate')),
                ('tracer', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='infusate_links', to='DataRepo.tracer')),
            ],
            options={
                'verbose_name': 'infusate_tracer_link',
                'verbose_name_plural': 'infusate_tracer_links',
                'ordering': ['infusate', 'tracer', 'concentration'],
            },
        ),
        migrations.AddField(
            model_name='infusate',
            name='tracers',
            field=models.ManyToManyField(help_text="Tracers included in this infusate 'recipe' at specific concentrations.", related_name='infusates', through='DataRepo.InfusateTracer', to='DataRepo.Tracer'),
        ),
        migrations.CreateModel(
            name='FCirc',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('element', models.CharField(choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur'), ('P', 'Phosphorus')], default='C', help_text='An element that is labeled in any of the tracers in this infusate (e.g. "C", "H", "O").', max_length=1)),
                ('is_last', models.BooleanField(default=False, help_text='This field indicates whether the last peak group of this serum sample and this tracer, is the last among the serum samples/tracers for the associated animal. Maintained field. Do not edit/set.')),
                ('serum_sample', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='fcircs', to='DataRepo.sample')),
                ('tracer', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='fcircs', to='DataRepo.tracer')),
            ],
            options={
                'verbose_name': 'fcirc',
                'verbose_name_plural': 'fcircs',
                'ordering': ['serum_sample', 'tracer', 'element'],
            },
        ),
        migrations.CreateModel(
            name='CompoundSynonym',
            fields=[
                ('name', models.CharField(help_text='A synonymous name for a compound that is commonly used within the laboratory. (e.g. "palmitic acid", "hexadecanoic acid", "C16", and "palmitate" might also be synonyms for "C16:0").', max_length=256, primary_key=True, serialize=False, unique=True)),
                ('compound', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='synonyms', to='DataRepo.compound')),
            ],
            options={
                'verbose_name': 'synonym',
                'verbose_name_plural': 'synonyms',
                'ordering': ['compound', 'name'],
            },
        ),
        migrations.CreateModel(
            name='AnimalLabel',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('element', models.CharField(choices=[('C', 'Carbon'), ('N', 'Nitrogen'), ('H', 'Hydrogen'), ('O', 'Oxygen'), ('S', 'Sulfur'), ('P', 'Phosphorus')], default='C', help_text='An element that is labeled in any of the tracers in this infusate (e.g. "C", "H", "O").', max_length=1)),
                ('animal', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='labels', to='DataRepo.animal')),
            ],
            options={
                'verbose_name': 'animal_label',
                'verbose_name_plural': 'labels',
                'ordering': ['animal', 'element'],
            },
        ),
        migrations.AddField(
            model_name='animal',
            name='infusate',
            field=models.ForeignKey(help_text='The solution infused into the animal containing 1 or more tracer compounds at specific concentrations.', on_delete=django.db.models.deletion.RESTRICT, related_name='animal', to='DataRepo.infusate'),
        ),
        migrations.AddField(
            model_name='animal',
            name='studies',
            field=models.ManyToManyField(help_text='The experimental study(ies) the the animal is associated with.', related_name='animals', to='DataRepo.Study'),
        ),
        migrations.AddField(
            model_name='animal',
            name='treatment',
            field=models.ForeignKey(blank=True, help_text='The laboratory controlled label of the actions taken on an animal.', limit_choices_to={'category': 'animal_treatment'}, null=True, on_delete=django.db.models.deletion.RESTRICT, related_name='animals', to='DataRepo.protocol'),
        ),
        migrations.AddConstraint(
            model_name='tracerlabel',
            constraint=models.UniqueConstraint(fields=('tracer', 'element', 'mass_number', 'count', 'positions'), name='unique_tracerlabel'),
        ),
        migrations.AddConstraint(
            model_name='peakgrouplabel',
            constraint=models.UniqueConstraint(fields=('peak_group', 'element'), name='unique_peakgrouplabel'),
        ),
        migrations.AddConstraint(
            model_name='peakgroup',
            constraint=models.UniqueConstraint(fields=('name', 'msrun'), name='unique_peakgroup'),
        ),
        migrations.AddConstraint(
            model_name='peakdatalabel',
            constraint=models.UniqueConstraint(fields=('peak_data', 'element'), name='unique_peakdata'),
        ),
        migrations.AddConstraint(
            model_name='msrun',
            constraint=models.UniqueConstraint(fields=('researcher', 'date', 'protocol', 'sample'), name='unique_msrun'),
        ),
        migrations.AddConstraint(
            model_name='infusatetracer',
            constraint=models.UniqueConstraint(fields=('infusate', 'tracer', 'concentration'), name='unique_infusate_tracer'),
        ),
        migrations.AddConstraint(
            model_name='fcirc',
            constraint=models.UniqueConstraint(fields=('serum_sample', 'tracer', 'element'), name='unique_fcirc'),
        ),
        migrations.AddConstraint(
            model_name='animallabel',
            constraint=models.UniqueConstraint(fields=('animal', 'element'), name='unique_animal_label'),
        ),
    ]
