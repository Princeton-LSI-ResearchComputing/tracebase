# Generated by Django 3.2.5 on 2022-11-10 22:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0004_alter_animal_infusate'),
    ]

    operations = [
        migrations.AlterField(
            model_name='study',
            name='description',
            field=models.TextField(blank=True, help_text='A long form description for the study which may include the experimental design process, citations, and other relevant details.', null=True),
        ),
    ]
