# Generated by Django 3.1.8 on 2021-04-29 18:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0008_changed_unique_constraints_coding'),
    ]

    operations = [
        migrations.AlterField(
            model_name='msrun',
            name='date',
            field=models.DateField(),
        ),
    ]
