# Generated by Django 3.2.4 on 2021-09-20 19:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('DataRepo', '0006_auto_20210909_1607'),
    ]

    operations = [
        migrations.AddField(
            model_name='tissue',
            name='description',
            field=models.TextField(blank=True, help_text='Description of this tissue type.'),
        ),
    ]
