from django import forms
from django.forms import ModelForm
from django.forms import modelformset_factory
from .models import Compound


class CompoundForm(forms.ModelForm):
    class Meta:
        model = Compound
        fields = [ 'name', 'formula', 'hmdb_id'] 
#       fields = '__all__'
#       exclude = [ 'id' ]

CompoundFormSet = modelformset_factory(Compound, fields=('name', 'formula', 'hmdb_id'), extra=2)

