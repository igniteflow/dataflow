import StringIO

from django import forms
from django.test import TestCase

from dataflow.importers.base import CsvImporter
from .models import Contact


class ContactForm(forms.ModelForm):
    class Meta:
        model = Contact
        fields = ['email', 'name']


class ContactCsvImporter(CsvImporter):
    form_class = ContactForm
    fields = ['email', 'name']
    model = Contact
    lookup_column_name = 'email'
    lookup_model_attr = 'email'


VALID_CSV_DATA = """email,name
phil@example.com,phil
bob@example.com,bob
"""

class TestCsvImporter(TestCase):

    def test_import(self):
        csv_file = StringIO.StringIO(VALID_CSV_DATA)
        contact_importer = ContactCsvImporter()
        contact_importer.import_csvfile(csv_file)
        self.assertEqual(
            Contact.objects.count(), 2
        )