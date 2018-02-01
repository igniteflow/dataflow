import StringIO

from django import forms
from django.test import TestCase

from dataflow.importers.base import CsvImporter, ShardedCsvImporter
from dataflow.tests.models import Contact


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


class ShardedContactCsvImporter(ShardedCsvImporter):
    form_class = ContactForm
    fields = ['email', 'name']
    model = Contact
    lookup_column_name = 'email'
    lookup_model_attr = 'email'


VALID_CSV_DATA = """email,name
phil@example.com,phil
bob@example.com,bob
"""

INVALID_CSV_DATA = """email,name
1,phil
bob@example.com,bob
"""


class TestCsvImporter(TestCase):

    def test_import(self):
        csv_file = StringIO.StringIO(VALID_CSV_DATA)
        contact_importer = ContactCsvImporter()
        contact_importer.process_file(csv_file)
        self.assertEqual(
            Contact.objects.count(), 2
        )

    def test_invalid(self):
        csv_file = StringIO.StringIO(INVALID_CSV_DATA)
        contact_importer = ContactCsvImporter()
        contact_importer.process_file(csv_file)
        self.assertEqual(
            contact_importer.errors,
            [[1, {'email': [u'Enter a valid email address.']}, {'_number': 1, 'email': '1', 'name': 'phil'}]]
        )


class TestShardedCsvImporter(TestCase):

    def test_import(self):
        csv_file = StringIO.StringIO(VALID_CSV_DATA)
        contact_importer = ShardedContactCsvImporter()
        contact_importer.process_file(csv_file)
        self.assertEqual(
            Contact.objects.count(), 2
        )
