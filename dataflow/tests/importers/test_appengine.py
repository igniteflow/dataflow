import StringIO

from django import forms

from djangae.test import TestCase

from dataflow.importers.appengine import AppengineCsvImporter, AuditedCsvImporter
from dataflow.tests.models import Contact
from dataflow.models import CsvImportLog


class ContactForm(forms.ModelForm):
    class Meta:
        model = Contact
        fields = ['email', 'name']


class ShardedContactCsvImporter(AppengineCsvImporter):
    form_class = ContactForm
    fields = ['email', 'name']
    model = Contact
    lookup_column_name = 'email'
    lookup_model_attr = 'email'


class DeferredContactImport(ShardedContactCsvImporter):
    defer = True


class AuditedContactCsvImporter(AuditedCsvImporter):
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


class TestDeferredShardedCsvImporter(TestCase):

    def test_import(self):
        csv_file = StringIO.StringIO(VALID_CSV_DATA)
        contact_importer = ShardedContactCsvImporter()
        contact_importer.process_file(csv_file)
        self.assertEqual(
            Contact.objects.count(), 2
        )

    def test_deferred_import(self):
        csv_file = StringIO.StringIO(VALID_CSV_DATA)
        contact_importer = DeferredContactImport()
        contact_importer.process_file(csv_file)
        self.assertNumTasksEquals(1, queue_name=contact_importer.queue)
        self.process_task_queues()
        self.assertEqual(
            Contact.objects.count(), 2
        )


class AuditedCsvImporterTest(TestCase):

    def test_log_created(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            '..',
            'tests/files/valid.csv',
        )
        with open(path) as csv_file:
            contact_importer = AuditedContactCsvImporter()
            contact_importer.process_file(csv_file)
            self.assertEqual(
                CsvImportLog.objects.count(), 1
            )
