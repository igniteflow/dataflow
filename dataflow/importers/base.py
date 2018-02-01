import csv
import logging

from ..utils import chunks


class CsvImporter(object):
    form_class = None
    fields = None
    model = None
    update_existing = True

    lookup_column_name = None
    lookup_model_attr = 'pk'

    def import_csvfile(self, csvfile):
        """ Import the file in one pass """
        reader = csv.DictReader(csvfile)
        for row_number, row in enumerate(reader):
            row['_number'] = row_number
            self.import_row(row)

    def import_row(self, row):
        form_kwargs = {'data': row}
        if self.fields:
            form_kwargs['data'] = {k: v for k, v in row.items() if k in self.fields + ['_number']}
        
        if self.update_existing:
            instance = self.get_object_from_row(row)
            if instance:
                form_kwargs['instance'] = instance
        
        form = self.form_class(**form_kwargs)
        if form.is_valid():
            self.row_success(form)
        else:
            self.row_error(form)

    def get_object_from_row(self, row):
        try:
            return self.model.objects.get(**{self.lookup_model_attr: row[self.lookup_column_name]})
        except self.model.DoesNotExist as e:
            logging.error(e)

    def row_success(self, form):
        form.save()
        logging.info(u'Row {} saved: {}'.format(form.data['_number'], form.instance))

    def row_error(self, form):
        logging.error(u'Error on row {}: {}'.format(form.data['_number'], form.errors))


class ShardedCsvImporter(CsvImporter):
    shard_size = 100

    def chunked_import(self, csvfile):
        """ Divide and conquer.  Split the file into n-sized chunks """
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]
        for shard in chunks(rows, self.shard_size):
            self.process_shard(shard)

    def process_shard(self, shard):
        """ a shard is just a subset of rows """
        for row in shard:
            self.import_row(row)