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
    errors = None

    def process_file(self, csvfile):
        """ Import the file in one pass """
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]

        self.initialise(csvfile)

        # annotate rows with row number for error reporting
        for i, row in enumerate(rows, start=1):
            row['_number'] = i

        self.process_rows(rows)
        self.post_process()

    def process_rows(self, rows):
        for row in rows:
            self.process_row(row)

    def initialise(self, csvfile):
        pass

    def post_process(self):
        if self.errors:
            logging.error(self.errors)
        else:
            logging.info('File was imported successfully')

    def process_row(self, row):
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
            self.row_error(form, row)

    def get_object_from_row(self, row):
        try:
            return self.model.objects.get(**{self.lookup_model_attr: row[self.lookup_column_name]})
        except self.model.DoesNotExist as e:
            logging.error(e)

    def row_success(self, form):
        form.save()

    def row_error(self, form, row):
        if self.errors is None:
            self.errors = []
        self.errors.append([form.data['_number'], form.errors, row])


class ShardedCsvImporter(CsvImporter):
    shard_size = 100

    def process_rows(self, rows):
        """ Divide and conquer.  Split the file into n-sized chunks """
        for shard in chunks(rows, self.shard_size):
            self.process_shard(shard)

    def process_shard(self, shard):
        """ a shard is just a subset of rows """
        for row in shard:
            self.process_row(row)
