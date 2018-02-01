from google.appengine.ext import deferred

from .base import ShardedCsvImporter
from ..models import CsvImportLog

from .. import cloud_storage_utils
from ..utils import chunks


class AppengineCsvImporter(ShardedCsvImporter):
    queue = 'default'
    shard_size = 100
    defer = False

    def process_rows(self, rows):
        """ Divide and conquer.  Split the file into n-sized chunks """
        for shard in chunks(rows, self.shard_size):
            if self.defer:
                deferred.defer(self.process_shard, shard, _queue=self.queue)
            else:
                self.process_shard(shard)

    def process_shard(self, shard):
        """ a shard is just a subset of rows """
        for row in shard:
            self.process_row(row)


class AuditedCsvImporter(AppengineCsvImporter):
    """
    Logs imports and their errors, creating an error file
    """
    defer = True

    def initialise(self, csvfile):
        self.import_log = CsvImportLog.objects.create(
            csv_file=csvfile,
        )

    def post_process(self):
        if self.errors is None:
            self.import_log.status = CsvImportLog.Status.SUCCESS
        else:
            import ipdb; ipdb.set_trace()
            filename = cloud_storage_utils.get_filename('bar.csv')
            rows = []
            for line_number, errors, row in self.errors:
                rows.append()

            error_csv_file = cloud_storage_utils.writerows(filename, rows)
            self.import_log.error_file = error_csv_file

        self.import_log.save()
