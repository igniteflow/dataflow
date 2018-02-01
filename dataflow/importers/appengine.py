from google.appengine.ext import deferred

from .base import CsvImporter


class AppengineCsvImporter(CsvImporter):
    defer = False
    queue = 'default'

    def process_shard(self, shard):
        if self.defer:
            deferred.defer(
                super(AppengineCsvImporter, self).process_shard, shard, _queue=self.queue
            )
        else:
            super(AppengineCsvImporter, self).process_shard(shard)
