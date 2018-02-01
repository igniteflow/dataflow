import unicodecsv
import cloudstorage as gcs

from contextlib import contextmanager
from django.conf import settings
from google.appengine.api import app_identity


@contextmanager
def gcs_file(filename, mode='r', content_type='text/plain', options=None):
    retry_params = gcs.RetryParams(backoff_factor=1.1)
    open_kwargs = {
        'retry_params': retry_params,
    }
    if mode == 'w':
        open_kwargs.update({
            'content_type': content_type,
            'options': options or {},
        })

    gcs_file = gcs.open(
        filename,
        mode,
        **open_kwargs
    )
    yield gcs_file
    gcs_file.close()


def get_filename(filename):
    """
    returns a Cloud Storage 'filename' which is a full path
    e.g. /my_bucket/my_filename
    """
    return '{}/{}'.format(
        bucket_name(),
        filename,
    )


def bucket_name(self):
    if hasattr(settings, 'CLOUD_STORAGE_BUCKET'):
        bucket = settings.CLOUD_STORAGE_BUCKET
    else:
        bucket = app_identity.get_default_gcs_bucket_name()

    return '/{}'.format(bucket)


def writerows(self, filename, rows, writer_kwargs=None):
    """
    filename = get_filename('bar.csv')
    rows = [
        ['a', 'b', 'c'],
        ['d', 'e', 'f'],
    ]
    csv_writer = CloudStorageCsvWriter()
    csv_writer.writerows(filename, rows)
    """
    if writer_kwargs is None:
        writer_kwargs = {
            'quoting': unicodecsv.QUOTE_ALL,
            'quotechar': '"'
        }

    with gcs_file(filename, 'w') as f:
        writer = unicodecsv.writer(f, **writer_kwargs)
        writer.writerows(rows)
        return f
