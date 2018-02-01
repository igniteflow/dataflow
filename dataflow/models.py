from django.db import models


class Status:
    UNPROCESSED = 'unprocessed'
    SUCCESS = 'success'
    ERRORS = 'errors'

    @classmethod
    def choices(cls):
        return (
            (cls.UNPROCESSED, u'Unprocessed'),
            (cls.SUCCESS, u'Success'),
            (cls.ERRORS, u'Errors'),
        )


class CsvImportLog(models.Model):
    Status = Status
    csv_file = models.FileField()
    error_file = models.FileField(null=True)
    status = models.CharField(choices=Status.choices(), default=Status.UNPROCESSED)

    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
