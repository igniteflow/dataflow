from django.db import models


class CsvImportLog(models.Model):
    csv_file = models.FileField()
    num_rows = models.IntegerField()
    time_taken = models.DecimalField()

    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)