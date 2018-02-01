from django.db import models


class Contact(models.Model):
    email = models.EmailField()
    name = models.CharField(max_length=255)