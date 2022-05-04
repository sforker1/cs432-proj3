from django.db import models

class MovieInfo(models.Model):
    title = models.CharField(max_length=255)
    year = models.CharField(max_length=255)
    genre = models.CharField(max_length=255)
    rating = models.CharField(max_length=255)
