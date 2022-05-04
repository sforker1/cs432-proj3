from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from .models import MovieInfo

def index(request):
  template = loader.get_template('index.html')
  return HttpResponse(template.render())

def find(request):
  template = loader.get_template('find.html')
  return HttpResponse(template.render({}, request))

def findmovie(request):
  x = request.POST['title']
  MovieInfo.objects.all().delete()
  member = MovieInfo(title=x, year="yea", genre="gen", rating="rate")
  member.save()
  mymovies = MovieInfo.objects.all().values()
  template = loader.get_template('results.html')
  context = {
    'mymovies': mymovies
  }
  return HttpResponse(template.render(context, request))

def goback(request):
  return HttpResponseRedirect(reverse('index'))