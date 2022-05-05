from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from .models import MovieInfo
from pyspark import SparkContext

def run_once(f):
  def wrapper(*args, **kwargs):
    if not wrapper.has_run:
      wrapper.has_run = True
      return f(*args, **kwargs)
  wrapper.has_run = False
  return wrapper

def index(request):
  dataPrep()
  template = loader.get_template('index.html')
  return HttpResponse(template.render())

def find(request):
  template = loader.get_template('find.html')
  return HttpResponse(template.render({}, request))

def findmovie(request):
  userInput = request.POST['title']
  MovieInfo.objects.all().delete()

  dataRDD = filterfinal
  movies = dataRDD.filter(lambda x: userInput in x[0])
  #outputCount = movies.count()
  # movies.foreach(lambda x: MovieInfo(title=x[0], year=x[1], genre=x[2], rating=x[3]).save())
  recList = movies.take(1)
  for x in recList:
    MovieInfo(title=str(x[0]), year=str(x[1]), genre=str(x[2]), rating=str(x[3])).save()
    break
  
  # MovieInfo(title=str(outputCount), year="yea", genre="gen", rating="rate").save()
  mymovies = MovieInfo.objects.all().values()
  template = loader.get_template('results.html')
  context = {
    'mymovies': mymovies
  }
  return HttpResponse(template.render(context, request))

def goback(request):
  return HttpResponseRedirect(reverse('index'))

@run_once
def dataPrep():
  moviesFile = "../movies.dat"
  ratingsFile = "../ratings.dat"    
  sc = SparkContext("local", "movies app")
  moviesData = sc.textFile(moviesFile).cache()
  moviesKey = moviesData.map(lambda x: (x.split("::")[0], (x.split("::")[1].split(" (")[0], x.split("::")[1].split("(")[1][:-1], x.split("::")[2])))
  
  #reduce list by average of same ratings
  #make title all uppercase!
    
  ratingsData = sc.textFile(ratingsFile).cache()
  ratingsKey = ratingsData.map(lambda x: (x.split("::")[1], (x.split("::")[2])))
  combine = moviesKey.union(ratingsKey).reduceByKey(lambda x,y : x+(y,))#moviesKey.cartesian(ratingsKey)
  remap = combine.map(lambda x: x[1])
  filter1 = remap.filter(lambda x: isinstance(x, tuple))
  global filterfinal 
  filterfinal = filter1.filter(lambda x: len(x) == 4)