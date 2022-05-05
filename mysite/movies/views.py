from django.http import HttpResponse, HttpResponseRedirect
from django.template import loader
from django.urls import reverse
from .models import MovieInfo
from pyspark import SparkContext
import logging
import itertools
import copy

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

def rec(request):
  template = loader.get_template('rec.html')
  return HttpResponse(template.render({}, request))

def rate(request):
  template = loader.get_template('rate.html')
  return HttpResponse(template.render({}, request))

def recmovie(request):
  logger = logging.getLogger("mylogger")
  age = request.POST['year']
  rating = request.POST['rating']
  genre = request.POST['genre']
  sentence = request.POST['sentence']
  MovieInfo.objects.all().delete()

  sentence = sentence.lower()
  genre = genre.lower()

  dataRDD = filterfinal

  # Create weight for tracking relevance
  mappedData = dataRDD.map(lambda x: (x, 0))

  ageData = ageEst(mappedData, age)
  # if(DEBUG): ageData.foreach(lambda x: print(x))

  ratingData = ratingEst(ageData, rating)
  # if(DEBUG): ratingData.foreach(lambda x: print(x))

  genreData = genreEst(ratingData, genre)
  # if(DEBUG): genreData.foreach(lambda x: print(x))

  sentenceData = sentenceEst(genreData, sentence)
  # if(DEBUG): sentenceData.foreach(lambda x: print(x))

  orderedData = sentenceData.sortBy(lambda x: x[1])

  recList = orderedData.take(1)

  logger.info(recList)

  for x in recList:
    MovieInfo(title=str(x[0][0]), year=str(x[0][1]), genre=str(x[0][2]), rating=str(x[0][3])).save()
    break
  
  mymovies = MovieInfo.objects.all().values()
  template = loader.get_template('results.html')
  context = {
    'mymovies': mymovies
  }
  return HttpResponse(template.render(context, request))

def findmovie(request):
  userInput = request.POST['title']
  MovieInfo.objects.all().delete()
  userInput = userInput.lower()

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

def ratemovie(request):
  dataRDD = filterfinal
  title = request.POST['title']
  genres = request.POST['genres']

  title = title.lower()
  genres = genres.lower()

  #check if commas
  titleList = title.split(" ")
  genreList = genres.split(" ")

  #order combination list by amount of elements, small -> large
  titleComb = []
  for x in range(1, len(titleList)+1):
    medium = [list(y) for y in itertools.combinations(titleList, x)]
    titleComb.extend(medium)
  # if(DEBUG): print("Title Combo: {}".format(titleComb))
  
  holdingRDD = sc.emptyRDD()
  compareSize = 1
  ifFirstRun = 1
  tempTitleComb = copy.deepcopy(titleComb)
  
  # Check ordering
  while(tempTitleComb):
    comparison = tempTitleComb.pop(0)

    compareString = ""
    if(len(comparison) > 1):
      compareString = " ".join(comparison)
    else:
      compareString = comparison[0]
    
    # if(DEBUG): print("compareString is {}".format(compareString))

    tempRDD = dataRDD.filter(lambda x: True if all(y in x[0] for y in compareString) else False)
    # tempRDD = dataRDD.filter(lambda x: str(compareString) in x)
    # if(DEBUG): tempRDD.foreach(lambda x: print("Temp First: {}".format(x)))

    if(ifFirstRun):
      holdingRDD = holdingRDD.union(tempRDD)
      ifFirstRun = 0
    # What if unioned? for all similar len(comparisons)
    elif(tempRDD.count() > holdingRDD.count() and len(comparison) == compareSize):
      holdingRDD = tempRDD
    elif(tempRDD.count() > 0 and len(comparison) > compareSize):
      holdingRDD = tempRDD

    compareSize = len(comparison)
  
  # if(DEBUG): holdingRDD.foreach(lambda x: print("First: {}".format(x)))


  holdingRDD1 = sc.emptyRDD()
  compareSize = 1
  ifFirstRun = 1
  tempTitleComb = copy.deepcopy(titleComb)

  # Check amount matching
  while(tempTitleComb):
    comparison = tempTitleComb.pop(0)

    tempRDD = dataRDD.filter(lambda x: True if all(y in x[0] for y in comparison) else False)
    # if(DEBUG): tempRDD.foreach(lambda x: print("Temp Second: {}".format(x)))

    if(ifFirstRun):
      holdingRDD1 = holdingRDD1.union(tempRDD)
      ifFirstRun = 0
        # if(DEBUG): print("First Run")
    # What if unioned? for all similar len(comparisons)
    elif(tempRDD.count() > holdingRDD1.count() and len(comparison) == compareSize):
        # if(DEBUG): print("Second Op")
      holdingRDD1 = tempRDD
    elif(tempRDD.count() > 0 and len(comparison) > compareSize):
        # if(DEBUG): print("Third Op")
      holdingRDD1 = tempRDD

    compareSize = len(comparison)

  # if(DEBUG): holdingRDD1.foreach(lambda x: print("Second: {}".format(x)))
  
  # Check for all genres
  genreRDD = dataRDD.filter(lambda x: True if all(y in x[2] for y in genreList) else False)
  # if(DEBUG): genreRDD.foreach(lambda x: print("Third: {}".format(x)))


  holdingRDD = holdingRDD.distinct()
  holdingRDD1 = holdingRDD1.distinct()
  genreRDD = genreRDD.distinct()

  # TODO change this V because the first value might have nothing!!
  finalRDD = holdingRDD
  if(finalRDD.intersection(holdingRDD1).count() > 0):
    finalRDD = finalRDD.intersection(holdingRDD1)
  
  if(finalRDD.intersection(genreRDD).count() > 0):
    finalRDD = finalRDD.intersection(genreRDD)
  
  calcRDD = finalRDD.map(lambda x: int(float(x[3])))

  predictedRating = calcRDD.mean()

  # print("\nThe Predicted Rating Is: {0}".format(predictedRating))

  MovieInfo.objects.all().delete()
  
  MovieInfo(title=str(predictedRating), year=str(predictedRating), genre=str(predictedRating), rating=str(predictedRating)).save()
  
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
  moviesFile = "../MovieTweetings-master/latest/movies.dat"
  ratingsFile = "../MovieTweetings-master/latest/ratings.dat"    
  global sc
  sc = SparkContext("local", "movies app")
  moviesData = sc.textFile(moviesFile).cache()
  moviesKey = moviesData.map(lambda x: (x.split("::")[0], (x.split("::")[1].split(" (")[0].lower(), x.split("::")[1].split("(")[1][:-1], x.split("::")[2].lower())))
    
    #make title all uppercase!
    
  ratingsData = sc.textFile(ratingsFile).cache()
  ratingsKey = ratingsData.map(lambda x: (x.split("::")[1], (x.split("::")[2])))
  tempTuple = (0,0)
  tempRDD = ratingsKey.aggregateByKey(tempTuple, lambda a,b: (int(a[0]) + int(b), int(a[1]) + 1), lambda a,b: (int(a[0]) + int(b[0]), int(a[1]) + int(b[1])))
  finalResult = tempRDD.mapValues(lambda x: x[0] / x[1])

  ratingsKey = finalResult.map(lambda x: (x[0], str(x[1])))
  combine = moviesKey.union(ratingsKey).reduceByKey(lambda x,y : x+(y,))#moviesKey.cartesian(ratingsKey)
  remap = combine.map(lambda x: x[1])
  filter1 = remap.filter(lambda x: isinstance(x, tuple))
  global filterfinal 
  filterfinal = filter1.filter(lambda x: len(x) == 4)

def ageEst(data, age):
  newData = data.map(lambda x: (x[0], x[1] + (abs(int(float(x[0][1])) - int(float(age))) * 5)))
  return newData

def ratingEst(data, rating):
  newData = data.map(lambda x: (x[0], x[1] + (abs(int(float(x[0][3])) - int(float(rating))) * 10)))
  return newData

def genreEst(data, genre):
  newData = data.map(lambda x: (x[0], x[1] - 25) if genre in x[0][2] else (x[0], x[1]))
  return newData

def sentenceEst(data, sentence):
  words = sentence.split()

  for y in words:
    data = data.map(lambda x: (x[0], x[1] - 15) if y in x[0][0] else (x[0], x[1]))
  return data