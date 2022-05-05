from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('find/', views.find, name='find'),
    path('rec/', views.rec, name='rec'),
    path('rate/', views.rate, name='rate'),
    path('rate/ratemovie/', views.ratemovie, name='ratemovie'),
    path('rate/ratemovie/goback/', views.goback, name='goback'),
    path('rec/recmovie/', views.recmovie, name='recmovie'),
    path('rec/recmovie/goback/', views.goback, name='goback'),
    path('find/findmovie/', views.findmovie, name='findmovie'),
    path('find/findmovie/goback/', views.goback, name='goback'),
]