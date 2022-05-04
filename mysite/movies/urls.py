from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('find/', views.find, name='find'),
    path('find/findmovie/', views.findmovie, name='findmovie'),
    path('find/findmovie/goback/', views.goback, name='goback'),
]