from django.urls import path
from app import views

urlpatterns = [
    path('calculate_total_load/', views.perform_calculation, name='calc'),
]

