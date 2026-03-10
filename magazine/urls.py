from django.urls import path
from . import views

urlpatterns = [
    path("", views.search, name="search"),
    path("issue/<int:pub_id>/", views.issue_detail, name="issue_detail"),
    path("author/", views.author_search, name="author_search"),
]
