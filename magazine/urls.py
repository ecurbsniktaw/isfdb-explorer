from django.urls import path
from . import views

urlpatterns = [
    path("", views.search, name="search"),
    path("authors/", views.find_authors_view, name="find_authors"),
    path("issue/<int:pub_id>/", views.issue_detail, name="issue_detail"),
    path("author/", views.author_search, name="author_search"),
    path("author/<int:author_id>/", views.author_detail, name="author_detail"),
    path("author/<int:author_id>/books/", views.author_books, name="author_books"),
    path("author/<int:author_id>/works/", views.author_works, name="author_works"),
]
