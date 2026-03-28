from django.urls import path
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("search/", views.combined_search, name="combined_search"),
    path("issues/", views.search, name="search"),
    path("authors/", views.find_authors_view, name="find_authors"),
    path("issue/<int:pub_id>/", views.issue_detail, name="issue_detail"),
    path("author/", views.author_search, name="author_search"),
    path("author/<int:author_id>/", views.author_detail, name="author_detail"),
    path("author/<int:author_id>/books/", views.author_books, name="author_books"),
    path("author/<int:author_id>/works/", views.author_works, name="author_works"),
    path("book/<int:title_id>/", views.book_detail, name="book_detail"),
    path("magazines/", views.magazine_list, name="magazine_list"),
    path("magazines/browse/", views.magazine_issues_by_name, name="magazine_issues_by_name"),
    path("magazines/<str:mag_code>/", views.magazine_issues, name="magazine_issues"),
    path("random/<str:kind>/", views.random_item, name="random_item"),
    path("about/", views.about, name="about"),
]
