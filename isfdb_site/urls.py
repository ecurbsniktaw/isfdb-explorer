from django.urls import path, include
from magazine import views as magazine_views

urlpatterns = [
    path("", include("magazine.urls")),
]

handler404 = magazine_views.error_404
handler500 = magazine_views.error_500
