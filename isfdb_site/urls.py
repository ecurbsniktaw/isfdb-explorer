from django.conf import settings
from django.urls import path, include
from magazine import views as magazine_views

urlpatterns = [
    path("", include("magazine.urls")),
]

# Serve app static files via the staticfiles finders in development.
# (StaticFilesHandler requires STATIC_URL to start with '/' which our
# settings omit; the explicit urlpattern is more reliable.)
if settings.DEBUG:
    from django.contrib.staticfiles.urls import staticfiles_urlpatterns
    urlpatterns += staticfiles_urlpatterns()

handler404 = magazine_views.error_404
handler500 = magazine_views.error_500
