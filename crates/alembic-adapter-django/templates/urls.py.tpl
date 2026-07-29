from django.urls import include, path
from rest_framework import routers
{{schema_import}}{{view_import}}

router = routers.DefaultRouter()
{{routes}}

urlpatterns = [
{{schema_routes}}    path("", include(router.urls)),
]
