from django.urls import include, path
from rest_framework import routers
from rest_framework.schemas import get_schema_view
{{view_import}}

router = routers.DefaultRouter()
{{routes}}

schema_view = get_schema_view(title="{{app_name}} API")

urlpatterns = [
    path("schema/", schema_view),
    path("", include(router.urls)),
]
