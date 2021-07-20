from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^item/(?P<item_id>.*)/$', views.item_view, name='itemView'),
    url(r'^$', views.index, name='index'),
]
