import sys

from unittest import TestCase

from airflow.models import DagBag

sys.path.append('.')

DAGS_FOLDER = "./dags/"


class HarvestDagsTest(TestCase):
    def dag_bag(self):
        return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

    def test_no_import_errors(self):
        assert not self.dag_bag().import_errors

