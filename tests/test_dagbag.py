"""Test the validity of all DAGs (unicity, cycles, task parameters, ...)."""

import unittest
import os
from airflow.models import DagBag

DAGS_PATH = os.path.join(os.path.dirname(__file__), "../dags")

class TestDagBag(unittest.TestCase):
    
    def test_dagbag(self):
        """
        Validate DAG files using Airflow's DagBag.
        This includes sanity checks e.g. do tasks have required arguments, are DAG ids unique & do DAGs have no cycles.
        """
        dag_bag = DagBag(include_examples=False, dag_folder=DAGS_PATH)
        assert not dag_bag.import_errors  