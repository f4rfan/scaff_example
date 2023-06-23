from unittest import TestCase
from pyspark.sql import SparkSession, DataFrame

import exampleenginepythonqiyhbwvw.common.constants as c
from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from unittest.mock import MagicMock


class TestApp(TestCase):
    def test_run_experiment(self):

        parameters = {"clients_path": "resources/data/input/clients.csv",
                      "clients_schema": "resources/schemas/clients_schema.json",
                      "contracts_path": "resources/data/input/contracts.csv",
                      "contracts_schema": "resources/schemas/contracts_schema.json",
                      "products_path": "resources/data/input/products.csv",
                      "products_schema": "resources/schemas/products_schema.json",
                      "output_path": "resources/data/output",
                      "output_schema": "resources/schemas/output_schema.json"}

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        spark = SparkSession.builder.appName("unittest_job").master("local[*]").getOrCreate()

        out_df = spark.read.parquet(parameters["output_path"]+c.FINAL_TABLE)
        self.assertIsInstance(out_df, DataFrame)

