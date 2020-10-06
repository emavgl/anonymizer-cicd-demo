from analyticsbackend.utils.spark_runner import SparkRunner
from analyticsbackend.utils.pipeline import read_config_file
from analyticsbackend.anonymization_pipeline import AnonymizerPipeline
import shutil
import unittest
import sys


class AnonymizationPipelineTest(SparkRunner):
    def run(self, config: dict):
        try:
            # initialize source table
            source_df = self.spark.read.option("multiline", "true").json(
                config["tests_input"]
            )
            source_df.write.format("delta").mode("overwrite").save(
                config["source_table_path"]
            )

            # run pipeline
            AnonymizerPipeline(config).run()

            # asserts
            actual_table_df = (
                self.spark.read.format("delta")
                .load(config["destination_table_path"])
                .orderBy("name")
            )
            expected_table_df = (
                self.spark.read.option("multiline", "true")
                .json(config["expected_output"])
                .orderBy("name")
            )

            tc = unittest.TestCase()
            tc.assertEqual(
                set(actual_table_df.schema),
                set(expected_table_df.schema),
                "Dataframe schemas are not equal!",
            )
            tc.assertEqual(
                actual_table_df.collect(),
                expected_table_df.collect(),
                "Dataframe contents are not equal!",
            )
            print("All assertions passed! :)")
        finally:
            shutil.rmtree(config["test_folder"], ignore_errors=True)


argv = sys.argv
config_dict = read_config_file(argv[1], argv[2])
AnonymizationPipelineTest().run(config_dict)
