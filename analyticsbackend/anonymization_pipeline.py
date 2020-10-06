from analyticsbackend.utils.spark_runner import SparkRunner
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col, udf


class AnonymizationUDF:
    """
    Implements the UDF for Anonymization
    """

    udf_output_spark_schema = StringType()

    def __init__(self):
        self.udf = udf(self.anonymize_name, AnonymizationUDF.udf_output_spark_schema)
        super().__init__()

    def anonymize_name(self, input_name):
        """
        Revert the input string
        """
        if input_name is None:
            return None
        return input_name[::-1]


class AnonymizerPipeline(SparkRunner):
    """
    1. Reads in batch from an input delta table
    2. Performs the anonymization of the field 'name'
    3. Writes the resulting dataframe to the output path
    """

    def __init__(self, config):
        super().__init__()

        self.source_table_path = config["source_table_path"]
        self.destination_table_path = config["destination_table_path"]
        self.destination_table_partitions = config["destination_table_partitions"]

    def run(self):
        # read
        source_df = self.spark.read.format("delta").load(self.source_table_path)

        # transform
        anonymization_udf = AnonymizationUDF().udf
        transformed_df = source_df.withColumn(
            "name", anonymization_udf(col("name"))
        ).select("country", "name")

        # write
        (
            transformed_df.write.format("delta")
            .mode("overwrite")
            .partitionBy(self.destination_table_partitions)
            .save(self.destination_table_path)
        )
