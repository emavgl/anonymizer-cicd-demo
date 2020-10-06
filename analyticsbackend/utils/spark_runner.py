from abc import ABC
from pyspark.sql import SparkSession


class SparkRunner(ABC):
    def __init__(self):
        super().__init__()
        self.spark = (
            SparkSession.builder.appName("MyApp")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
            .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
        )
        import delta.tables  # needs to be imported after spark session is created

        self.delta_table_api = delta.tables.DeltaTable
