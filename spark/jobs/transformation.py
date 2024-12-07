from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataTransformationInit:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName("CrimeAnalytics") \
            .getOrCreate()

    def clean_bicycle_data(self, df):
        df_clean = df.withColumn(
            'PREMISES_TYPE',
            when(col('PREMISES_TYPE').isin(['Apartment', 'House']), 'Residential')
            .when(col('PREMISES_TYPE') == 'Transit', 'Other')
            .when(col('PREMISES_TYPE') == 'Educational', 'Commercial')
            .otherwise(col('PREMISES_TYPE'))
        )
        
        df_clean = df_clean.withColumn('OCC_DATE', to_date(col('OCC_DATE'))) \
                           .withColumn('REPORT_DATE', to_date(col('REPORT_DATE'))) \
                           .withColumn(
                               'BIKE_COST',
                               when(col('BIKE_COST').isNotNull(), col('BIKE_COST').cast(DoubleType()))
                               .otherwise(lit(0.0))
                           )

        if 'BIKE_SPEED' in df.columns:
            df_clean = df_clean.withColumn(
                'BIKE_SPEED',
                when(col('BIKE_SPEED').rlike('^[0-9]+(\\.[0-9]+)?$'), col('BIKE_SPEED').cast(DoubleType()))
                .otherwise(lit(None))
            )
            median_speed = df_clean.filter(col('BIKE_SPEED').isNotNull()) \
                                   .approxQuantile('BIKE_SPEED', [0.5], 0.01)[0]
            df_clean = df_clean.withColumn(
                'BIKE_SPEED',
                when(col('BIKE_SPEED').isNull(), lit(median_speed))
                .otherwise(col('BIKE_SPEED'))
            )
        
        df_clean = df_clean.withColumn('BIKE_TYPE', coalesce(col('BIKE_TYPE'), lit('UNKNOWN'))) \
                           .withColumn('LOCATION_TYPE', coalesce(col('LOCATION_TYPE'), lit('UNKNOWN'))) \
                           .withColumn('PREMISES_TYPE', coalesce(col('PREMISES_TYPE'), lit('Other'))) \
                           .withColumn(
                               'REPORT_DELAY',
                               when(col('OCC_DATE').isNotNull() & col('REPORT_DATE').isNotNull(),
                                    datediff(col('REPORT_DATE'), col('OCC_DATE')) * 24.0)
                               .otherwise(lit(None))
                           ) \
                           .withColumn('BIKE_MAKE', upper(trim(col('BIKE_MAKE')))) \
                           .withColumn('BIKE_MODEL', upper(trim(col('BIKE_MODEL'))))
        return df_clean

    def load_and_prepare_data(self, input_path):
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        return self.clean_bicycle_data(df)

    def process_premise_data(self, df):
        return df.groupBy("PREMISES_TYPE", "OCC_YEAR", "OCC_MONTH") \
                 .agg(
                     count("*").alias("total_cases"),
                     sum(when(col("BIKE_COST") != 0, col("BIKE_COST")).otherwise(0)).alias("sum_non_zero"),
                     avg(when(col("BIKE_COST") != 0, col("BIKE_COST"))).alias("avg_value")
                 )

    def process_temporal_data(self, df):
        return df.groupBy("OCC_HOUR", "OCC_DOW", "OCC_YEAR") \
                 .agg(
                     count("*").alias("total_cases"),
                     avg(when(col("BIKE_COST") != 0, col("BIKE_COST"))).alias("avg_value")
                 )

    def close_spark_session(self):
        self.spark.stop()
