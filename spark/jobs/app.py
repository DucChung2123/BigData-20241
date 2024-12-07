from transformation import DataTransformationInit
from loader import DataImporter
from config import Config
import logging

logging.basicConfig(level=logging.INFO)

def main():
    logging.info("Starting Spark job")
    
    config = Config()
    processor = DataTransformationInit()
    importer = DataImporter(
        host=config.HOST,
        port=config.PORT,
        dbname=config.DATABASE,
        user=config.USER,
        password=config.PASSWORD
    )

    try:
        input_path = "hdfs://hdfs-namenode:9000/data"
        crime_df = processor.load_and_prepare_data(input_path)

        # Process data
        premise_df = processor.process_premise_data(crime_df)
        temporal_df = processor.process_temporal_data(crime_df)

        # Import data to PostgreSQL
        importer.import_data(crime_df, "crime")
        importer.import_data(premise_df, "premise")
        importer.import_data(temporal_df, "temporal")

        logging.info("Job completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        processor.close_spark_session()

if __name__ == "__main__":
    main()
