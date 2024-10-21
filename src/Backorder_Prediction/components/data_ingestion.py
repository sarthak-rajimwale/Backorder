import os
import sys
import pandas as pd
from src.Backorder_Prediction.exception import CustomException
from src.Backorder_Prediction.logger import logging
from src.Backorder_Prediction.utils import read_training_data,  read_test_data  # Import both

from dataclasses import dataclass

@dataclass
class DataIngestionConfig:
    """Configuration for data ingestion, defining paths for saving the data."""
    train_data_path: str = os.path.join('artifacts', 'train.csv')
    test_data_path: str = os.path.join('artifacts', 'test.csv')
class DataIngestion:
    def __init__(self):
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self):
        try:
            # Read both training and test data from MySQL
            df_train = read_training_data()
            df_test = read_test_data()

            logging.info("Successfully read both training and test data from MySQL")

            # Create the artifacts directory if it doesn't exist
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)

            # Save the raw and processed training data to CSV files
            df_train.to_csv(self.ingestion_config.train_data_path, index=False, header=True)

            # Save the raw and processed test data to CSV files
            df_test.to_csv(self.ingestion_config.test_data_path, index=False, header=True)

            logging.info("Data Ingestion completed successfully")

            # Return the paths to both training and test data
            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )

        except Exception as e:
            logging.error(f"Error in data ingestion: {e}")
            raise CustomException(e, sys)
