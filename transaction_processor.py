import csv
import json
import argparse
from typing import List, Dict
from datetime import datetime

class TransactionProcessor:
    """
    A utility class for processing bank transaction CSV files and outputting 
    standardized transaction data.
    """
    
    def __init__(self, file_path: str, config_path: str):
        """
        Initialize the TransactionProcessor with a CSV file path.
        
        Args:
            file_path (str): Path to the CSV file containing bank transactions
        """
        self.file_path = file_path
        self.config_path = config_path
        self.transactions: List[Dict] = []
        self.header_mapping = self._load_config()
    
    def _load_config(self) -> Dict:
        """
        Load header mapping configuration from JSON file.
        
        Returns:
            Dict: Header mapping configuration
        """
        try:
            with open(self.config_path, 'r') as config_file:
                config = json.load(config_file)
                required_keys = {'date_field', 'description_field', 'amount_field', 'category_field'}
                if not all(key in config for key in required_keys):
                    raise ValueError("Config file missing required field mappings")
                return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON in config file: {self.config_path}")

    def load_transactions(self) -> None:
        """
        Load transactions from the CSV file and store them in a standardized format.
        Uses header mapping from config file to identify correct columns.
        """
        try:
            with open(self.file_path, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    transaction = {
                        'date': self._standardize_date(row.get(self.header_mapping['date_field'], '')),
                        'description': row.get(self.header_mapping['description_field'], '').strip(),
                        'amount': self._standardize_amount(row.get(self.header_mapping['amount_field'], '0')),
                        'category': row.get(self.header_mapping['category_field'], '').strip()
                    }
                    self.transactions.append(transaction)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        except Exception as e:
            raise Exception(f"Error loading transactions: {str(e)}")
    
    def _standardize_date(self, date_str: str) -> str:
        """
        Convert date string to standard YYYY-MM-DD format.
        Handles common date formats.
        """
        try:
            # Add more date formats as needed
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d'):
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
            raise ValueError(f"Unable to parse date: {date_str}")
        except Exception as e:
            raise ValueError(f"Date standardization error: {str(e)}")
    
    def _standardize_amount(self, amount_str: str) -> float:
        """
        Convert amount string to standard float format.
        Handles common currency formats and converts to float.
        """
        try:
            # Remove currency symbols and whitespace
            amount = amount_str.replace('$', '').replace('£', '').replace('€', '').strip()
            # Replace commas with empty string for proper float conversion
            amount = amount.replace(',', '')
            return float(amount)
        except Exception as e:
            raise ValueError(f"Amount standardization error: {str(e)}")
    
    def get_transactions(self) -> List[Dict]:
        """
        Return the list of standardized transactions.
        
        Returns:
            List[Dict]: List of standardized transaction dictionaries
        """
        return self.transactions
    
    def write_standardized_csv(self, output_path: str) -> None:
        """
        Write standardized transactions to a new CSV file.
        
        Args:
            output_path (str): Path where the standardized CSV should be written
        """
        if not self.transactions:
            raise ValueError("No transactions loaded. Call load_transactions() first.")
            
        try:
            with open(output_path, 'w', newline='') as csvfile:
                fieldnames = ['date', 'description', 'amount', 'category']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.transactions)
        except Exception as e:
            raise Exception(f"Error writing standardized CSV: {str(e)}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process bank transaction CSV files')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_file', help='Path to the output CSV file')
    parser.add_argument('--config', required=True, help='Path to the config JSON file')
    
    args = parser.parse_args()

    try:
        # Initialize and run the processor
        processor = TransactionProcessor(args.input_file, args.config)
        processor.load_transactions()
        processor.write_standardized_csv(args.output_file)
        print(f"Successfully processed {len(processor.get_transactions())} transactions")
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()



