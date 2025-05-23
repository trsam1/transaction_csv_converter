import psycopg2
import csv
from typing import Dict, List
from datetime import datetime
from transaction_processor import TransactionProcessor
import os

class PostgresTransactionLoader:
    """
    A class to handle loading transaction data into a PostgreSQL database
    """
    
    def __init__(self):
        """Initialize database connection using environment variables"""
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to PostgreSQL database using environment variables"""
        try:
            self.conn = psycopg2.connect(
                dbname=os.environ.get('DB_NAME'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT', '5432')
            )
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            raise Exception(f"Database connection error: {str(e)}")

    def create_transactions_table(self):
        """Create the transactions table if it doesn't exist"""
        try:
            create_table_query = """
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    transaction_date DATE NOT NULL,
                    description TEXT NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    category VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            self.cursor.execute(create_table_query)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Error creating table: {str(e)}")

    def load_transactions(self, transactions: List[Dict]):
        """
        Load transactions into the database
        
        Args:
            transactions (List[Dict]): List of transaction dictionaries
        """
        try:
            insert_query = """
                INSERT INTO transactions (transaction_date, description, amount, category)
                VALUES (%(date)s, %(description)s, %(amount)s, %(category)s)
            """
            self.cursor.executemany(insert_query, transactions)
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Error loading transactions: {str(e)}")

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def main():
    # Initialize the transaction processor and postgres loader
    try:
        # First process the CSV file using your existing TransactionProcessor
        processor = TransactionProcessor('transactions.csv', 'DSC.json')
        processor.load_transactions()
        transactions = processor.get_transactions()

        # Now load the processed transactions into PostgreSQL
        loader = PostgresTransactionLoader()
        loader.connect()
        loader.create_transactions_table()
        loader.load_transactions(transactions)
        
        print(f"Successfully loaded {len(transactions)} transactions into the database")
    
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        loader.close()

if __name__ == "__main__":
    main()
