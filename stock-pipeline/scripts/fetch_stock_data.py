import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            database=os.getenv('POSTGRES_DB', 'stockdb'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def fetch_stock_data(symbol='IBM'):
    """Fetch stock data from Alpha Vantage API."""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set")
    
    url = f"https://www.alphavantage.co/query"
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize': 'compact'
    }
    
    try:
        logger.info(f"Fetching data for {symbol}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if 'Error Message' in data:
            raise ValueError(f"API Error: {data['Error Message']}")
        
        if 'Note' in data:
            logger.warning(f"API Note: {data['Note']}")
            raise ValueError("API rate limit reached")
        
        if 'Time Series (Daily)' not in data:
            logger.error(f"Unexpected response format: {data}")
            raise ValueError("Invalid API response format")
        
        return data['Time Series (Daily)']
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise

def parse_and_store_data(stock_data, symbol='IBM'):
    """Parse JSON data and store in PostgreSQL."""
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        records = []
        for date_str, values in stock_data.items():
            try:
                record = (
                    symbol,
                    datetime.strptime(date_str, '%Y-%m-%d').date(),
                    float(values.get('1. open', 0)),
                    float(values.get('2. high', 0)),
                    float(values.get('3. low', 0)),
                    float(values.get('4. close', 0)),
                    int(values.get('5. volume', 0)),
                    datetime.now()
                )
                records.append(record)
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping record for {date_str}: {e}")
                continue
        
        if not records:
            logger.warning("No valid records to insert")
            return 0
        
        # Upsert data (insert or update on conflict)
        query = """
            INSERT INTO stock_data 
            (symbol, date, open, high, low, close, volume, updated_at)
            VALUES %s
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                updated_at = EXCLUDED.updated_at
        """
        
        execute_values(cursor, query, records)
        conn.commit()
        
        logger.info(f"Successfully inserted/updated {len(records)} records")
        return len(records)
    
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database operation failed: {e}")
        raise
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_and_store_stock_data():
    """Main function to fetch and store stock data."""
    try:
        stock_data = fetch_stock_data('IBM')
        records_count = parse_and_store_data(stock_data, 'IBM')
        logger.info(f"Pipeline completed successfully. Records processed: {records_count}")
        return records_count
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    fetch_and_store_stock_data()