#!/usr/bin/env python3
"""
Database Utilities for Spotify Data Pipeline

This module provides database connection and query utilities for the Spotify data pipeline.
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor, DictCursor, execute_values
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database connection parameters from environment variables
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'spotify_data')
DB_USER = os.getenv('DB_USER', 'spotify_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'spotify_password')


def get_connection(db_name=None, cursor_factory=None):
    """
    Create a connection to the PostgreSQL database.
    
    Args:
        db_name (str, optional): Database name to connect to. If None, uses default from env vars.
        cursor_factory: Type of cursor to use (e.g., DictCursor, RealDictCursor)
    
    Returns:
        tuple: (connection, cursor)
    
    Raises:
        psycopg2.Error: If connection fails
    """
    try:
        # Use provided database name or default
        database = db_name if db_name else DB_NAME
        
        # Create connection
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=database,
            user=DB_USER,
            password=DB_PASSWORD
        )
        
        # Create cursor with specified factory
        cursor = conn.cursor(cursor_factory=cursor_factory)
        
        return conn, cursor
    
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise


def close_connection(conn, cursor):
    """
    Close database connection and cursor.
    
    Args:
        conn: PostgreSQL connection
        cursor: PostgreSQL cursor
    """
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def execute_query(query, params=None, fetch=True, dict_cursor=True, commit=False):
    """
    Execute a SQL query and return results.
    
    Args:
        query (str): SQL query to execute
        params (tuple, optional): Parameters for the query
        fetch (bool): Whether to fetch and return results
        dict_cursor (bool): Whether to use a dictionary cursor
        commit (bool): Whether to commit the transaction
    
    Returns:
        list: Query results as a list of dictionaries or None
    
    Raises:
        psycopg2.Error: If query execution fails
    """
    conn = None
    cursor = None
    try:
        # Use DictCursor if requested
        cursor_factory = RealDictCursor if dict_cursor else None
        conn, cursor = get_connection(cursor_factory=cursor_factory)
        
        # Execute query
        cursor.execute(query, params)
        
        # Fetch results if requested
        results = None
        if fetch:
            results = cursor.fetchall()
        
        # Commit if requested
        if commit:
            conn.commit()
        
        return results
    
    except psycopg2.Error as e:
        logger.error(f"Query execution error: {e}")
        logger.error(f"Query: {query}")
        if params:
            logger.error(f"Params: {params}")
        
        # Rollback on error
        if conn:
            conn.rollback()
        
        raise
    
    finally:
        close_connection(conn, cursor)


def bulk_insert(table, data, columns, batch_size=1000):
    """
    Insert multiple records into a table.
    
    Args:
        table (str): Table name to insert into
        data (list): List of tuples containing the data to insert
        columns (list): List of column names
        batch_size (int): Number of records to insert in each batch
    
    Returns:
        int: Total number of records inserted
    
    Raises:
        psycopg2.Error: If insertion fails
    """
    conn = None
    cursor = None
    total_inserted = 0
    
    try:
        conn, cursor = get_connection()
        
        # Process in batches to avoid memory issues
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            
            # Construct the query
            column_str = ', '.join(columns)
            placeholder = '%s'
            query = f"INSERT INTO {table} ({column_str}) VALUES %s"
            
            # Execute batch insert
            execute_values(cursor, query, batch)
            conn.commit()
            
            total_inserted += len(batch)
            logger.info(f"Inserted batch of {len(batch)} records into {table}. Total: {total_inserted}")
        
        return total_inserted
    
    except psycopg2.Error as e:
        logger.error(f"Bulk insert error: {e}")
        if conn:
            conn.rollback()
        raise
    
    finally:
        close_connection(conn, cursor)


def execute_script(script_path):
    """
    Execute a SQL script file.
    
    Args:
        script_path (str): Path to the SQL script file
    
    Returns:
        bool: True if successful, False otherwise
    
    Raises:
        FileNotFoundError: If script file not found
    """
    if not os.path.exists(script_path):
        logger.error(f"Script file not found: {script_path}")
        raise FileNotFoundError(f"Script file not found: {script_path}")
    
    try:
        # Read script content
        with open(script_path, 'r') as f:
            script = f.read()
        
        # Execute script
        conn, cursor = get_connection()
        cursor.execute(script)
        conn.commit()
        
        logger.info(f"Successfully executed script: {script_path}")
        return True
    
    except psycopg2.Error as e:
        logger.error(f"Script execution error: {e}")
        if conn:
            conn.rollback()
        return False
    
    finally:
        close_connection(conn, cursor)


def table_exists(table_name, schema='raw'):
    """
    Check if a table exists in the database.
    
    Args:
        table_name (str): Name of the table to check
        schema (str): Schema containing the table
    
    Returns:
        bool: True if table exists, False otherwise
    """
    query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    );
    """
    
    try:
        conn, cursor = get_connection()
        cursor.execute(query, (schema, table_name))
        exists = cursor.fetchone()[0]
        return exists
    
    except psycopg2.Error as e:
        logger.error(f"Error checking if table exists: {e}")
        return False
    
    finally:
        close_connection(conn, cursor)


def get_table_count(table_name, schema='raw'):
    """
    Get the number of rows in a table.
    
    Args:
        table_name (str): Name of the table
        schema (str): Schema containing the table
    
    Returns:
        int: Number of rows in the table
    """
    try:
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        conn, cursor = get_connection()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        return count
    
    except psycopg2.Error as e:
        logger.error(f"Error getting table count: {e}")
        return 0
    
    finally:
        close_connection(conn, cursor)


# Example usage
if __name__ == "__main__":
    # Test database connection
    try:
        conn, cursor = get_connection()
        logger.info("Successfully connected to the database")
        close_connection(conn, cursor)
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        sys.exit(1)
    
    # Example: Get count of users
    user_count = get_table_count('users')
    logger.info(f"Number of users in the database: {user_count}")
    
    # Example: Execute query
    query = "SELECT * FROM raw.users LIMIT 5"
    try:
        results = execute_query(query, dict_cursor=True)
        logger.info(f"Sample users: {results}")
    except Exception as e:
        logger.error(f"Query failed: {e}") 