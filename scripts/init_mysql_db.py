#!/usr/bin/env python
"""Initialize MySQL database for detection system."""
import mysql.connector
from mysql.connector import Error

# MySQL connection config (use local MySQL)
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",  # Empty password by default
    "port": 3306,
}

DB_NAME = "az_hackathon"


def create_database():
    """Create database and tables."""
    conn = None
    cursor = None

    try:
        # Connect to MySQL server
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        cursor.execute(f"USE {DB_NAME}")
        print(f"✓ Created database: {DB_NAME}")

        # Create DIM_ACCOUNT table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS DIM_ACCOUNT (
            ACCOUNT_KEY INT PRIMARY KEY,
            ACCOUNT_CODE VARCHAR(100),
            ACCOUNT_DESCRIPTION VARCHAR(255)
        )
        """)
        print("✓ Created table: DIM_ACCOUNT")

        # Create DIM_SCENARIO table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS DIM_SCENARIO (
            SCENARIO_KEY INT PRIMARY KEY,
            SCENARIO_NAME VARCHAR(50)
        )
        """)
        print("✓ Created table: DIM_SCENARIO")

        # Create FACT_PNL_BASE_BRAND table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS FACT_PNL_BASE_BRAND (
            ACCOUNT_KEY INT,
            MANAGEMENT_UNIT_KEY INT,
            PRODUCT_KEY INT,
            SCENARIO_KEY INT,
            TIME_KEY VARCHAR(10),
            VALUE DECIMAL(20,4),
            QTD DECIMAL(20,4),
            YTD DECIMAL(20,4),
            Q1 DECIMAL(20,4),
            Q2 DECIMAL(20,4),
            Q3 DECIMAL(20,4),
            Q4 DECIMAL(20,4),
            H1 DECIMAL(20,4),
            H2 DECIMAL(20,4),
            PY_VALUE DECIMAL(20,4),
            PY_QTD DECIMAL(20,4),
            PY_YTD DECIMAL(20,4),
            PY_Q1 DECIMAL(20,4),
            PY_Q2 DECIMAL(20,4),
            PY_Q3 DECIMAL(20,4),
            PY_Q4 DECIMAL(20,4),
            PY_H1 DECIMAL(20,4),
            PY_H2 DECIMAL(20,4),
            PY_QTD_VARIANCE DECIMAL(20,4),
            PY_YTD_VARIANCE DECIMAL(20,4),
            BUD_VALUE DECIMAL(20,4),
            BUD_QTD DECIMAL(20,4),
            BUD_YTD DECIMAL(20,4),
            BUD_Q1 DECIMAL(20,4),
            BUD_Q2 DECIMAL(20,4),
            BUD_Q3 DECIMAL(20,4),
            BUD_Q4 DECIMAL(20,4),
            BUD_H1 DECIMAL(20,4),
            BUD_H2 DECIMAL(20,4),
            BUD_QTD_VARIANCE DECIMAL(20,4),
            BUD_YTD_VARIANCE DECIMAL(20,4),
            MTP_VALUE DECIMAL(20,4),
            MTP_QTD DECIMAL(20,4),
            MTP_YTD DECIMAL(20,4),
            MTP_Q1 DECIMAL(20,4),
            MTP_Q2 DECIMAL(20,4),
            MTP_Q3 DECIMAL(20,4),
            MTP_Q4 DECIMAL(20,4),
            MTP_H1 DECIMAL(20,4),
            MTP_H2 DECIMAL(20,4),
            MTP_QTD_VARIANCE DECIMAL(20,4),
            MTP_YTD_VARIANCE DECIMAL(20,4),
            RBU2LTP_VALUE DECIMAL(20,4),
            RBU2LTP_QTD DECIMAL(20,4),
            RBU2LTP_YTD DECIMAL(20,4),
            RBU2LTP_Q1 DECIMAL(20,4),
            RBU2LTP_Q2 DECIMAL(20,4),
            RBU2LTP_Q3 DECIMAL(20,4),
            RBU2LTP_Q4 DECIMAL(20,4),
            RBU2LTP_Q1 DECIMAL(20,4),
            RBU2LTP_Q2 DECIMAL(20,4),
            PY_VARIANCE DECIMAL(20,4),
            BUD_VARIANCE DECIMAL(20,4),
            RBU2LTP_VARIANCE DECIMAL(20,4),
            PRIMARY KEY (ACCOUNT_KEY, MANAGEMENT_UNIT_KEY, PRODUCT_KEY, SCENARIO_KEY, TIME_KEY)
        )
        """)
        print("✓ Created table: FACT_PNL_BASE_BRAND")

        conn.commit()
        print("\n✓ Database initialization completed successfully!")
        return True

    except Error as e:
        print(f"\n❌ Error: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    print("=" * 80)
    print("MySQL Database Initialization")
    print("=" * 80)
    print(f"\nHost: {DB_CONFIG['host']}")
    print(f"Port: {DB_CONFIG['port']}")
    print(f"Database: {DB_NAME}")
    print("=" * 80)

    import sys
    sys.exit(0 if create_database() else 1)
