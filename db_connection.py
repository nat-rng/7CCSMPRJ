def create_primary_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Blocks (
        block_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        block_number VARCHAR(255)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Addresses (
        address_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        address VARCHAR(255)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS TxCategories (
        category_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        category_name VARCHAR(255)
    )
    """)

def create_transactions_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Transactions (
        tx_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        tx_hash VARCHAR(255) DEFAULT NULL,
        block_id INT,
        from_id INT,
        to_id INT,
        asset_value FLOAT DEFAULT NULL,
        erc721_token_id VARCHAR(255) DEFAULT NULL,
        erc1155_token_id VARCHAR(255) DEFAULT NULL,
        erc1155_value VARCHAR(255) DEFAULT NULL,
        token_id VARCHAR(255) DEFAULT NULL,
        asset VARCHAR(255) DEFAULT NULL,
        category_id INT,
        timestamp TIMESTAMP DEFAULT NULL,
        contract_id INT DEFAULT NULL,
        FOREIGN KEY (block_id) REFERENCES Blocks(block_id),
        FOREIGN KEY (from_id) REFERENCES Addresses(address_id),
        FOREIGN KEY (to_id) REFERENCES Addresses(address_id),
        FOREIGN KEY (category_id) REFERENCES TxCategories(category_id)
    )
    """)
import mysql.connector
from sqlalchemy import create_engine

config_gsql = {
    'user': 'eth_project',
    'password': 'eth_000!',
    'host': 'localhost',
    'port': '3306',
    'database': 'ETHProjectDB'
}

def create_contracts_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Contracts (
        contract_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        tx_id INT,
        FOREIGN KEY (tx_id) REFERENCES Transactions(tx_id)
    )
    """)

def connect_db(config):
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    return cnx, cursor

def export_df_to_sql(df, table_name, config):
    # Configure options for the JDBC connection
    properties = {
        "driver": "org.mariadb.jdbc.Driver",  # MariaDB JDBC driver
        "user": config['user'],
        "password": config['password']
    }
    
    url = f"jdbc:mariadb://{config['host']}:{config['port']}/{config['database']}"
    # Write the DataFrame to the SQL table
    df.write.format("jdbc").mode("append").option("url", url).option("dbtable", table_name).options(**properties).save()
