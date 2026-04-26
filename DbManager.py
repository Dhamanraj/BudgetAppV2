import mysql.connector
from mysql.connector import pooling, Error
import os
import dotenv

dotenv.load_dotenv(dotenv_path='DbConfig.env', override=True)

class DatabaseManager:

    _pool = None

    @classmethod
    def _initialize_pool(cls):
        if cls._pool is None:
            try:
                db_config = {
                    'host': os.getenv('HOST'),
                    'user': os.getenv('USER'),
                    'password': os.getenv('PASSWORD'),
                    'database': os.getenv('DATABASE')
                }

                if not all(db_config.values()):
                    raise ValueError("One or more database environment variables are missing in DbConfig.env")

                # Creating a pool of 5 connections (adjustable based on load)
                cls._pool = pooling.MySQLConnectionPool(
                    pool_name="finance_agent_pool",
                    pool_size=5,  # Efficient for 16GB RAM
                    pool_reset_session=True,
                    **db_config
                )
                print("Connection pool initialized successfully.")
            except Error as e:
                print(f"Error initializing connection pool: {e}")
                raise

    @classmethod
    def execute_query(cls, sql: str, params: tuple = None):
        """
        Executes a raw SQL string and returns the result.
        Returns a list of dictionaries for SELECT statements.
        """
        if cls._pool is None:
            cls._initialize_pool()

        connection = None
        try:
            connection = cls._pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            
            # 1. Safety Check: Block destructive operations BEFORE execution by checking the first token
            forbidden = ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "CREATE")
            tokens = sql.strip().upper().split()
            first_word = tokens[0] if tokens else ""
            
            if first_word in forbidden:
                print(f"Access Denied: Destructive operation '{first_word}' blocked.")
                return None

            # 2. Execute and fetch results only if available
            cursor.execute(sql, params or ())
            result = cursor.fetchall() if cursor.with_rows else None
                
            cursor.close()
            return result

        except Error as e:
            print(f"Query Error: {e}")
            return None
        finally:
            if connection and connection.is_connected():
                connection.close() # Returns connection to the pool