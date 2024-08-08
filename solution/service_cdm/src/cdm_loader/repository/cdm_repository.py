import uuid
from typing import Dict, List
from lib.pg import PgConnect
from psycopg import Cursor

class DataRepository:
    def __init__(self, database: PgConnect, logger):
        self._database = database
        self._logger = logger  

    def store_messages(self, messages: List[Dict]):
        with self._database.connection() as connection:
            with connection.cursor() as cursor:
                try:
                    self._initialize_temp_tables(cursor)
                    self._populate_temp_tables(cursor, messages)
                    self._logger.info("******\nЗагрузка временной таблицы******\n")
                    self._insert_into_data_marts(cursor)
                    self._logger.info("******\nЗагрузка завершена******\n")
                except Exception as error:
                    self._logger.error(f"Произошла ошибка: {error}")
                finally:
                    cursor.execute("DROP TABLE IF EXISTS temp_product_table;")
                    cursor.execute("DROP TABLE IF EXISTS temp_category_table;")

    def _initialize_temp_tables(self, cursor: Cursor):
        cursor.execute(
            """
                CREATE TEMP TABLE temp_product_table
                (
                    user_id         VARCHAR,
                    product_id      VARCHAR,
                    product_name    VARCHAR,
                    order_count     INT
                )
                ON COMMIT DROP;
            """
        )
        cursor.execute(
            """
                CREATE TEMP TABLE temp_category_table
                (
                    user_id         VARCHAR,
                    category_id     VARCHAR,
                    category_name   VARCHAR,
                    order_count     INT
                )
                ON COMMIT DROP;
            """
        )

    def _populate_temp_tables(self, cursor: Cursor, records: List[Dict]):
        for record in records:
            if record['object_type'] == 'user_prod':
                cursor.execute(
                    """
                        INSERT INTO temp_product_table (user_id, product_id, product_name, order_count)
                        VALUES 
                        (
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            %(order_count)s
                        );
                    """,
                    {
                        'user_id': record['payload']['user_id'],
                        'product_id': record['payload']['product_id'],
                        'product_name': record['payload']['product_name'],
                        'order_count': int(record['payload']['order_cnt'])
                    }
                )
            elif record['object_type'] == 'user_categ':
                cursor.execute(
                    """
                        INSERT INTO temp_category_table (user_id, category_id, category_name, order_count)
                        VALUES 
                        (
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            %(order_count)s
                        );
                    """,
                    {
                        'user_id': record['payload']['user_id'],
                        'category_id': record['payload']['category_id'],
                        'category_name': record['payload']['category_name'],
                        'order_count': int(record['payload']['order_cnt'])
                    }
                )

    def _insert_into_data_marts(self, cursor: Cursor):
        cursor.execute(
            """
                INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_count)
                SELECT  tt.user_id, 
                        tt.product_id, 
                        tt.product_name, 
                        SUM(tt.order_count)
                FROM temp_product_table AS tt
                GROUP BY user_id, product_id, product_name
                ON CONFLICT (user_id, product_id) DO UPDATE 
                SET product_name = EXCLUDED.product_name,
                    order_count = EXCLUDED.order_count;
            """
        )

        cursor.execute(
            """
                INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_count) 
                SELECT  tt.user_id, 
                        tt.category_id, 
                        tt.category_name, 
                        SUM(tt.order_count)
                FROM temp_category_table AS tt
                GROUP BY user_id, category_id, category_name
                ON CONFLICT (user_id, category_id) DO UPDATE 
                SET category_name = EXCLUDED.category_name,
                    order_count = EXCLUDED.order_count;
            """
        )