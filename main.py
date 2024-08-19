# main part of the script to write the data to the database
# and answer the questions
import sqlite3
import pandas as pd


def create_database(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    print(f"Database created at: {db_path}")
    return conn


def insert_data_to_db(
    conn: sqlite3.Connection,
    tenant_store_map_df: pd.DataFrame,
    detailed_orders_df: pd.DataFrame,
    orders_info_df: pd.DataFrame
):
    tenant_store_map_df.to_sql(
        con=conn, if_exists='replace'
    )
    detailed_orders_df.to_sql('order_items', con=conn)
    # create an index on the order uuid
    conn.execute(
        'CREATE INDEX idx_order_items_order_uuid ON order_items (order_uuid)'
    )
    orders_info_df.to_sql('orders', con=conn)
    # create index on order uuid
    conn.execute(
        'CREATE INDEX idx_order_items_order_uuid ON order_items (order_uuid)'
    )


if __name__ == '__main__':
    from data_loading import load_data
    from data_preprocessing import (
        return_tenant_store_mapping, return_order_information_dataframe,
        return_detailed_order_information_dataframe
    )
    data_dir = r'/Users/philip.papasavvas/Downloads/vmj_int/'

