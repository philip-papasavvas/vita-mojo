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
        'tenant_store_map',
        con=conn
    )
    detailed_orders_df.to_sql('order_items', con=conn)
    # create an index on the order uuid
    conn.execute(
        'CREATE INDEX idx_order_items_order_uuid ON order_items (order_uuid)'
    )
    orders_info_df.to_sql('orders', con=conn)
    # create index on order uuid
    conn.execute(
        'CREATE INDEX idx_order_order_uuid ON orders (order_uuid)'
    )


if __name__ == '__main__':
    from data_loading import load_data
    from data_preprocessing import (
        return_tenant_store_mapping, return_order_information_dataframe,
        return_detailed_order_information_dataframe
    )
    data_dir = r'/Users/philip.papasavvas/Downloads/vmj_int/'
    json_orders = load_data(filepath=f"{data_dir}/task_data.json")

    detailed_orders_df = return_detailed_order_information_dataframe(
        json_data=json_orders)
    order_high_level_info_df = return_order_information_dataframe(
        json_data=json_orders)
    tenant_store_map_df = return_tenant_store_mapping(
        json_data=json_orders
    )
    conn = create_database(f"{data_dir}/vita_mojo_orders.db")
    insert_data_to_db(
        conn=conn,
        tenant_store_map_df=tenant_store_map_df,
        detailed_orders_df=detailed_orders_df,
        orders_info_df=order_high_level_info_df
    )

    # QUESTIONS
    # Q1: How many orders are there in total?
    total_orders = conn.execute("SELECT COUNT(*) FROM orders").fetchall()
    # 5865 total orders

    # Q2: How many orders from each channel
    query = """
    SELECT 
        DISTINCT("requested_from"), COUNT(*)
    FROM 
        orders
    GROUP BY 1
    ORDER BY 2 DESC
    """
    orders_per_channel = conn.execute(query).fetchall()
    # [('opat', 3985), ('kiosk', 779), ('pos', 649), ('online', 426), ('delivery', 26)]

    # Q3: Items sold at each hour of the day for each tenant?
    # -----
    # abandoned due to spending too much time converting timezone
    # (see function return_order_information_dataframe, lines 70-74)
    # query = """
    # SELECT DATETIME(created_at/ 1000.0, 'unixepoch', timezone) AS local_time
    # FROM
    # orders
    # LIMIT 5"""
    # cursor.execute(query)
    # result = cursor.fetchall()

    # Q4: Top 5 items sold for each tenant?
    query = """
    SELECT 
        tenant_uuid,
        name,
        order_count
    FROM 
        (
            SELECT 
                orders.tenant_uuid, 
                order_items.name, 
                COUNT(*) as order_count,
                ROW_NUMBER() OVER (PARTITION BY orders.tenant_uuid ORDER BY COUNT(*) DESC
                ) as row_number        
            FROM 
                orders
            LEFT JOIN
                order_items
            ON 
                orders.order_uuid = order_items.order_uuid
            GROUP BY 
                orders.tenant_uuid,
                order_items.name
        ) AS subquery
    WHERE row_number <= 5
    ORDER BY 
        tenant_uuid, order_count DESC
    """
    result = conn.execute(query).fetchall()
    # see the result below
    result_df = pd.DataFrame(result, columns=['tenant_uuid', 'name', 'order_count'])
    #                               tenant_uuid                        name  order_count
    # 0    030b0972-3e96-11ea-975d-06dea2640edc                   Six Wings           39
    # 1    030b0972-3e96-11ea-975d-06dea2640edc                French Fries           33
    # 2    030b0972-3e96-11ea-975d-06dea2640edc                Twelve Wings           28
    # 3    030b0972-3e96-11ea-975d-06dea2640edc                Loaded Fries           27
    # 4    030b0972-3e96-11ea-975d-06dea2640edc                 Fruit Punch           18
    # ..                                    ...                         ...          ...
    # 119  f983770c-4943-46c4-8b65-03d2c52dd558       Viet Drip White (hot)            9
    # 120  f983770c-4943-46c4-8b65-03d2c52dd558        Iced Viet Drip Black            5
    # 121  f983770c-4943-46c4-8b65-03d2c52dd558       Viet Drip Black (hot)            4
    # 122  f983770c-4943-46c4-8b65-03d2c52dd558        Iced Viet Drip White            4
    # 123  f983770c-4943-46c4-8b65-03d2c52dd558  Lemongrass Chicken Hot Box            3
    # [124 rows x 3 columns]

    # Q5. What were the items for each tenant that were sold more than 5 of?
    # ----------
    # change the above query very slightly
    query = """
    SELECT 
        tenant_uuid,
        name
    FROM 
        (
            SELECT 
                orders.tenant_uuid, 
                order_items.name, 
                COUNT(*) as order_count,
                ROW_NUMBER() OVER (PARTITION BY orders.tenant_uuid ORDER BY COUNT(*) DESC
                ) as row_number        
            FROM 
                orders
            LEFT JOIN
                order_items
            ON 
                orders.order_uuid = order_items.order_uuid
            GROUP BY 
                orders.tenant_uuid,
                order_items.name
        ) AS subquery
    WHERE order_count > 5
    ORDER BY 
        tenant_uuid DESC
    """
    result = conn.execute(query).fetchall()
    # print result
    pd.DataFrame(result, columns=['tenant_id', 'name'])
    #                                 tenant_id                            name
    # 0    f983770c-4943-46c4-8b65-03d2c52dd558           Viet Drip White (hot)
    # 1    f63d7d52-0fa8-4d56-b954-a9ecb86e350a                             soy
    # 2    f63d7d52-0fa8-4d56-b954-a9ecb86e350a             chicken katsu curry
    # 3    f63d7d52-0fa8-4d56-b954-a9ecb86e350a                       YO! fries
    # 4    f63d7d52-0fa8-4d56-b954-a9ecb86e350a                         napkins
    # ..                                    ...                             ...
    # 638  030b0972-3e96-11ea-975d-06dea2640edc       Buffalo & Blue Cheese Dip
    # 639  030b0972-3e96-11ea-975d-06dea2640edc          Sour Cream & Chive Dip
    # 640  030b0972-3e96-11ea-975d-06dea2640edc               Garlic & Herb Dip
    # 641  030b0972-3e96-11ea-975d-06dea2640edc               Fanta Fruit Twist
    # 642  030b0972-3e96-11ea-975d-06dea2640edc  Eight Boneless Chicken Fillets
    # [643 rows x 2 columns]

    # Q6: Ran out of time