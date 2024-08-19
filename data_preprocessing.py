# data pre-processing file to prepare data before insertion to the DB
import pandas as pd


def return_tenant_store_mapping(json_data: list) -> pd.DataFrame:
    """Helper function to return the mapping from extTenantUUID to extStoreUUID.
    This would make it faster to use joins in tables"""
    df_tenant_store = pd.DataFrame()
    df_tenant_store['tenant_uuid'] = list(d['extTenantUUID'] for d in json_data)
    df_tenant_store['store_uuid'] = list(d['extStoreUUID'] for d in json_data)
    df_tenant_store.drop_duplicates(inplace=True)
    return df_tenant_store


def return_detailed_order_information_dataframe(json_data: list) -> pd.DataFrame:
    """
    Utilise the pandas functionality to normalise a dataframe, we wish to
    preserve most of the attributes of the order, and we can drop the other
    parts
    """
    all_df_lst = []
    for record in json_data:
        if 'bundles' in record:
            df_bundles = pd.json_normalize(
                data=record['bundles'],
                record_path=['itemTypes', 'items'],
                meta=[
                    'basketUUID',
                    'description',
                    'discount',
                    'menuUUID',
                    'name',
                    'status',
                    'subtotalAmount',
                    'totalAmount',
                    'uuid',
                    ['category', 'name'],
                    ['category', 'uuid'],
                    ['kitchenStation', 'name'],
                    ['kitchenStation', 'uuid']
                ],
                meta_prefix='bundle_'
            )
            df_bundles['order_uuid'] = record['uuid']
            all_df_lst.append(df_bundles)

    out_df = pd.concat(all_df_lst, ignore_index=True)
    return out_df


# although this isn't very efficient, it's good to process the data initially
def return_order_information_dataframe(json_data: list) -> pd.DataFrame:
    """Function to return all the metadata for each order, as there's no need
    to have it in the full orders information table. Link it using """
    orders_table_df = pd.DataFrame()
    measures_to_extract_dct = {
        'order_uuid': 'uuid',
        'tenant_uuid': 'extTenantUUID',
        'store_uuid': 'extStoreUUID',
        'created_at': 'createdAt',
        'requested_from': 'requestedFrom',
        'status': 'status',
        'takeaway': 'takeaway',
        'timezone': 'timezone'
    }
    for mapped_name, original_name in measures_to_extract_dct.items():
        orders_table_df[mapped_name] = list(d[original_name] for d in json_data)
    # --- abandoned
    ## # fix the date
    # orders_table_df['created_at'] = pd.to_datetime(
    #     orders_table_df['created_at'], unit='ms')
    # cannot get the localisation according to the timezone to work

    return orders_table_df


if __name__ == "__main__":
    from data_loading import load_data
    data_dir = r'/Users/philip.papasavvas/Downloads/vmj_int/'
    json_orders = load_data(filepath=f"{data_dir}/task_data.json")

    tenant_store_mapping_df = return_tenant_store_mapping(json_data=json_orders)
    order_items_df = return_detailed_order_information_dataframe(json_data=json_orders)
    orders_df = return_order_information_dataframe(json_data=json_orders)
