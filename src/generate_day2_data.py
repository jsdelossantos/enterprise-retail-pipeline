import pandas as pd
from pipeline import extract_data

# load the existing CSV first
df = extract_data("olist_orders_dataset.csv")

# change the status of first order
df.loc[0, 'order_status'] = 'invoiced'

print("Step 1 & 2 Complete: First row updated to 'invoiced'.")

print(df.head())


# defining a new order
new_order = {
    'order_id': 'DAY2_TEST_ORDER_999',
    'customer_id': 'DAY2_TEST_CUSTOMER',
    'order_status': 'invoiced',
    'order_purchase_timestamp': '2026-08-19 10:00:00',
    'order_approved_at': '2026-08-19 10:15:00',
    'order_delivered_carrier_date': None,
    'order_delivered_customer_date': None,
    'order_estimated_delivery_date': '2026-08-30 00:00:00'
}

# converting that order into a pd dataframe
new_order_df = pd.DataFrame([new_order])

# concatenating the two dataframes
df = pd.concat([df, new_order_df], ignore_index=True)

# save the mutated data back to the CSV
df.to_csv("../data/olist_orders_dataset.csv", index=False)
print("Day 2 Synthetic Data Generated and Saved!")