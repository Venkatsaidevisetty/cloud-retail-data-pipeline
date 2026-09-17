# test_data_quality.py
#
# Purpose: Data quality tests for the pipeline (e.g. using pytest).
#
# Planned checks (not implemented yet):
# - No null customer_id / order_id / product_id
# - No duplicate primary keys
# - order quantities are positive numbers
# - foreign keys in orders.csv match ids in customers.csv / products.csv
