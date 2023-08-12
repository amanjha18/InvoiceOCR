import pandas as pd

# Sample data for Shipment Cost table
shipment_cost_data = {
    'shipment_cost_number': [109867, 109867, 109887],
    'condition_document_no': [45678, 45679, 34567]
}

# Sample data for Shipment Cost Item table
shipment_cost_item_data = {
    'condition_document_no': [45678, 45678, 45678, 45678, 45679, 45679, 34567],
    'net_amt': [5678, 34, 65, 68, 0, 0, 123],
    'calc_base': ['B', 'B', 'B' ,'B', 'P', 'D', 'B'],
    'delivery_doc_no': [89765, 89765, 89765, 89765, 0, 10675, 87965],
    'delivery_doc_item_no': [10, 10, 20, 20, 0, 0, 30]
}

# Create data frames from the sample data
shipment_cost_df = pd.DataFrame(shipment_cost_data)
shipment_cost_item_df = pd.DataFrame(shipment_cost_item_data)

# Merge the two data frames on the common key 'condition_document_no'
merged_df = pd.merge(shipment_cost_df, shipment_cost_item_df, on='condition_document_no', how='left')

# Apply aggregation rules
def aggregate_data(group):
    if 'B' in group['calc_base'].values:
        group['net_amt'] = group['net_amt'].sum()
    elif 'P' in group['calc_base'].values:
        sort_group = group[group['calc_base'] == 'B'].sort_values(by=['delivery_doc_no', 'delivery_doc_item_no'], ascending=[False, False])
        if not sort_group.empty:
            group['net_amt'] = sort_group['net_amt'].values[0]
            group['delivery_doc_no'] = sort_group['delivery_doc_no'].values[0]
            group['delivery_doc_item_no'] = sort_group['delivery_doc_item_no'].values[0]
    elif 'D' in group['calc_base'].values:
        sort_group = group[group['calc_base'] == 'B'].sort_values(by=['delivery_doc_no', 'delivery_doc_item_no'])
        if not sort_group.empty:
            group['net_amt'] = sort_group['net_amt'].values[0]
            group['delivery_doc_no'] = sort_group['delivery_doc_no'].values[0]
            group['delivery_doc_item_no'] = sort_group['delivery_doc_item_no'].values[0]
    return group

final_report_df = merged_df.groupby(['shipment_cost_number', 'condition_document_no']).apply(aggregate_data)

# Select and reorder columns for the final report
final_report_df = final_report_df[['shipment_cost_number', 'condition_document_no', 'net_amt', 'calc_base', 'delivery_doc_no', 'delivery_doc_item_no']]

# Print the final report
print(final_report_df)
