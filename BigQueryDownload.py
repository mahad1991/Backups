import pandas as pd
import pandas_gbq

project_id = "jlr-dl-mpldwh"

table_id = "jlr-dl-mpldwh.JLR_DL_MPLDWH_E2OPEN."

table_name = ["GOODS_RECEIPT_ARCHIVE",
"SHIPMENT_ARCHIVE",
"RECEIVING_CALENDAR_ARCHIVE",
"ITEM_MASTER_ARCHIVE",
"DCI_ARCHIVE",
"CURRENT_PARTS_NITRA_ARCHIVE",
"SUPPLIER_MASTER_ARCHIVE",
"FORECAST_HISTORY_ARCHIVE",
"INVENTORY_ARCHIVE",
"LOCATION_ITEM_ATTRIBUTES_ARCHIVE",
"CURRENT_PARTS_SCHEDULE_ARCHIVE",
"CONSUMPTION_HISTORY_ARCHIVE",
"LOCATION_MASTER_ARCHIVE",
"PROCESS_ATTRIBUTES_ARCHIVE"
]

for name in table_name: #looping through the table names (only archived tables)
    sql = f"SELECT * FROM {table_id}{name} where cast(RUN_DATETIME as string) like '2021-10-08 %'" #SQL Query with a where clause
    df = pd.read_gbq(sql, project_id=project_id, dialect='standard')#read big query table using the project and table name
    save_location = f"C:/Users/mkhan59/Documents/GCP_Extracts/{name}.csv" #variable to set the save name
    df.to_csv(save_location, index=False) #save to location
