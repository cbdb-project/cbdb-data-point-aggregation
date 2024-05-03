import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect("latest.db")

# Define query parameters

# Time parameters for person
DY_LIST = (7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)
INDEX_YEAR_START = 907
INDEX_YEAR_END = 1234

# Get biographcical address data
MAIN_TABLE_NAME = "BIOG_ADDR_DATA"
MAIN_TABLE_DATA_POINT_ID_COLUMN = "c_addr_id"
MAIN_TABLE_DATA_POINT_NAME_TABLE = "ADDR_CODES"
MAIN_TABLE_DATA_POINT_NAME_COLUMN = "c_name_chn"
MAIN_TABLE_DATA_POINT_TYPE_TABLE = "BIOG_ADDR_CODES"
MAIN_TABLE_DATA_POINT_TYPE_ID_COLUMN = "c_addr_type"
MAIN_TABLE_DATA_POINT_TYPE_NAME_COLUMN = "c_addr_desc_chn"

# Join BIOG_MAIN to get person information
BIOG_MAIN_TABLE_NAME = "BIOG_MAIN"
MAIN_TABLE_PERSON_ID_COLUMN = "c_personid"
MAIN_TABLE_PERSON_NAME_COLUMN = "c_name_chn"
MAIN_TABLE_PERSON_INDEX_YEAR_COLUMN = "c_index_year"
MAIN_TABLE_PERSON_DY_COLUMN = "c_dy"
DYNASTY_TABLE_NAME = "DYNASTIES"
DYNASTY_NAME_COLUMN = "c_dynasty_chn"

# read person_id.txt
try:
    person_id = pd.read_csv("person_id.txt", header=None).values.tolist()
    person_id = tuple([i[0] for i in person_id])
except:
    person_id = ()

# Create sql query
sql_query = f"""
SELECT
    {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_ID_COLUMN} AS person_id,
    {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_NAME_COLUMN} AS person_name,
    {MAIN_TABLE_NAME}.{MAIN_TABLE_DATA_POINT_ID_COLUMN} AS data_point_id,
    {MAIN_TABLE_DATA_POINT_NAME_TABLE}.{MAIN_TABLE_DATA_POINT_NAME_COLUMN} AS data_point_name,
    {MAIN_TABLE_DATA_POINT_TYPE_TABLE}.{MAIN_TABLE_DATA_POINT_TYPE_ID_COLUMN} AS data_point_type_id,
    {MAIN_TABLE_DATA_POINT_TYPE_TABLE}.{MAIN_TABLE_DATA_POINT_TYPE_NAME_COLUMN} AS data_point_type_name,
    {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_INDEX_YEAR_COLUMN} AS index_year,
    {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_DY_COLUMN} AS dynasty,
    {DYNASTY_TABLE_NAME}.{DYNASTY_NAME_COLUMN} AS dynasty_name
FROM
    {MAIN_TABLE_NAME}
JOIN
    {MAIN_TABLE_DATA_POINT_NAME_TABLE}
ON
    {MAIN_TABLE_NAME}.{MAIN_TABLE_DATA_POINT_ID_COLUMN} = {MAIN_TABLE_DATA_POINT_NAME_TABLE}.{MAIN_TABLE_DATA_POINT_ID_COLUMN}
JOIN
    {MAIN_TABLE_DATA_POINT_TYPE_TABLE}
ON
    {MAIN_TABLE_NAME}.{MAIN_TABLE_DATA_POINT_TYPE_ID_COLUMN} = {MAIN_TABLE_DATA_POINT_TYPE_TABLE}.{MAIN_TABLE_DATA_POINT_TYPE_ID_COLUMN}
JOIN
    {BIOG_MAIN_TABLE_NAME}
ON
    {MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_ID_COLUMN} = {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_ID_COLUMN}
JOIN
    {DYNASTY_TABLE_NAME}
ON
    {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_DY_COLUMN} = {DYNASTY_TABLE_NAME}.{MAIN_TABLE_PERSON_DY_COLUMN}
WHERE
    ({BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_DY_COLUMN} IN {DY_LIST}
    OR(
        {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_INDEX_YEAR_COLUMN} >= {INDEX_YEAR_START}
        AND
        {BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_INDEX_YEAR_COLUMN} <= {INDEX_YEAR_END})
    )
"""

if len(person_id) != 0:
    sql_query += (
        f"AND ({BIOG_MAIN_TABLE_NAME}.{MAIN_TABLE_PERSON_ID_COLUMN} IN {person_id})"
    )

# print(sql_query)

# Execute the query
df = pd.read_sql_query(sql_query, conn)
df["index_year"] = df["index_year"].fillna("no_data")
df["dynasty"] = df["dynasty"].fillna("no_data")
df["dynasty_name"] = df["dynasty_name"].fillna("no_data")

SEP = "|"

print("Data loaded successfully!")
print("Start aggregating data...")


# aggregate data by personid, the output format is as follows:
# person_id, person_name, data_point_type_name1【data_point_name1】{SEP}data_point_type_name2【data_point_name2】{SEP}..., dynasty-dynasty_name, index_year
def aggregate_data(group):
    data_points = group.apply(
        lambda row: f'【{row["data_point_name"]}】{row["data_point_type_name"]}', axis=1
    ).tolist()
    person_name = group["person_name"].iloc[0]
    dynasty_info = f'{group["dynasty"].iloc[0]}-{group["dynasty_name"].iloc[0]}'
    index_year = group["index_year"].iloc[0]
    return pd.Series(
        [person_name, "|".join(data_points), dynasty_info, index_year],
        index=["person_name", "data_points", "dynasty_info", "index_year"],
    )


aggregated_data = df.groupby("person_id").apply(aggregate_data).reset_index()

conn.close()

print(aggregated_data.head())

aggregated_data.to_csv("aggregated_data.csv", index=False, encoding="utf_8_sig")
aggregated_data.to_excel("aggregated_data.xlsx", index=False)

print("Finished!")
