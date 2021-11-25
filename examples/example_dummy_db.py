from oopdb.OOPDB import OOPDB
from oopdb.ColumnConfig import ColumnConfig, DataTypes

import random
import os

def create_dummy_db(table_cnt : int, max_columns_per_table : int, max_rows_per_table : int) -> None:
    db = OOPDB()
    db_file_path = "dummy.db"
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
    db.open(db_file_path)
    for table_id in range(table_cnt):
        column_cnt = random.randint(1, max_columns_per_table)
        columns = []
        column_names = []
        table_name = f"Table_{table_id}"
        for column_id in range(column_cnt):
            column_name = f"{table_name}_Column_{column_id}"
            column_names.append(column_name)
            columns.append(ColumnConfig(column_name, random.choice(list(DataTypes))))
        db.create_table(table_name, columns).execute()
        
        row_cnt = random.randint(1, max_rows_per_table)
        for row_id in range(1, row_cnt):
            row = []
            for column_id in range(column_cnt):
                if columns[column_id].type == DataTypes.INTEGER:
                    row.append(random.randint(1, 100))
                elif columns[column_id].type == DataTypes.TEXT:
                    row.append(f"{table_name}_({row_id}, {column_id})_Text")
            db.insert_into(table_name, column_names, row).execute()
    db.close()

def select_from_dummy():
    db = OOPDB()
    db.open("dummy.db")
    print(db.select("Table_0").fetch())

if __name__ == "__main__":
    create_dummy_db(10, 10, 100)
    select_from_dummy()