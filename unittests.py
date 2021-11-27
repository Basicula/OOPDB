from oopdb.OOPDB import OOPDB
from oopdb.ColumnConfig import ColumnConfig, PrimaryKey, DataTypes
import unittest
import sqlite3
import os
from typing import List

class TempFileHolder:
    def __init__(self, filename : str):
        self.filename = filename

    def __del__(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

class TempDB:
    def __init__(self):
        self.holder = TempFileHolder("temp.db")
        self.db = OOPDB()
        self.db.open(self.holder.filename)

    def __del__(self):
        self.db.close()

def compare_files(file1 : str, file2 : str) -> bool:
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        data1 = f1.read()
        data2 = f2.read()
        return data1 == data2

def add_table_to_db(db : OOPDB, table_name : str, column_names : List[str], row_cnt : int, row_content : List[str] = []) -> None:
    columns = []
    for column_name in column_names:
        columns.append(ColumnConfig(column_name, DataTypes.TEXT))
    db.create_table(table_name, columns).execute()
    for row in range(row_cnt):
        values = []
        if len(row_content) > 0:
            values = row_content
        else:
            for col in range(len(columns)):
                values.append(f"Content_{row}_{col}")
        db.insert_into(table_name, column_names, values).execute()

class TestOOPDB(unittest.TestCase):
    def test_creation(self):
        table_name = "table_name"
        temp1 = TempFileHolder("temp1.db")
        temp2 = TempFileHolder("temp2.db")

        # create through wrapper
        db =  OOPDB()
        db.open(temp1.filename)
        column = PrimaryKey(name="id", is_auto_increment=False)
        db.create_table(table_name, [column]).execute()
        self.assertTrue(os.path.exists(temp1.filename))

        # raw creation
        connection = sqlite3.connect(temp2.filename)
        connection.execute(f"CREATE TABLE {table_name} (id {DataTypes.INTEGER.name} NOT NULL PRIMARY KEY);")
        self.assertTrue(os.path.exists(temp2.filename))

        db.close()
        connection.close()

        self.assertTrue(compare_files(temp1.filename, temp2.filename))

    def test_tables_creation_table_names(self):
        temp_db = TempDB()
        db = temp_db.db

        table_cnt = 10
        expected_table_names = []
        for i in range(table_cnt):
            table_name = f"Table_{i}"
            expected_table_names.append(table_name)
            add_table_to_db(db, table_name, ["Column"], 0)
        actual_table_names = db.table_names().fetch()
        actual_table_names = [a[0] for a in actual_table_names]

        self.assertListEqual(expected_table_names, actual_table_names)

    def test_tables_creation_column_names(self):
        temp_db = TempDB()
        db = temp_db.db

        column_cnt = 10
        expected_column_names = []
        for i in range(column_cnt):
            column_name = f"Column_{i}"
            expected_column_names.append(column_name)
        table_name = "TestTable"
        add_table_to_db(db, table_name, expected_column_names, 0)
        actual_column_names = db.column_names(table_name).fetch()
        actual_column_names = [a[0] for a in actual_column_names]

        self.assertListEqual(expected_column_names, actual_column_names)

    def test_data_selection_count(self):
        temp_db = TempDB()
        db = temp_db.db

        table_name = "TestTable"
        column_name = "TestColumn"
        row_cnt = 123
        add_table_to_db(db, table_name, [column_name], row_cnt)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

    def test_data_selection(self):
        temp_db = TempDB()
        db = temp_db.db

        table_name = "TestTable"
        column_names = ["TestColumn"]
        row_cnt = 123
        row_content = ["RowContent"]
        add_table_to_db(db, table_name, column_names, row_cnt, row_content)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        actual_row_contents = db.select(table_name).fetch()
        for actual_row_content in actual_row_contents:
            self.assertEqual(row_content[0], actual_row_content[0])


if __name__ == "__main__":
    unittest.main()