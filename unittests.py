from oopdb.OOPDB import OOPDB, RowsStyle
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

    def test_dictionary_row_styles(self):
        temp_db = TempDB()
        db = temp_db.db

        table_name = "TestTable"
        column_names = ["TestColumn_1", "TestColumn_2"]
        row_cnt = 123
        row_content = ["TestColumn_1Content_1", "TestColumn_2Content_2"]
        add_table_to_db(db, table_name, column_names, row_cnt, row_content)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        actual_row_contents = db.select(table_name).fetch(RowsStyle.DICTIONARY)
        self.assertEqual(type(actual_row_contents[0]), dict)
        for actual_row_content in actual_row_contents:
            self.assertListEqual(list(actual_row_content.keys()), column_names)
        for actual_row_content in actual_row_contents:
            self.assertListEqual(row_content, [actual_row_content[column_names[0]], actual_row_content[column_names[1]]])

    def test_integer_type(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "Integers"
        int_column = ColumnConfig("Integers", DataTypes.INTEGER, False)
        db.create_table(table_name, [int_column]).execute()
        db.insert_into(table_name, [int_column.name], [321]).execute()
        result = db.select(table_name).fetch()[0][0]
        self.assertEqual(type(result), int)

    def test_bool_type(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "Bools"
        bool_column = ColumnConfig("Bools", DataTypes.BOOL, False)
        db.create_table(table_name, [bool_column]).execute()
        db.insert_into(table_name, [bool_column.name], [True]).execute()
        db.insert_into(table_name, [bool_column.name], [False]).execute()
        results = db.select(table_name).fetch()
        self.assertEqual(type(results[0][0]), bool)
        self.assertTrue(results[0][0])
        self.assertEqual(type(results[1][0]), bool)
        self.assertFalse(results[1][0])

    def test_updating(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "TestTable"
        bool_column = ColumnConfig("Bools", DataTypes.BOOL, False)
        int_column = ColumnConfig("Integers", DataTypes.INTEGER, False)
        text_column = ColumnConfig("Text", DataTypes.TEXT, False)
        column_names = [int_column.name, text_column.name, bool_column.name]
        db.create_table(table_name, [int_column, text_column, bool_column]).execute()
        db.insert_into(table_name, column_names, [123, "Test123", False]).execute()
        initial_values = db.select(table_name).fetch()
        self.assertEqual(len(initial_values), 1)
        self.assertEqual(initial_values[0][0], 123)
        self.assertEqual(initial_values[0][1], "Test123")
        self.assertEqual(initial_values[0][2], False)

        db.update(table_name, column_names, [321, "Test321", True]).execute()
        updated_values = db.select(table_name).fetch()
        self.assertEqual(len(updated_values), 1)
        self.assertEqual(updated_values[0][0], 321)
        self.assertEqual(updated_values[0][1], "Test321")
        self.assertEqual(updated_values[0][2], True)

if __name__ == "__main__":
    unittest.main()