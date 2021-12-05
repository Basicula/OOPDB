from oopdb.OOPDB import OOPDB, RowsStyle
from oopdb.ColumnConfig import ColumnConfig, PrimaryKey, DataTypes
from oopdb.Expression import Expression, Operation
import unittest
import sqlite3
import os
from typing import Any, List

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

def add_table_to_db(db : OOPDB, table_name : str, columns : List[ColumnConfig], rows_content : List[List[Any]] = []) -> None:
    column_names = []
    for column in columns:
        column_names.append(column.name)
    db.create_table(table_name, columns).execute()
    for row in range(len(rows_content)):
        db.insert_into(table_name, column_names, rows_content[row]).execute()

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
        column = ColumnConfig("Column", DataTypes.TEXT)
        expected_table_names = []
        for i in range(table_cnt):
            table_name = f"Table_{i}"
            expected_table_names.append(table_name)
            add_table_to_db(db, table_name, [column])
        actual_table_names = db.table_names().fetch()
        actual_table_names = [a[0] for a in actual_table_names]

        self.assertListEqual(expected_table_names, actual_table_names)

    def test_tables_creation_column_names(self):
        temp_db = TempDB()
        db = temp_db.db

        column_cnt = 10
        expected_column_names = []
        columns = []
        for i in range(column_cnt):
            columns.append(ColumnConfig(f"Column_{i}", DataTypes.TEXT))
            expected_column_names.append(columns[-1].name)
        table_name = "TestTable"
        add_table_to_db(db, table_name, columns)
        actual_column_names = db.column_names(table_name).fetch()
        actual_column_names = [a[0] for a in actual_column_names]

        self.assertListEqual(expected_column_names, actual_column_names)

    def test_data_selection(self):
        temp_db = TempDB()
        db = temp_db.db

        table_name = "TestTable"
        columns = [ColumnConfig("TestColumn", DataTypes.TEXT)]
        row_cnt = 123
        row_content = ["RowContent"]
        rows_content = [row_content for _ in range(row_cnt)]
        add_table_to_db(db, table_name, columns, rows_content)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        actual_row_contents = db.select(table_name).fetch()
        for actual_row_content in actual_row_contents:
            self.assertEqual(row_content[0], actual_row_content[0])

    def test_dictionary_row_styles(self):
        temp_db = TempDB()
        db = temp_db.db

        table_name = "TestTable"
        columns = [ColumnConfig("TestColumn_1", DataTypes.TEXT), ColumnConfig("TestColumn_2", DataTypes.TEXT)]
        row_cnt = 123
        row_content = ["TestColumn_1Content_1", "TestColumn_2Content_2"]
        rows_content = [row_content for _ in range(row_cnt)]
        add_table_to_db(db, table_name, columns, rows_content)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        actual_row_contents = db.select(table_name).fetch(RowsStyle.DICTIONARY)
        self.assertEqual(type(actual_row_contents[0]), dict)
        for actual_row_content in actual_row_contents:
            self.assertListEqual(list(actual_row_content.keys()), [columns[0].name, columns[1].name])
        for actual_row_content in actual_row_contents:
            self.assertListEqual(row_content, [actual_row_content[columns[0].name], actual_row_content[columns[1].name]])

    def test_integer_type(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "Integers"
        int_column = ColumnConfig("Integers", DataTypes.INTEGER, False)
        db.create_table(table_name, [int_column]).execute()
        db.insert_into(table_name, [int_column.name], [321]).execute()
        result = db.select(table_name).fetch()
        self.assertEqual(type(result[0][0]), int)
        self.assertEqual(result[0][0], 321)

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
        int_column = ColumnConfig("Integers", DataTypes.INTEGER, False)
        text_column = ColumnConfig("Text", DataTypes.TEXT, False)
        bool_column = ColumnConfig("Bools", DataTypes.BOOL, False)
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

    def test_where(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "TestTable"
        int_column = ColumnConfig("Id", DataTypes.INTEGER, False)
        text_column = ColumnConfig("Text", DataTypes.TEXT, False)
        bool_column = ColumnConfig("Enable", DataTypes.BOOL, False)
        row_cnt = 100
        rows = []
        for row_id in range(row_cnt):
            rows.append((row_id, f"ThisIsRow{row_id}", bool(row_id % 2)))
        add_table_to_db(db, table_name, [int_column, text_column, bool_column], rows)
        
        is_enabled = Expression(bool_column.name, Operation.EQUAL, True)
        enabled_rows_cnt = db.select_count(table_name).where(is_enabled).fetch()[0][0]
        self.assertEqual(enabled_rows_cnt, 50)
        enabled_rows = db.select(table_name).where(is_enabled).fetch()
        for row in enabled_rows:
            self.assertTrue(bool(row[0] % 2))
            self.assertTrue(row[2])

        text_like = Expression(text_column.name, Operation.LIKE, "ThisIsRow2%")
        regex_rows_cnt = db.select_count(table_name).where(text_like).fetch()[0][0]
        self.assertEqual(regex_rows_cnt, 11)

    def test_update_where(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "TestTable"
        int_column = ColumnConfig("Id", DataTypes.INTEGER, False)
        text_column = ColumnConfig("Text", DataTypes.TEXT, False)
        bool_column = ColumnConfig("Enable", DataTypes.BOOL, False)
        row_cnt = 100
        rows = []
        for row_id in range(row_cnt):
            rows.append([row_id, f"ThisIsRow{row_id}", bool(row_id % 2)])
        add_table_to_db(db, table_name, [int_column, text_column, bool_column], rows)
        
        is_enabled = Expression(bool_column.name, Operation.EQUAL, True)
        enabled_rows = db.select(table_name).where(is_enabled).fetch()
        for row in enabled_rows:
            self.assertTrue(bool(row[0] % 2))
            self.assertTrue(row[2])

        db.update(table_name, [bool_column.name], [False]).where(is_enabled).execute()
        enabled_rows_cnt = db.select_count(table_name).where(is_enabled).fetch()[0][0]
        self.assertEqual(enabled_rows_cnt, 0)

    def test_delete(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "TestTable"
        int_column = ColumnConfig("Id", DataTypes.INTEGER, False)
        row_cnt = 100
        rows = []
        for row_id in range(row_cnt):
            rows.append([row_id])
        add_table_to_db(db, table_name, [int_column], rows)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        db.delete(table_name).execute()
        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(0, actual_row_cnt)

    def test_delete_where(self):
        temp_db = TempDB()
        db = temp_db.db
        
        table_name = "TestTable"
        int_column = ColumnConfig("Id", DataTypes.INTEGER, False)
        bool_column = ColumnConfig("Enable", DataTypes.BOOL, False)
        row_cnt = 100
        rows = []
        for row_id in range(row_cnt):
            rows.append([row_id, bool(row_id % 2)])
        add_table_to_db(db, table_name, [int_column, bool_column], rows)

        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(row_cnt, actual_row_cnt)

        is_enable = Expression("Enable", Operation.EQUAL, True)
        db.delete(table_name).where(is_enable).execute()
        actual_row_cnt = db.select_count(table_name).fetch()[0][0]
        self.assertEqual(50, actual_row_cnt)

        disabled_rows = db.select(table_name).fetch(RowsStyle.DICTIONARY)
        for row in disabled_rows:
            self.assertFalse(bool(row["Id"] % 2))

if __name__ == "__main__":
    unittest.main()