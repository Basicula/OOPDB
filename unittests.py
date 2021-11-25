from oopdb.OOPDB import OOPDB
from oopdb.ColumnConfig import PrimaryKey, DataTypes
import unittest
import sqlite3
import os

class TempFileHolder:
    def __init__(self, filename):
        self.filename = filename

    def __del__(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

def compare_files(file1, file2) -> bool:
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        data1 = f1.read()
        data2 = f2.read()
        return data1 == data2

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

if __name__ == "__main__":
    unittest.main()