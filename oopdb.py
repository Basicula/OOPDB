import sqlite3
import enum
from typing import Any, List

def format_array(array : List[Any], element_wrapper : str = ''):
    return "(" + ", ".join(f"{element_wrapper}{str(elem)}{element_wrapper}" for elem in array) + ")"

class DataTypes(enum.Enum):
    '''
    Supported data types that maps on SQL data types
        TEXT
            Holds a string with a maximum length of 65,535 bytes
        INTEGER
            A medium integer. Signed range is from -2147483648 to 2147483647.
    '''
    TEXT = 'TEXT'
    INTEGER = 'INTEGER'

class ColumnConfig:
    '''
    Base column configuration as abstraction for data base column

    Commonly defined by 'name' and 'type' also 'is_null' optional option is available
    '''

    def __init__(self, name : str, type : DataTypes, is_null : bool = True) -> 'ColumnConfig':
        '''
        name : str, required
            The name for column
        sound : str, required
            The data type for column taken for supported data types(DataTypes)
        is_null : bool, optional
            Tells is the data in table cell can be null or always need to be set
        '''
        self.name = name
        self.type = type
        self.is_null = is_null

    def __str__(self) -> str:
        res = self.name + " " + self.type.name
        if not self.is_null:
            res += " NOT NULL"
        return res

class PrimaryKey(ColumnConfig):
    '''
    Primary key column configuration based of ColumnConfig

    Abstraction for primary key in data base
    '''

    def __init__(self, name : str, is_auto_increment : bool = True) -> 'PrimaryKey':
        '''
        name : str, required
            The name of primary key
        is_auto_increment: bool, optional
            Allows to increment primary key for each new row in table
            also allows not to set its value on each data insertion
        '''
        super(PrimaryKey, self).__init__(name, DataTypes.INTEGER, False)
        self.is_auto_increment = is_auto_increment

    def __str__(self) -> str:
        res = super(PrimaryKey, self).__str__()
        res += " PRIMARY KEY"
        if self.is_auto_increment:
            res += " AUTOINCREMENT"
        return res

class ForeignKey(ColumnConfig):
    '''
    Foreign key column configuration based of ColumnConfig

    Abstraction for foreign key in data base that is reference to some other table in data base
    '''
    def __init__(self, name : str, reference_table : str, reference_column : str):
        '''
        name : str, required
            The name of foreign key
        reference_table : str, required
            Name of the table referenced by the key
        reference_column : str, required
            Name of the column referenced by the key
        '''
        super(ForeignKey, self).__init__(name, DataTypes.INTEGER, False)
        self.reference_table = reference_table
        self.reference_column = reference_column

    def __str__(self) -> str:
        res = super(ForeignKey, self).__str__()
        res += f" REFERENCES {self.reference_table}({self.reference_column})"
        return res

class OOPDB:
    '''
    OOP abstraction for data base communication based on sqlite3
    '''

    def __init__(self, db_path : str) -> None:
        '''
        db_path : str, required
            The path to the data base
        If the file doesn't exist creates new empty data base using set path
        '''
        if db_path:
            try:
                self.connection = sqlite3.connect(db_path)
            except sqlite3.Error as e:
                print(f"The error '{e}' occurred")
        self.query = ""

    def execute(self) -> bool:
        '''
        Executes all queued commands
        
        Commonly used after pushing, updating and other commands without any output
        '''
        query = self.query + ";"
        self.query = ""
        try:
            cursor = self.connection.cursor()
            cursor.executescript(query)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"The error '{e}' occurred for query '{query}'")
            return False

        return True

    def fetch(self) -> List[Any]:
        '''
        Executes all queued commands

        Commonly used after select or other commands with any output

        Returns list of rows
        '''
        query = self.query + ";"
        self.query = ""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except sqlite3.Error as e:
            print(f"The error '{e}' occurred for query '{query}'")
            return []

    def close(self) -> None:
        self.connection.close()

    def create_table(self, table_name : str, columns : List[ColumnConfig]) -> 'OOPDB':
        '''
        Adds to the queue table creation command

        table_name : str, required
            The name for the new table
        columns : List[ColumnConfig], required
            List of column configs for new table
        '''
        self.query += f"CREATE TABLE {table_name} "
        self.query += format_array(columns) + ";"

        return self

    def insert_into(self, table_name : str, columns : List[str], values : List[Any]) -> 'OOPDB':
        '''
        Adds to the queue data row insertion command

        table_name : str, required
            The name for the target table
        columns : List[str], required
            List of column names that will be defined by new values
        values : List[Any], required
            List of values for selected columns
        '''
        self.query += f"INSERT INTO {table_name} "
        self.query += format_array(columns) + " "
        self.query += "VALUES "
        self.query += format_array(values, "\"") + ";"
        
        return self

    def select(self, table_name : str, columns : List[str] = []) -> 'OOPDB':
        '''
        Adds to the queue select data rows command

        table_name : str, required
            The name for the target table
        columns : List[str], optional
            List of column names, if empty all columns will be taken for result
        '''
        if len(columns) > 0:
            # format_array returns string with parentheses thats why substring[1:-1] is taken from returned string
            self.query += f"SELECT {format_array(columns)[1:-1]} "
        else:
            self.query += f"SELECT * "
        self.query += f"FROM {table_name} "

        return self

    def inner_join(self, table : str, table_column : str, target_table_column : str) -> 'OOPDB':
        '''
        Adds to the queue inner join command
        
        table : str, required
            The name for the target table
        table_column : str, required
            The name for the column in the selecting table on which joining will be applied
        target_table_column : str, required
            The name for the target table column on which joining will be applied
        '''
        self.query += f"INNER JOIN {table} ON {table_column} = {table}.{target_table_column} "
        return self