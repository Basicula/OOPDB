import enum

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
        res = self.name + " " + self.type.value
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