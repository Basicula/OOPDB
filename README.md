# OOPDB

OOPDB - OOP wrapper for work with database. For now it's based on sqlite3 python lib.

## Supported database features

- [ ] Data types
    - [x] Integer (DataTypes.INTEGER) - simple type for storing integer numbers
    - [x] Text (DataTypes.TEXT) - simple type for storing text/strings with a maximum length of 65'535 bytes
- [ ] Column configurations
    - [x] Column config - base abstraction for describing column configuration using folowing information
        - [x] Name
        - [x] Nullability
        - [x] Type - type from DataTypes
    - [x] Primary key - abstraction based on column config abstraction using following additional information
        - [x] Autoincrement
    - [x] Foreign key - abstraction based on column config abstraction using following additional information
        - [x] Reference table name
        - [x] Reference column name
- [ ] Commands
    - [x] Create table - creates table with the given name and list of column configurations
    - [x] Select - select data from the given table and list of given column names in the table
    - [x] Table names - get all table names that are exist in database
    - [x] Column names - get all column names that are exist in the table with the given table name
    - [x] Insert into - append row values to the table with the given name and list of column names
    - [x] Select count - select row count from the table with the given name
    - [x] Inner join - merges two tables with the given table names and column names
    - [x] Order by - sort result by the given lists of column names and orders for each column
    - [x] Update - updates the table with the given name by the given lists of column names and new values for that columns