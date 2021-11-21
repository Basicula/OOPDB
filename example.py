from oopdb import *
from oopdb_utils import *

import random
import os

def create_test_db(table_cnt : int, max_columns_per_table : int, max_rows_per_table : int) -> None:
    db = OOPDB("test.db")
    for table_id in range(table_cnt):
        column_cnt = random.randint(1, max_columns_per_table)
        columns = []
        column_names = []
        table_name = f"Table_{table_id}"
        for column_id in range(column_cnt):
            column_names.append(f"{table_name}_Column_{column_id}")
            columns.append(
                ColumnConfig(
                    column_names[-1],
                    random.choice(list(DataTypes))
                    )
                )
        db.create_table(table_name, columns).execute()
        
        row_cnt = random.randint(1, max_rows_per_table)
        for row_id in range(1, row_cnt):
            row = []
            for column_id in range(column_cnt):
                if columns[column_id].type == DataTypes.INTEGER:
                    row.append(random.randint(1, 100))
                elif columns[column_id].type == DataTypes.TEXT:
                    row.append(f"\"{table_name}_({row_id}, {column_id})_Text\"")
            db.insert_into(table_name, column_names, row).execute()
    db.close()

def select_test():
    db = OOPDB("test.db")
    print(db.select("Table_0").fetch())

def tagged_content_example():
    db_file_path = "tagged_content.db"
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
    db = OOPDB(db_file_path)
    
    content_rows_cnt = 10
    content_table_name = "Content"
    content_id_column = PrimaryKey(name="Id",is_auto_increment=True)
    content_title_column = ColumnConfig(name="Title", type=DataTypes.TEXT, is_null=False)
    db.create_table(content_table_name, [content_id_column, content_title_column]).execute()
    for i in range(content_rows_cnt):
        db.insert_into(content_table_name, [content_title_column.name], [f"Column{i}"])
    db.execute()

    tags_rows_cnt = 5
    tags_table_name = "Tags"
    tags_id_column = PrimaryKey(name="Id")
    tags_name_column = ColumnConfig(name="Name", type=DataTypes.TEXT, is_null=False)
    db.create_table(tags_table_name, [tags_id_column, tags_name_column]).execute()
    for i in range(tags_rows_cnt):
        db.insert_into(tags_table_name, [tags_name_column.name], [f"Tag{i}"]).execute()

    relations_table_name = "Relations"
    relations_content_id_column = ForeignKey(name="ContentId", reference_table=content_table_name, reference_column=content_id_column.name)
    relations_tag_id_column = ForeignKey(name="TagId", reference_table=tags_table_name, reference_column=tags_id_column.name)
    db.create_table(relations_table_name, [relations_tag_id_column, relations_content_id_column]).execute()
    for content_id in range(content_rows_cnt):
        for tag_id in range(tags_rows_cnt):
            if bool(random.getrandbits(1)):
                db.insert_into(relations_table_name, [relations_content_id_column.name, relations_tag_id_column.name], [content_id, tag_id]).execute()

def select_relations_from_tagged():
    db = OOPDB("tagged_content.db")
    res = db.select("Content", ["Name", "Title"]).inner_join("Relations", "Content.Id", "ContentId").inner_join("Tags", "TagId", "Id").fetch()
    print_rows(res, ["Name", "Title"])

if __name__ == "__main__":
    #create_test_db(10, 10, 100)
    #select_test()
    tagged_content_example()
    select_relations_from_tagged()
