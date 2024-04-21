# Small Database Engine with B+ Tree Index Support

This project implements a small database engine supporting a B+ tree index and ANTLR-based SQL statement processing capabilities. It offers a range of features, including table creation, tuple insertion and updating, tuple deletion, and binary search for efficient page and tuple retrieval whenever applicable. It's essential to recognize that this database engine is designed for simplicity, thus excluding functionalities such as foreign keys, referential integrity constraints, or joins.


## Team
<a href="https://github.com/ZeyadAttia5/DB2-project/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=ZeyadAttia5/DB2-project" />
</a>

[//]: # (Made with [contrib.rocks]&#40;https://contrib.rocks&#41;.)

[//]: # (1. [Jana Saad]&#40;https://github.com/janasaad7&#41;)

[//]: # (2. [Moaaz]&#40;https://github.com/Moaaz101&#41;)

[//]: # (3. [Nabila Shrief]&#40;https://github.com/nabilasherif&#41;)

[//]: # (4. [Salma Rashad]&#40;https://github.com/salmarashad&#41;)

[//]: # (5. [Yahya Alazhary]&#40;https://github.com/YahyaAlAzhary&#41;)

[//]: # (6. [Zeyad Attia]&#40;https://github.com/ZeyadAttia5&#41;)


## Table of Contents
- [Features](#features)
- [Tools Used](#tools-used)
- [Installation](#installation)
- [Usage](#usage)
- [License](#license)


## Features

1. **Creating Tables:** Allows the creation of tables within the database.
2. **Inserting and Updating Tuples:** Supports inserting new tuples into tables and updating existing tuples.
3. **Deleting Tuples:** Enables the deletion of tuples from tables.
4. **Linear Searching:** Provides functionality for searching in tables linearly.
5. **B+ Tree Index:** Supports the creation of a B+ tree index on demand.
6. **Index Usage:** Utilizes the created indices for efficient data retrieval where applicable.
7. **SQL Statement Processing:** Includes support for processing SQL statements using ANTLR, ensuring that only statements compatible with the mini database engine (Select, createIndex, insert, delete, and update) are accepted.

## Tools Used

- Java
- Maven
- Git
- ANTLR (Parser Generator)


## Installation

1. Clone the repository:
   ```shell
   git clone https://github.com/ZeyadAttia5/DB2-project.git
   ```
2. Clone the repository:
   ```shell
   cd project-directory
   ```
3. Clone the repository:
   ```shell
   mvn clean install
   ```

## Usage

1. **Modify Test Class:**
   - Edit the `main` function in the `Test` class to include your test cases or interactions with the database.

2. **Use DBApp Functions:**
   - Utilize the public functions of the `DBApp` class to perform database operations:
      - Insert data into a table:
        ```java
        dbApp.insertIntoTable("TableName", htblColNameValue);
        ```
      - Update data in a table:
        ```java
        dbApp.updateTable("TableName", "ClusteringKeyValue", htblColNameValue);
        ```
      - Delete data from a table:
        ```java
        dbApp.deleteFromTable("TableName", htblColNameValue);
        ```
      - Select data from a table based on conditions:
        ```java
        dbApp.selectFromTable(arrSQLTerms, strarrOperators);
        ```
      - Create an index on a column:
        ```java
        dbApp.createIndex("TableName", "ColumnName", "IndexName");
        ```
      - Create a new table with specified columns and data types:
        ```java
        dbApp.createTable("TableName", "ClusteringKeyColumn", htblColNameType);
        ```

3. **SQL Statements:**
   - Use SQL statements compatible with the mini database engine to interact with the database. For example:
     ```sql
     CREATE TABLE table_name (
         column1 datatype,
         column2 datatype,
         column3 datatype,
     );
     SELECT * FROM TableName WHERE Condition;
     CREATE INDEX IndexName ON TableName (ColumnName);
     INSERT INTO TableName (Column1, Column2) VALUES (Value1, Value2);
     UPDATE TableName SET Column1=Value1 WHERE Condition;
     DELETE FROM TableName WHERE Condition;
     ```

4. **Testing and Execution:**
   - After making changes and setting up your database operations, run the modified `main` function in the `Test` class to execute your code.

### Public Functions in DBApp Class

#### `public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException`

This function allows you to insert data into a specified table.

- `strTableName`: Name of the table to insert into.
- `htblColNameValue`: Hashtable containing column names as keys and corresponding values to insert.

#### `public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException`

This function lets you update data in a specified table based on a clustering key value.

- `strTableName`: Name of the table to update.
- `strClusteringKeyValue`: Value of the clustering key for the record to update.
- `htblColNameValue`: Hashtable containing column names as keys and corresponding values to update.

#### `public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException`

This function allows you to delete data from a specified table based on column values.

- `strTableName`: Name of the table to delete from.
- `htblColNameValue`: Hashtable containing column names as keys and corresponding values to match for deletion.

#### `public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException`

This function allows you to select data from a specified table based on SQL terms and operators.

- `arrSQLTerms`: Array of SQLTerm objects representing individual conditions for selection.
- `strarrOperators`: Array of String operators (e.g., "AND", "OR") to combine SQL terms.

#### `public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException`

This function allows you to create an index on a specified column of a table.

- `strTableName`: Name of the table to create the index on.
- `strColName`: Name of the column to create the index on.
- `strIndexName`: Name of the index to create.

#### `public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException`

This function allows you to create a new table with specified columns and data types.

- `strTableName`: Name of the table to create.
- `strClusteringKeyColumn`: Name of the column that will be the primary key and clustering column.
- `htblColNameType`: Hashtable containing column names as keys and corresponding data types as values.


All functions throw a `DBAppException` in case of errors or invalid operations.

## License
This project is licensed under the MIT License.
