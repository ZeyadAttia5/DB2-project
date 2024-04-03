/**
 * @author Wael Abouelsaadat
 */

import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;


public class DBApp {


    public DBApp() {
//		String currentDirectory = System.getProperty("user.dir");
//		System.out.println("Current Directory: " + currentDirectory);

    }

    //

    public static void main(String[] args) {

        try {
            String strTableName = "Student";
            DBApp dbApp = new DBApp();
//
            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");
            dbApp.createTable(strTableName, "ID", htblColNameType);
//			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 2343432 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 453455 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0]._strTableName =  "Student";
//			arrSQLTerms[0]._strColumnName=  "name";
//			arrSQLTerms[0]._strOperator  =  "=";
//			arrSQLTerms[0]._objValue     =  "John Noor";
//
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 1.5 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
        } catch (Exception exp) {
            exp.printStackTrace();
        }
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() {


    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType) throws DBAppException {

        try {
            //create the metaData.csv file using the hashtable input and store it in the metaData package
            Metadata.writeToCSV(htblColNameType, strTableName, strClusteringKeyColumn);
            //initialize a new table object
            Table newTable = new Table(strTableName, strClusteringKeyColumn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // following method creates a B+tree index
    public void createIndex(String strTableName,
                            String strColName,
                            String strIndexName) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        // Get the metadata of the table
        List<String[]> metadata = Metadata.getTableMetadata(strTableName);

        // Iterate through each column in the metadata
        for (String[] col : metadata) {
            String columnName = col[1]; // Column Name
            String columnType = col[2]; // Column Type

            // Check if the column needs to be updated exists
            if (htblColNameValue.containsKey(columnName)) {
                // Get the new value for the column
                Object newValue = htblColNameValue.get(columnName);

                // Check the data type of the column and handle the update
                if (columnType.equals("java.lang.Integer") && newValue instanceof Integer) {
                    // Perform update for Integer type
                    Integer intValue = (Integer) newValue;
                    // Update the row in the database with intValue
                } else if (columnType.equals("java.lang.String") && newValue instanceof String ) {
                    // Perform update for String type
                    String stringValue = (String) newValue;
                    // Update the row in the database with stringValue
                } else if (columnType.equals("java.lang.Double") && newValue instanceof Double) {
                    // Perform update for Double type
                    double doubleValue = (double) newValue;
                    // Update the row in the database with doubleValue
                } else {
                    throw new DBAppException("Invalid data type for column: " + columnName);
                }
            }
        }
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {

        return null;
    }

}