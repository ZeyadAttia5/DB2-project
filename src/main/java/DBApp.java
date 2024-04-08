
/** * @author Wael Abouelsaadat */

import java.io.IOException;
import java.util.Iterator;
import java.util.Hashtable;


public class DBApp {



	public DBApp( ){

	}


	//

	// this does whatever initialization you would like
	// or leave it empty if there is no code you want to
	// execute at application startup
	public void init( ){


	}


	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException{

		try {

			//initialize a new table object
			Table newTable = new Table(strTableName);
			// should do something here to prevent calling createTable twice onthe same parameters from overwriting a serialized object
			newTable.serialize();
			//create the metaData.csv file using the hashtable input and store it in the metaData package
			csvConverter.convert(htblColNameType, strTableName, strClusteringKeyColumn);

		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

	// following method creates a B+tree index
	public void createIndex(String   strTableName, String   strColName, String   strIndexName) throws DBAppException{

		// adjusting metadata file with new index
		boolean found = csvConverter.addIndexToCSV(strTableName, strColName, strIndexName);
		if(!found) {
			DBAppException e = new DBAppException("The index name " + strIndexName + " already exists in " + strTableName );
			throw e;
		}

		// retrieving data type for desired column
		String tmp = csvConverter.getDataType(strTableName, strColName);

		// initialising b+tree
		BPTree tree = null;
		if (tmp.equalsIgnoreCase("java.lang.Integer")) {
			tree = new BPTree<Integer>(10);

		} else if (tmp.equalsIgnoreCase("java.lang.String")) {
			tree = new BPTree<String>(10);

		} else if (tmp.equalsIgnoreCase("java.lang.Double")) {
			tree = new BPTree<Double>(10);
		} else {
			DBAppException e = new DBAppException("Not found");
			throw e;
		}

		tree.serialize(strTableName, strIndexName );

	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName,
								Hashtable<String,Object>  htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

		Table target = Table.deserialize("src/main/resources/tables/"+strTableName+"/"+strTableName+".class");
		Tuple newTuple = new Tuple(htblColNameValue);
		target.insert(newTuple);
		System.out.println(Page.deserialize(target.tablePages.get(0)));
	}


	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName,
							String strClusteringKeyValue,
							Hashtable<String,Object> htblColNameValue   )  throws DBAppException{

		throw new DBAppException("not implemented yet");
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName,
								Hashtable<String,Object> htblColNameValue) throws DBAppException{

		throw new DBAppException("not implemented yet");
	}


	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
									String[]  strarrOperators) throws DBAppException{

		return null;
	}


	public static void main( String[] args ){

		try{

			String strTableName = "Student";
			DBApp	dbApp = new DBApp( );
//
			Hashtable htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			dbApp.createTable( strTableName, "id", htblColNameType );

			String strTableName2 = "Girl";

			Hashtable htblColNameType2 = new Hashtable<>();
			htblColNameType2.put("id", "java.lang.Integer");
			htblColNameType2.put("name", "java.lang.String");
			htblColNameType2.put("gpa", "java.lang.Integer");
			dbApp.createTable( strTableName2, "gpa", htblColNameType2 );

			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

			BPTree b = BPTree.deserialize("Student", "gpa");

//			dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//			Hashtable htblColNameValue = new Hashtable( );
//			htblColNameValue.put("id", new Integer( 0 ));
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
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}


}