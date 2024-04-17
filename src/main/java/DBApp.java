/**
 * @author Wael Abouelsaadat
 */

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;


public class DBApp {


    public DBApp() {

    }


    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init() {
        csvConverter.createMetaDataFile();

    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {

		// Check if the table already exists
		if(csvConverter.tablePresent(strTableName))
			throw new DBAppException("This page is already present.");
        try {
            // Initialize a new table object
            Table newTable = new Table(strTableName);
            newTable.serialize();
            // Create the metaData.csv file using the hashtable input and store it in the metaData package
            csvConverter.convert(htblColNameType, strTableName, strClusteringKeyColumn);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // following method creates a B+tree index
    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, IOException, ClassNotFoundException {

        // Adjusting metadata file with new index
        boolean found = csvConverter.addIndexToCSV(strTableName, strColName, strIndexName);
        if (!found) {
            DBAppException e = new DBAppException("The index name " + strIndexName + " already exists in " + strTableName);
            throw e;
        }

        // Retrieving data type for desired column
        String tmp = csvConverter.getDataType(strTableName, strColName);

        // Initialising b+tree
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

		// Populating tree if table is not empty
		Table currentTable = Table.deserialize(strTableName);
		for(String page: currentTable.tablePages){
			Page currentPage = Page.deserialize(page);
			for(int i =0; i< currentPage.getTuples().size(); i++){
				Tuple currentTuple = currentPage.getTuples().get(i);
				Object value = currentTuple.getValues().get(strColName);
				tree.insert((Comparable) value, new Ref(page, i));
			}
		}

        tree.serialize(strTableName, strIndexName);

    }

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Table target = Table.deserialize(strTableName);
        Tuple newTuple = new Tuple(htblColNameValue);
        target.insert(newTuple);
        System.out.println(Page.deserialize(target.tablePages.get(0)));
        target = Table.deserialize(strTableName);
        for(String pageName : target.tablePages)
        {
            System.out.println("Page name: " + pageName);
            System.out.println("The page: " + Page.deserialize(pageName));
        }
        System.out.println(target.tablePages);
        System.out.println();

    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        // Get the metadata of the table
        Table currTable = Table.deserialize(strTableName);
        // Check if the table exists
        if (currTable == null) {
            throw new DBAppException("Table not found: " + strTableName);
        }
        try {
            for (String column : htblColNameValue.keySet()) {
                String indexName = csvConverter.getIndexName(strTableName, column);
                if (!indexName.equalsIgnoreCase("null")) {
                    currTable.updateIndexedTable(column, strClusteringKeyValue, htblColNameValue);
                } else {
                    currTable.updateTable(strClusteringKeyValue, htblColNameValue);
                }
            }

        } catch (IOException e) {
            System.out.println("Table not found\n" + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("Table not found\n" + e.getMessage());
        }

        //save changes
        currTable.serialize();
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
    }

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[]  strarrOperators) throws DBAppException {
		if (arrSQLTerms.length == 0)
			throw new DBAppException("No SQL terms");
		if (strarrOperators != null && strarrOperators.length + 1 != arrSQLTerms.length)
			throw new DBAppException("number of operators doesn't match the SQL terms count");
		ArrayList<ArrayList<Tuple>> res = new ArrayList<>();
		int j = 0;
		Iterator finalRes = null;
		for (SQLTerm sqlTerm : arrSQLTerms) {
			String columnName = sqlTerm._strColumnName;
			Object value = sqlTerm._objValue;
			String tableName = sqlTerm._strTableName;
			String operator = sqlTerm._strOperator.toUpperCase();
			if(!operator.equals("=") || !operator.equals(">") || !operator.equals("<") ||operator!="!="||operator!=">="||operator!="<="){
				throw new DBAppException("Invalid operator");
			}
			try {//ehtmal n serialize kol haga tany b3d el deserializing
				Table tableitself = Table.deserialize(tableName);
				if (csvConverter.getIndexName(tableName, columnName)!=null && operator!="!=") {
					ArrayList<Tuple> helper=new ArrayList<>();
					BPTree ind = BPTree.deserialize(tableName, columnName);
					ArrayList<Ref> references = new ArrayList<>();
					int indexInPage=0;
					String pagename="";
					Page pagenow;
					switch (operator){
						case "=":
							ind.search((Comparable) value);
							pagename = references.get(0).getPage();
							pagenow = Page.deserialize(tableName + "/" + pagename + ".class");
							indexInPage=references.get(0).getIndexInPage();
							helper.add(pagenow.tuples.get(indexInPage));
							res.add(helper);
							break;
						case ">=":
							ind.searchGreaterEqual((Comparable) value);
							for(int k=0;k<references.size();k++){
								pagename = references.get(k).getPage();
								pagenow = Page.deserialize(tableName + "/" + pagename + ".class");
								indexInPage=references.get(k).getIndexInPage();
								helper.add(pagenow.tuples.get(indexInPage));
							}
							res.add(helper);
							break;
						case ">":
							ind.searchGreaterthan((Comparable) value);
							for(int k=0;k<references.size();k++){
								pagename = references.get(k).getPage();
								pagenow = Page.deserialize(tableName + "/" + pagename + ".class");
								indexInPage=references.get(k).getIndexInPage();
								helper.add(pagenow.tuples.get(indexInPage));
							}
							res.add(helper);
							break;
						case "<=":
							for(int k=0;k<=tableitself.tablePages.size();k++){
								pagename=tableitself.tablePages.get(k);
								pagenow=Page.deserialize(tableName + "/" + pagename + ".class");
								for(int p=0;p<pagenow.tuples.size();p++){
									helper.add(pagenow.tuples.get(p));
								}
							}
							ind.searchGreaterEqual((Comparable) value);
							for(int k=0;k<references.size();k++){
								pagename = references.get(k).getPage();
								pagenow = Page.deserialize(tableName + "/" + pagename + ".class");
								indexInPage=references.get(k).getIndexInPage();
								helper.remove(pagenow.tuples.get(indexInPage));
							}
							res.add(helper);
							break;
						case "<":
							for(int k=0;k<=tableitself.tablePages.size();k++){
								pagename=tableitself.tablePages.get(k);
								pagenow=Page.deserialize(tableName + "/" + pagename + ".class");
								for(int p=0;p<pagenow.tuples.size();p++){
									helper.add(pagenow.tuples.get(p));
								}
							}
							ind.searchGreaterthan((Comparable) value);
							for(int k=0;k<references.size();k++){
								pagename = references.get(k).getPage();
								pagenow = Page.deserialize(tableName + "/" + pagename + ".class");
								indexInPage=references.get(k).getIndexInPage();
								helper.remove(pagenow.tuples.get(indexInPage));
							}
							res.add(helper);
							break;
					}
				}
				else{
				res.add(tableitself.searchTable(columnName, operator, value));}
				tableitself.serialize();//might remove depending on deserialize
			} catch (Exception e) {
				throw new DBAppException("Table " + tableName + "not found.");
			}
			if (res.size() > 1) {
				ArrayList<Tuple> l1 = (ArrayList<Tuple>) res.remove(0);//check valid datatype
				ArrayList<Tuple> l2 = (ArrayList<Tuple>) res.remove(0);
				ArrayList<Tuple> anding = new ArrayList<>(l1);
				anding.retainAll(l2);
				ArrayList<Tuple> oring = new ArrayList<>(l1);
				oring.addAll(l2);
				switch (strarrOperators[j].toUpperCase()) {
					case "AND":
						res.add(anding);
						break;
					case "OR":
						res.add(oring);
						break;
					case "XOR":
						oring.retainAll(anding);
						res.add(oring);
						break;
					default: throw new DBAppException("inavalid operator only and, or , xor are allowed");
				}
				j++;
			}
		}
		return res.remove(0).iterator();
	}


    public static void main(String[] args) {

        try {

            DBApp dbApp = new DBApp();
            dbApp.init();


			String strTableName = "Student";

//			// Table Creation
//            Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.double");
//            dbApp.createTable(strTableName, "id", htblColNameType);
//
//
//			// inserting: 25, ahmed noor, 0.95
//            Hashtable htblColNameValue = new Hashtable();
//            htblColNameValue.put("id", Integer.valueOf(25));
//            htblColNameValue.put("name", "Ahmed Noor");
//            htblColNameValue.put("gpa", new Double(0.95));
//            dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//			// inserting: 453455, ahmed noor, 0.95
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 453455 ));
//			htblColNameValue.put("name", new String("Ahmed Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.95 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			// inserting: 5674567, dalia noor, 1.25
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 5674567 ));
//			htblColNameValue.put("name", new String("Dalia Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.25 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//			// inserting 23498, john noor, 1.5
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 23498 ));
//			htblColNameValue.put("name", new String("John Noor" ) );
//			htblColNameValue.put("gpa", new Double( 1.5 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			// inserting 78452, zaky noor, 0.88
//			htblColNameValue.clear( );
//			htblColNameValue.put("id", new Integer( 78452 ));
//			htblColNameValue.put("name", new String("Zaky Noor" ) );
//			htblColNameValue.put("gpa", new Double( 0.88 ) );
//			dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//			dbApp.createIndex(strTableName, "name", "nameIndex");

//			 Attempting to re-create the same table -> should throw an exception yay
//			Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.double");
//            dbApp.createTable(strTableName, "id", htblColNameType);

			// Attempting to insert a tuple with the same clustering key
//			Hashtable htblColNameValue = new Hashtable();
//            htblColNameValue.put("id", Integer.valueOf(25));
//            htblColNameValue.put("name", "Ahmed Noor");
//            htblColNameValue.put("gpa", new Double(0.95));
//            dbApp.insertIntoTable(strTableName, htblColNameValue);


			Table currentTable = Table.deserialize("Student");
			System.out.println(currentTable);
//			Hashtable<String, Object> ht = new Hashtable<>();
//			ht.put("name", "Zeyaddd");
//			ht.put("gpa", 0.8);
//			dbApp.updateTable(strTableName, "0", ht);
//
//			System.out.println("After Update: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(0)));
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



}
