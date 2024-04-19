/**
 * @author Wael Abouelsaadat
 */

import java.io.IOException;
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
        if (csvConverter.tablePresent(strTableName)) throw new DBAppException("This page is already present.");
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
        csvConverter.addIndexToMetadata(strTableName, strColName, strIndexName);

        // Retrieving data type for desired column
        String dataType = csvConverter.getDataType(strTableName, strColName);

        // Initialising b+tree
        BPTree tree = null;
        if (dataType.equalsIgnoreCase("java.lang.Integer")) {
            tree = new BPTree<Integer>(3);
        } else if (dataType.equalsIgnoreCase("java.lang.String")) {
            tree = new BPTree<String>(3);
        } else  {
            tree = new BPTree<Double>(3);
        }

        // Populating tree if table is not empty
        Table currentTable = Table.deserialize(strTableName);
        for (String page : currentTable.tablePages) {
            Page currentPage = Page.deserialize(page);
            for (int i = 0; i < currentPage.getTuples().size(); i++) {
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
        for (String pageName : target.tablePages) {
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
                Hashtable ht = new Hashtable<String, Object>();
                ht.put(column, htblColNameValue.get(column));

                if (!indexName.equalsIgnoreCase("null")) {
                    currTable.updateIndexedTable(column, strClusteringKeyValue, ht);
                } else {

                    currTable.updateTable(strClusteringKeyValue, ht);
                }
            }

        } catch (IOException e) {
            throw new DBAppException("Table not found\n" + e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Table not found\n" + e.getMessage());
        }
        //save changes
        currTable.serialize();
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException {
        Table table = Table.deserialize( strTableName);
        if (table != null) {
            table.delete(htblColNameValue);
            table.serialize();
        } else {
            throw new DBAppException("table " + strTableName + " does not exist");
        }
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

        if (arrSQLTerms.length == 0) throw new DBAppException("No SQL terms");

        if (strarrOperators != null && strarrOperators.length + 1 != arrSQLTerms.length)
            throw new DBAppException("Number of operators doesn't match the SQL terms count");

        ArrayList<ArrayList<Tuple>> res = new ArrayList<>();
        int j = 0;
        String prev=null;
        for (SQLTerm sqlTerm : arrSQLTerms) {

            String columnName = sqlTerm._strColumnName;
            Object value = sqlTerm._objValue;
            String tableName = sqlTerm._strTableName;
            String operator = sqlTerm._strOperator.toUpperCase();
            String columnType = csvConverter.getColumnType(tableName, columnName);
            if (columnType == null) {
                throw new DBAppException("Column " + columnName + " not found");
            }
            // If operator is invalid
            if (!sqlTerm.validOperator()) {
                throw new DBAppException("Invalid operator");
            }
            try {
                Table tableitself = Table.deserialize(tableName);

                // If column has an index and operator is not "!="
                if (!csvConverter.getIndexName(tableName, columnName).equals("null") && !operator.equals("!=")) {

                    ArrayList<Tuple> helper = new ArrayList<>();
                    BPTree ind = BPTree.deserialize(tableName, columnName);
                    ArrayList<Ref> references = new ArrayList<>();
                    int indexInPage = 0;
                    String pagename = "";
                    Page pagenow;
                    switch (operator) {
                        case "=":
                            references = ind.search((Comparable) value);
                            for (int i = 0; i < references.size(); i++) {
                            pagename = references.get(0).getPage();
                            pagenow = Page.deserialize(pagename);
                            indexInPage = references.get(i).getIndexInPage();
                            helper.add(pagenow.tuples.get(indexInPage));
                            }
                            res.add(helper);
                            break;
                        case ">=":
                            references = ind.getRefsGreaterEqual((Comparable) value);
                            for (int k = 0; k < references.size(); k++) {
                                pagename = references.get(k).getPage();
                                pagenow = Page.deserialize(pagename);
                                indexInPage = references.get(k).getIndexInPage();
                                helper.add(pagenow.tuples.get(indexInPage));
                            }
                            res.add(helper);
                            break;
                        case ">":
                            references = ind.getRefsGreaterThan((Comparable) value);
                            for (int k = 0; k < references.size(); k++) {
                                pagename = references.get(k).getPage();
                                pagenow = Page.deserialize(pagename);
                                indexInPage = references.get(k).getIndexInPage();
                                helper.add(pagenow.tuples.get(indexInPage));
                            }
                            res.add(helper);
                            break;
                        case "<=":
                            references = ind.getRefsLessEqual((Comparable) value);
                            for (int k = 0; k < references.size(); k++) {
                                pagename = references.get(k).getPage();
                                pagenow = Page.deserialize(pagename);
                                indexInPage = references.get(k).getIndexInPage();
                                helper.add(pagenow.tuples.get(indexInPage));
                            }
                            res.add(helper);
                            break;
                        case "<":
                            references = ind.getRefsLessThan((Comparable) value);
                            for (int k = 0; k < references.size(); k++) {
                                pagename = references.get(k).getPage();
                                pagenow = Page.deserialize(pagename);
                                indexInPage = references.get(k).getIndexInPage();
                                helper.add(pagenow.tuples.get(indexInPage));
                            }
                            res.add(helper);
                            break;
                    }
                } else {
                    res.add(tableitself.searchTable(columnName, operator, value));
                }

            } catch (Exception e) {
                throw new DBAppException("Table " + tableName + " not found.");
            }
            if (res.size() > 1) {
                if(prev!=null&&!prev.equals(tableName)){
                    throw new DBAppException("Engine is not supporting joins");
                }
                prev =tableName;
                ArrayList<Tuple> l1 = res.remove(0);
                ArrayList<Tuple> l2 = res.remove(0);
                ArrayList<Tuple> midres = new ArrayList<>();
                switch (strarrOperators[j].toUpperCase()) {
                    case "AND":
                        for (Tuple tuple: l1) {
                            for(Tuple tuple2 :l2){
                                if(tuple2.getValues().equals(tuple.getValues())){
                                    midres.add(tuple);
                                }
                            }
                        }
                        res.add(midres);
                        break;
                    case "OR":
                        for (Tuple tuple: l1) {
                            midres.add(tuple);
                        }
                        int mid=midres.size();
                        for(Tuple t2 :l2) {
                            boolean flag = false;
                            for(int i=0;i<mid;i++){
                                if (midres.get(i).getValues().equals(t2.getValues())){
                                    flag=true;
                                }
                            }
                            if(!flag){
                                midres.add(t2);
                            }
                        }
                        res.add(midres);
                        break;
                    case "XOR":
                        for(Tuple t1:l1){
                            boolean flag=false;
                            for(Tuple t2:l2){
                                if(t1.getValues().equals(t2.getValues())){
                                    flag=true;
                                    break;
                                }
                            }
                            if(!flag){
                                midres.add(t1);
                            }
                        }
                        for(Tuple t1:l2){
                            boolean flag=false;
                            for(Tuple t2:l1){
                                if(t1.getValues().equals(t2.getValues())){
                                    flag=true;
                                    break;
                                }
                            }
                            if(!flag){
                                midres.add(t1);
                            }
                        }
                        res.add(midres);
                        break;
                    default:
                        throw new DBAppException("inavalid operator only and, or , xor are allowed");
                }
                j++;
            }
        }
        return res.remove(0).iterator();
    }

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {

        try {

            String strTableName = "Student";
            DBApp dbApp = new DBApp();
            dbApp.init();

//
//          	Hashtable htblColNameType = new Hashtable();
//          	htblColNameType.put("id", "java.lang.Integer");
//          	htblColNameType.put("name", "java.lang.String");
//			htblColNameType.put("age", "java.lang.Integer");
//			htblColNameType.put("gpa", "java.lang.double");
//			htblColNameType.put("city", "java.lang.String");
//			htblColNameType.put("uni", "java.lang.String");
//			htblColNameType.put("birth", "java.lang.Integer");
//
//
//			dbApp.createTable(strTableName, "id", htblColNameType);
//
//          	dbApp.createIndex(strTableName, "gpa", "gpaIndex");
//          	dbApp.createIndex(strTableName,"id","idIndex");
//          	dbApp.createIndex(strTableName,"name","nameIndex");
//          	dbApp.createIndex(strTableName,"birth","birthIndex");
//
//          	Hashtable htblColNameValue1 = new Hashtable();
//          	htblColNameValue1.put("id", Integer.valueOf(1));
//          	htblColNameValue1.put("name", "Jana");
//			htblColNameValue1.put("age", 21);
//			htblColNameValue1.put("gpa", 0.7);
//			htblColNameValue1.put("city", "Cairo");
//			htblColNameValue1.put("uni", "GUC");
//			htblColNameValue1.put("birth", 5);
//
//			Hashtable htblColNameValue2 = new Hashtable();
//			htblColNameValue2.put("id", Integer.valueOf(2));htblColNameValue2.put("name", "Nabila");
//			htblColNameValue2.put("age", 21);
//			htblColNameValue2.put("gpa", 0.7);
//			htblColNameValue2.put("city", "Cairo");
//			htblColNameValue2.put("uni", "GUC");
//			htblColNameValue2.put("birth", 6);
//
//			Hashtable htblColNameValue3 = new Hashtable();
//			htblColNameValue3.put("id", Integer.valueOf(3));
//			htblColNameValue3.put("name", "Amr");
//			htblColNameValue3.put("age", 21);
//			htblColNameValue3.put("gpa", 0.7);
//			htblColNameValue3.put("city", "Cairo");
//			htblColNameValue3.put("uni", "GUC");
//			htblColNameValue3.put("birth", 6);
//
//			Hashtable htblColNameValue4 = new Hashtable();
//			htblColNameValue4.put("id", Integer.valueOf(4));
//			htblColNameValue4.put("name", "Laila");
//			htblColNameValue4.put("age", 10);
//			htblColNameValue4.put("gpa", 1.0);
//			htblColNameValue4.put("city", "Alex");
//			htblColNameValue4.put("uni", "ELS");
//			htblColNameValue4.put("birth", 11);
//
//			Hashtable htblColNameValue5 = new Hashtable();
//			htblColNameValue5.put("id", Integer.valueOf(5));
//			htblColNameValue5.put("name", "Maya");
//			htblColNameValue5.put("age", 20);
//			htblColNameValue5.put("gpa", 1.2);
//			htblColNameValue5.put("city", "Mans");
//			htblColNameValue5.put("uni", "GUC");
//			htblColNameValue5.put("birth", 6);
//
//			Hashtable htblColNameValue6 = new Hashtable();
//			htblColNameValue6.put("id", Integer.valueOf(6));
//			htblColNameValue6.put("name", "Gamila");
//			htblColNameValue6.put("age", 19);
//			htblColNameValue6.put("gpa", 1.5);
//			htblColNameValue6.put("city", "Mans");
//			htblColNameValue6.put("uni", "GUC");
//			htblColNameValue6.put("birth", 8);
//
//			Hashtable htblColNameValue7 = new Hashtable();
//			htblColNameValue7.put("id", Integer.valueOf(7));
//			htblColNameValue7.put("name", "Alia");
//			htblColNameValue7.put("age", 15);
//			htblColNameValue7.put("gpa", 2.2);
//			htblColNameValue7.put("city", "Cairo");
//			htblColNameValue7.put("uni", "BIS");
//			htblColNameValue7.put("birth", 9);
//
//			Hashtable htblColNameValue8 = new Hashtable();
//			htblColNameValue8.put("id", Integer.valueOf(8));
//			htblColNameValue8.put("name", "Farida");
//			htblColNameValue8.put("age", 15);
//			htblColNameValue8.put("gpa", 3.3);
//			htblColNameValue8.put("city", "Alex");
//			htblColNameValue8.put("uni", "BIS");
//			htblColNameValue8.put("birth", 9);
//
//			Hashtable htblColNameValue9 = new Hashtable();
//			htblColNameValue9.put("id", Integer.valueOf(9));
//			htblColNameValue9.put("name", "Ahmed");
//			htblColNameValue9.put("age", 23);
//			htblColNameValue9.put("gpa", 1.1);
//			htblColNameValue9.put("city", "Alex");
//			htblColNameValue9.put("uni", "SU");
//			htblColNameValue9.put("birth", 11);
//
//			Hashtable htblColNameValue10 = new Hashtable();
//			htblColNameValue10.put("id", Integer.valueOf(10));
//			htblColNameValue10.put("name", "Ali");
//			htblColNameValue10.put("age", 16);
//			htblColNameValue10.put("gpa", 2.2);
//			htblColNameValue10.put("city", "Cairo");
//			htblColNameValue10.put("uni", "ES");
//			htblColNameValue10.put("birth", 5);


            // Table Creation
            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");
            htblColNameType.put("numCourses", "java.lang.Integer");
            dbApp.createTable(strTableName, "id", htblColNameType);
//
//			Hashtable htblColNameValue11 = new Hashtable();
//			htblColNameValue11.put("id", Integer.valueOf(11));
//			htblColNameValue11.put("name", "Ibraheem");
//			htblColNameValue11.put("age", 5);
//			htblColNameValue11.put("gpa", 4.4);
//			htblColNameValue11.put("city", "Mans");
//			htblColNameValue11.put("uni", "ES");
//			htblColNameValue11.put("birth", 5);
//
//			Hashtable htblColNameValue12 = new Hashtable();
//			htblColNameValue12.put("id", Integer.valueOf(12));
//			htblColNameValue12.put("name", "Bika");
//			htblColNameValue12.put("age", 2);
//			htblColNameValue12.put("gpa", 0.1);
//			htblColNameValue12.put("city", "Cats");
//			htblColNameValue12.put("uni", "Kitten");
//			htblColNameValue12.put("birth", 11);
//
//			dbApp.insertIntoTable(strTableName, htblColNameValue1);
//          dbApp.insertIntoTable(strTableName, htblColNameValue2);
//			dbApp.insertIntoTable(strTableName, htblColNameValue3);
//			dbApp.insertIntoTable(strTableName, htblColNameValue4);
//			dbApp.insertIntoTable(strTableName, htblColNameValue5);
//			dbApp.insertIntoTable(strTableName, htblColNameValue6);
//			dbApp.insertIntoTable(strTableName, htblColNameValue7);
//			dbApp.insertIntoTable(strTableName, htblColNameValue8);
//			dbApp.insertIntoTable(strTableName, htblColNameValue9);
//			dbApp.insertIntoTable(strTableName, htblColNameValue10);
//			dbApp.insertIntoTable(strTableName, htblColNameValue11);
//			dbApp.insertIntoTable(strTableName, htblColNameValue12);

            Hashtable<String, Object> ht = new Hashtable<>();
//            ht.put("id", 7);
            ht.put("name", "Ahmed");
            ht.put("age", 23);
//			  ht.put("gpa",2.2);

//			ht.put("city","Cairo");
//			ht.put("uni","GUC");
//			ht.put("birth",8);

            dbApp.deleteFromTable(strTableName, ht);
            System.out.println("After Deletion: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(0)));

//          dbApp.updateTable(strTableName, "3", ht);
//
//			htblColNameValue.clear( );
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[1];
//			arrSQLTerms[0] = new SQLTerm("Student", "name", "<=", "Dalia Noor");
////			arrSQLTerms[1] = new SQLTerm();
////			arrSQLTerms[1]._strTableName =  "Student";
////			arrSQLTerms[1]._strColumnName=  "gpa";
////			arrSQLTerms[1]._strOperator  =  "=";
////			arrSQLTerms[1]._objValue     =  new Double( 7 );
//
//			String[]strarrOperators = new String[0];
////			strarrOperators[0] = "OR";
//			// select * from Student where name = "John Noor" or gpa = 1.5;
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//			while(resultSet.hasNext())
//				System.out.println(resultSet.next());


//        // Table doesn't exist
//        dbApp.createIndex("fake table", "gpa", "gpaIndex");
//        // Column doesn't exist
//        dbApp.createIndex(strTableName, "fake col", "gpaIndex");
//        // nameIndex already exists
//        dbApp.createIndex(strTableName, "name", "nameIndex");
//        dbApp.createIndex(strTableName, "gpa", "nameIndex");
//        // name already has an index
//        dbApp.createIndex(strTableName, "name", "nameIndex");
//        dbApp.createIndex(strTableName, "name", "otherName");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
