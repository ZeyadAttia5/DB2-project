/**
 * @author Wael Abouelsaadat
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
            if (csvConverter.tablePresent(strTableName))
                throw new DBAppException("This table is already present.");

            // Traverse column data to check validity of data
            HashSet<String> availableColumns = new HashSet<>();
            for (String key : htblColNameType.keySet()) {
                String dataType = htblColNameType.get(key);
                // Invalid data type for any of the columns
                if (!dataType.equalsIgnoreCase("java.lang.string") && !dataType.equalsIgnoreCase("java.lang.double") && !dataType.equalsIgnoreCase("java.lang.integer"))
                    throw new DBAppException("Invalid data type for " + key + " please enter integer, string or double");
                availableColumns.add(key);
            }

            // Attempting to create a clustering key on a column that doesn't exist
            if (!availableColumns.contains(strClusteringKeyColumn))
                throw new DBAppException("Cannot create a clustering key on a column that doesn't exist. Try creating the table again with valid inputs.");

            // Create the metaData.csv file using the hashtable input and store it in the metaData package
            csvConverter.convert(htblColNameType, strTableName, strClusteringKeyColumn);

            // Initialize a new table object
            Table newTable = new Table(strTableName);
            newTable.serialize();

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

        // Inserting into a table that doesn't exist
        if (!csvConverter.tablePresent(strTableName))
            throw new DBAppException("Cannot insert into a table that doesn't exist.");

        String clusteringKey = csvConverter.getClusteringKey(strTableName);
        if(!htblColNameValue.containsKey(clusteringKey))
            throw new DBAppException("You must insert a value for the primary key: " + clusteringKey + ".");

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
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        throw new DBAppException("not implemented yet");
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
            String operator = sqlTerm._strOperator;
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

        DBApp dbApp = new DBApp();
        dbApp.init();


        String strTableName = "Student";

        // Table Creation
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("numCourses", "java.lang.Integer");
        dbApp.createTable(strTableName, "id", htblColNameType);

//        Hashtable htblColNameValue = new Hashtable();
//
//        // inserting 78452, zaky noor, 0.88
//        htblColNameValue.put("id", Integer.valueOf(1));
//        htblColNameValue.put("name", "Amir Eid");
//        htblColNameValue.put("gpa", new Double(0.7));
//        htblColNameValue.put("numCourses", Integer.valueOf(300050));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//        // inserting: 5674567, dalia noor, 1.25
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(2));
//        htblColNameValue.put("name", "Dalia Noor");
//        htblColNameValue.put("gpa", new Double(1.25));
//        htblColNameValue.put("numCourses", Integer.valueOf(60));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
////
////
//        // inserting 23498, john noor, 1.5
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(3));
//        htblColNameValue.put("name", "John Noor");
//        htblColNameValue.put("gpa", new Double(1.5));
//        htblColNameValue.put("numCourses", Integer.valueOf(90));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
////
//        // inserting 78452, zaky noor, 0.88
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(4));
//        htblColNameValue.put("name", "Zaky Noor");
//        htblColNameValue.put("gpa", new Double(0.88));
//        htblColNameValue.put("numCourses", Integer.valueOf(300));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        // inserting 78452, zaky noor, 0.88
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(5));
//        htblColNameValue.put("name", "Zaky Noor");
//        htblColNameValue.put("gpa", new Double(0.88));
//        htblColNameValue.put("numCourses", Integer.valueOf(300));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//        // inserting 78452, zaky noor, 0.88
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(6));
//        htblColNameValue.put("name", "Ahmed Zaky");
//        htblColNameValue.put("gpa", new Double(0.88));
//        htblColNameValue.put("numCourses", Integer.valueOf(300));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//        // inserting 78452, zaky noor, 0.88
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(7));
//        htblColNameValue.put("name", "Fathy Sroor");
//        htblColNameValue.put("gpa", new Double(0.88));
//        htblColNameValue.put("numCourses", Integer.valueOf(300));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        // inserting: 25, ahmed noor, 0.95
//        htblColNameValue.clear();
//        htblColNameValue.put("id", Integer.valueOf(25));
//        htblColNameValue.put("name", "Ahmed Noor");
//        htblColNameValue.put("gpa", new Double(0.95));
//        htblColNameValue.put("numCourses", Integer.valueOf(50));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);

//			 Attempting to re-create the same table -> should throw an exception yay
//			Hashtable htblColNameType = new Hashtable();
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.double");
//            dbApp.createTable(strTableName, "id", htblColNameType);

//			 Attempting to insert a tuple with the same clustering key -> should throw an exception yay
//			htblColNameValue.clear( );
//            htblColNameValue.put("id", Integer.valueOf(25));
//            htblColNameValue.put("name", "Ahmed Noor");
//            htblColNameValue.put("gpa", new Double(0.95));
//            dbApp.insertIntoTable(strTableName, htblColNameValue);


//        Table currentTable = Table.deserialize("Student");
//        System.out.println("Before: " + currentTable);

//      // Tests calling updateTable on a Double, String Col
//        Hashtable<String, Object> ht = new Hashtable<>();
//        ht.put("name", "Zeyaddd");
//        ht.put("gpa", 0.8);
//        dbApp.updateTable(strTableName, "25", ht);

        // Tests calling updateTable on a Double, Integer, String Cols
//        Hashtable<String, Object> ht = new Hashtable<>();
//        ht.put("name", "Amir Eidd");
//        ht.put("gpa", 0.8);
//        ht.put("numCourses", 1300);
//        dbApp.updateTable(strTableName, "1", ht);
//
//        // Tests calling updateTable on a String Col
//        ht.clear();
//        ht.put("name", "Zeyaddd");
//        dbApp.updateTable(strTableName, "2", ht);
//
//        // Tests calling updateTable on a DOUBLE Col
//        ht.clear();
//        ht.put("gpa", 2.8);
//        dbApp.updateTable(strTableName, "4", ht);
//
//        // Tests calling updateTable on many pages
//        ht.clear();
//        ht.put("numCourses", 2);
//        dbApp.updateTable(strTableName, "5", ht);
//
//        // Tests calling updateTable on an empty htbl
//        ht.clear();
//        dbApp.updateTable(strTableName, "7", ht);
//        ht.put("numCourses", 2);
//        dbApp.updateTable(strTableName, "7", ht);
//
//        System.out.println("After: " + currentTable);


//			System.out.println("After Update: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(0)));

//
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

    }


}
