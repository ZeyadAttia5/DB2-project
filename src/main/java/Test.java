import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class Test {

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();
        dbApp.init();

//        Table t = Table.deserialize("Student");
//        System.out.println(t);
//        System.out.println();
//
//        BPTree tree = BPTree.deserialize(strTableName,"gpa");
//        BPTreeLeafNode currLeaf = tree.searchMinNode();
//
//        while(currLeaf!=null ){
//            for(int i = 0;i< currLeaf.numberOfKeys; i++){
//                System.out.println(currLeaf);
//                System.out.println("Page: " + currLeaf.records[i].getPage()+ ". Index: " + currLeaf.records[i].getIndexInPage());
//                if (currLeaf.getOverflow(i)!= null && currLeaf.getOverflow(i).size()>0 ) {
//                    int size = currLeaf.getOverflow(i).size();
//                    // Traverse the duplicates
//                    for(int j =0; j< size; j++){
//                        int currentIndex = ((Ref)currLeaf.getOverflow(i).get(j)).getIndexInPage();
//                        String currentPage =  ((Ref)currLeaf.getOverflow(i).get(j)).getPage();
//                        System.out.println("Page: "+ currentPage + ". Index: " + currentIndex);
//                    }
//                }
//            }
//            currLeaf = currLeaf.getNext();
//        }

//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("age", "java.lang.Integer");
//        htblColNameType.put("gpa", "java.lang.double");
//        htblColNameType.put("city", "java.lang.String");
//        htblColNameType.put("uni", "java.lang.String");
//        htblColNameType.put("birth", "java.lang.Integer");
//
//
//        dbApp.createTable(strTableName, "id", htblColNameType);
//
//        dbApp.createIndex(strTableName, "gpa", "gpaIndex");
//        dbApp.createIndex(strTableName,"id","idIndex");
//        dbApp.createIndex(strTableName,"name","nameIndex");
//        dbApp.createIndex(strTableName,"birth","birthIndex");

        Hashtable htblColNameValue1 = new Hashtable();
        htblColNameValue1.put("id", Integer.valueOf(1));
        htblColNameValue1.put("name", "Jana");
        htblColNameValue1.put("age", 21);
        htblColNameValue1.put("gpa", 0.7);
        htblColNameValue1.put("city", "Cairo");
        htblColNameValue1.put("uni", "GUC");
        htblColNameValue1.put("birth", 5);

        Hashtable htblColNameValue2 = new Hashtable();
        htblColNameValue2.put("id", Integer.valueOf(2));htblColNameValue2.put("name", "Nabila");
        htblColNameValue2.put("age", 21);
        htblColNameValue2.put("gpa", 0.7);
        htblColNameValue2.put("city", "Cairo");
        htblColNameValue2.put("uni", "GUC");
        htblColNameValue2.put("birth", 6);

        Hashtable htblColNameValue3 = new Hashtable();
        htblColNameValue3.put("id", Integer.valueOf(3));
        htblColNameValue3.put("name", "Amr");
        htblColNameValue3.put("age", 21);
        htblColNameValue3.put("gpa", 0.7);
        htblColNameValue3.put("city", "Cairo");
        htblColNameValue3.put("uni", "GUC");
        htblColNameValue3.put("birth", 6);

        Hashtable htblColNameValue4 = new Hashtable();
        htblColNameValue4.put("id", Integer.valueOf(4));
        htblColNameValue4.put("name", "Laila");
        htblColNameValue4.put("age", 10);
        htblColNameValue4.put("gpa", 1.0);
        htblColNameValue4.put("city", "Alex");
        htblColNameValue4.put("uni", "ELS");
        htblColNameValue4.put("birth", 11);

        Hashtable htblColNameValue5 = new Hashtable();
        htblColNameValue5.put("id", Integer.valueOf(5));
        htblColNameValue5.put("name", "Maya");
        htblColNameValue5.put("age", 20);
        htblColNameValue5.put("gpa", 1.2);
        htblColNameValue5.put("city", "Mans");
        htblColNameValue5.put("uni", "GUC");
        htblColNameValue5.put("birth", 6);

        Hashtable htblColNameValue6 = new Hashtable();
        htblColNameValue6.put("id", Integer.valueOf(6));
        htblColNameValue6.put("name", "Gamila");
        htblColNameValue6.put("age", 19);
        htblColNameValue6.put("gpa", 1.5);
        htblColNameValue6.put("city", "Mans");
        htblColNameValue6.put("uni", "GUC");
        htblColNameValue6.put("birth", 8);

        Hashtable htblColNameValue7 = new Hashtable();
        htblColNameValue7.put("id", Integer.valueOf(7));
        htblColNameValue7.put("name", "Alia");
        htblColNameValue7.put("age", 15);
        htblColNameValue7.put("gpa", 2.2);
        htblColNameValue7.put("city", "Cairo");
        htblColNameValue7.put("uni", "BIS");
        htblColNameValue7.put("birth", 9);

        Hashtable htblColNameValue8 = new Hashtable();
        htblColNameValue8.put("id", Integer.valueOf(8));
        htblColNameValue8.put("name", "Farida");
        htblColNameValue8.put("age", 15);
        htblColNameValue8.put("gpa", 3.3);
        htblColNameValue8.put("city", "Alex");
        htblColNameValue8.put("uni", "BIS");
        htblColNameValue8.put("birth", 9);

        Hashtable htblColNameValue9 = new Hashtable();
        htblColNameValue9.put("id", Integer.valueOf(9));
        htblColNameValue9.put("name", "Ahmed");
        htblColNameValue9.put("age", 23);
        htblColNameValue9.put("gpa", 1.1);
        htblColNameValue9.put("city", "Alex");
        htblColNameValue9.put("uni", "SU");
        htblColNameValue9.put("birth", 11);

        Hashtable htblColNameValue10 = new Hashtable();
        htblColNameValue10.put("id", Integer.valueOf(10));
        htblColNameValue10.put("name", "Ali");
        htblColNameValue10.put("age", 16);
        htblColNameValue10.put("gpa", 2.2);
        htblColNameValue10.put("city", "Cairo");
        htblColNameValue10.put("uni", "ES");
        htblColNameValue10.put("birth", 5);

        Hashtable htblColNameValue11 = new Hashtable();
        htblColNameValue11.put("id", Integer.valueOf(11));
        htblColNameValue11.put("name", "Ibraheem");
        htblColNameValue11.put("age", 5);
        htblColNameValue11.put("gpa", 4.4);
        htblColNameValue11.put("city", "Mans");
        htblColNameValue11.put("uni", "ES");
        htblColNameValue11.put("birth", 5);

        Hashtable htblColNameValue12 = new Hashtable();
        htblColNameValue12.put("id", Integer.valueOf(12));
        htblColNameValue12.put("name", "Bika");
        htblColNameValue12.put("age", 2);
        htblColNameValue12.put("gpa", 0.1);
        htblColNameValue12.put("city", "Cats");
        htblColNameValue12.put("uni", "Kitten");
        htblColNameValue12.put("birth", 11);

//        dbApp.insertIntoTable(strTableName, htblColNameValue1);
//        dbApp.insertIntoTable(strTableName, htblColNameValue2);
//        dbApp.insertIntoTable(strTableName, htblColNameValue3);
//        dbApp.insertIntoTable(strTableName, htblColNameValue4);
//        dbApp.insertIntoTable(strTableName, htblColNameValue5);
//        dbApp.insertIntoTable(strTableName, htblColNameValue6);
//        dbApp.insertIntoTable(strTableName, htblColNameValue7);
//        dbApp.insertIntoTable(strTableName, htblColNameValue8);
//        dbApp.insertIntoTable(strTableName, htblColNameValue9);
//        dbApp.insertIntoTable(strTableName, htblColNameValue10);
//        dbApp.insertIntoTable(strTableName, htblColNameValue11);
//        dbApp.insertIntoTable(strTableName, htblColNameValue12);

        Hashtable<String, Object> ht = new Hashtable<>();



        //deleting with clustering only (works)
//            ht.put("id", 5);

        // deleting with clustering + index (works)
//            ht.put("id", 7);
//            ht.put("gpa", 2.2);

        //deleting with clustering + no index (works)
//            ht.put("city", "Alex");
//            ht.put("gpa", 1.1);


        // deleting with clustering + index + no index (works)

//            ht.put("id", 5);
//            ht.put("city", "Cairo");
//            ht.put("gpa", 2.2);


        // deleting with [clustering+btree] only (works)
//            ht.put("id", 16);

        //deleting with index only (works)
//            ht.put("gpa", 9.9);

        // deleting with nonindex only (works)
//            ht.put("city","Mans");

        // deleting with [clustering+btree] + index (works)
//            ht.put("id", 7);
//            ht.put("gpa", 2.2);

        // deleting with index + without index (works)
//            ht.put("city", "Alex");
//            ht.put("gpa", 1.1);

        // deleting with [clustering+btree] + without index (works)
            ht.put("id", 10);
//            ht.put("city", "Cairo");

        // deleting with [clustering+btree] + without index + with index (works)
//            ht.put("id", 7);
//            ht.put("city", "Cairo");
//            ht.put("gpa", 2.2);


//            ht.put("id", 9);
//            ht.put("name", "yahya");
//            ht.put("age", 23);
//            ht.put("gpa",1.1);
//			ht.put("city","Alex");
//			ht.put("uni","GUC");
//			ht.put("birth",8);

        dbApp.deleteFromTable(strTableName, ht);


//          dbApp.updateTable(strTableName, "yahya", ht);
        System.out.println("After Deletion: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(0)));
        System.out.println("After Deletion: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(1)));
//        System.out.println("After Deletion: \n" + Page.deserialize(Table.deserialize(strTableName).tablePages.get(2)));
//
//			htblColNameValue1.clear( );
//			SQLTerm[] arrSQLTerms;
//			arrSQLTerms = new SQLTerm[2];
//			arrSQLTerms[0] = new SQLTerm("Student", "name", "!=", "Gamila");
//			arrSQLTerms[1] = new SQLTerm("Student", "age", ">=", 19);
//			arrSQLTerms[1] = new SQLTerm();
//			arrSQLTerms[1]._strTableName =  "Student";
//			arrSQLTerms[1]._strColumnName=  "gpa";
//			arrSQLTerms[1]._strOperator  =  "=";
//			arrSQLTerms[1]._objValue     =  new Double( 7 );
//
//			String[]strarrOperators = new String[1];
//			strarrOperators[0] = "AND";
//          System.out.println("before select");
//			Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//			while(resultSet.hasNext())
//				System.out.print(resultSet.next());


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
