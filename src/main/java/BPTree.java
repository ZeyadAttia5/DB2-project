import java.io.*;
import java.util.*;

public class BPTree<T extends Comparable<T>> implements Serializable {

    private static final long serialVersionUID = 1L;
    private int order;
    private BPTreeNode<T> root;
//    private String filename;
    private ArrayList<BPTreeNode<T>> nodes = new ArrayList<BPTreeNode<T>>();

    /**
     * Creates an empty B+ tree
     *
     * @param order the maximum number of keys in the nodes of the tree
     */
    public BPTree(int order) {
        this.order = order;
        root = new BPTreeLeafNode<T>(this.order);
        nodes.add(root);
        root.setRoot(true);
        // nodes=this.getNodes();
        // this.createFile(filename);
    }

    public void serialize(String tableName, String indexName) {
        try (FileOutputStream fileOut = new FileOutputStream("src/main/resources/tables/" + tableName +"/" + indexName + ".class");
             ObjectOutputStream objectOut = new ObjectOutputStream(fileOut)) {
            objectOut.writeObject(this);
//            System.out.println("B+ tree serialized successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static BPTree deserialize(String tableName, String colName) {
        String indexName = csvConverter.getIndexName(tableName, colName);
        BPTree bTree = null;
        try (FileInputStream fileIn = new FileInputStream("src/main/resources/tables/" + tableName +"/" + indexName + ".class");
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {
            bTree = (BPTree) objectIn.readObject();
//            System.out.println("B+ tree deserialized successfully.");
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return bTree;
    }

//    public String getFileName() {
//        return filename;
//    }

    public ArrayList getNodes() {
        ArrayList output = new ArrayList();
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).isRoot()) {

            } else {
                output.add(nodes.get(i));
            }

        }

        return output;
    }


    /**
     * Inserts the specified key associated with the given record in the B+ tree
     *
     * @param key             the key to be inserted
     * @param recordReference the reference of the record associated with the key
     */
    public void insert(T key, Ref recordReference) {
        BPTreeLeafNode leaf = this.searchNode(key);
        if (leaf != null) {
            leaf.insertOverflow(key, recordReference);
        } else {
            PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
            if (pushUp != null) {
                BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
                nodes.add(newRoot);
                newRoot.insertLeftAt(0, pushUp.key, root);
                newRoot.setChild(1, pushUp.newNode);
                root.setRoot(false);
                root = newRoot;
                root.setRoot(true);
            }
        }
    }

    /**
     * updates a value in the tree
     */
    public void update(T Oldkey, T newKey, Ref refNew, Ref refOld) {
        delete(Oldkey, refOld);
        insert(newKey, refNew);
    }

    /*
     * updates the references of shifted keys
     */

    public void updateRefNonKey(ArrayList<Ref> oldRefs, ArrayList<Ref> newRefs) {
        BPTreeLeafNode min = this.searchMinNode();
        while (min != null) {

            for (int j = 0; j < min.numberOfKeys; j++) {
                for (int i = 0; i < oldRefs.size(); i++) {
                    // for each leaf we will check if it is equal to one of the old ref,update if it
                    // does and remove
                    // old,new corresponding ref
                    Ref curRef = oldRefs.get(i);
                    Ref newRef = newRefs.get(i);
                    if (min.getRecord(j).isEqual(curRef)) {
                        min.setRecord(j, newRef);
                        oldRefs.remove(i);
                        newRefs.remove(i);
                        break;

                    } else if (min.getOverflow(j) != null && min.overflow.size() > 0) {
                        for (int k = 0; k < min.getOverflow(j).size(); k++) {
                            if (((Ref) min.getOverflow(j).get(k)).isEqual(curRef)) {
                                min.getOverflow(j).remove(k);
                                min.getOverflow(j).add(k, newRef);
                                oldRefs.remove(i);
                                newRefs.remove(i);
                                break;
                            }
                        }
                    }
                }
            }
            min = min.getNext();
        }

    }

    public void updateRef(ArrayList<Ref> refs, int index, BPTreeLeafNode leaf) {
        System.err.println(refs);
        System.err.println("Inex is" + 1);
        // leaf.setRecord(index, refs.get(0));
        int j = 0;
        for (int i = index; i < leaf.numberOfKeys && j < refs.size(); i++) {
            leaf.setRecord(i, refs.get(j));
            j++;
            if (leaf.getOverflow(i) != null && leaf.getOverflow(i).size() > 0) {
                System.err.println("I enter when duplicates of" + leaf.getKey(i));
                int size = leaf.getOverflow(i).size();
                leaf.initializeVectors(i);
                for (int m = 0; m < size && j < refs.size(); m++) {
                    leaf.getOverflow(i).add(m, refs.get(j));
                    j++;

                }
            }
        }
        leaf = leaf.getNext();
        while (leaf != null && j < refs.size()) {
            for (int i = 0; i < leaf.numberOfKeys && j < refs.size(); i++) {
                leaf.setRecord(i, refs.get(j));
                j++;
                if (leaf.getOverflow(i) != null && leaf.getOverflow(i).size() > 0) {
                    System.err.println("I enter when duplicates of" + leaf.getKey(i));
                    int size = leaf.getOverflow(i).size();
                    leaf.initializeVectors(i);
                    for (int m = 0; m < size && j < refs.size(); m++) {
                        leaf.getOverflow(i).add(m, refs.get(j));
                        j++;

                    }
                }
            }
            leaf = leaf.getNext();
        }
    }

    /**
     * Looks up for the record that is associated with th e specified key
     *
     * @param key the key to find its record
     * @return the reference of the record associated with this key
     */
//    public Ref search(T key) {
//        return root.search(key);
//    }

    public ArrayList<Ref>  search(T key) {
        return searchDuplicates(key);
    }

    public ArrayList<Ref> searchDuplicates(T Key) {

        return root.searchFromNodesDuplicates(Key);

    }

    public BPTreeLeafNode searchNode(T key) {
        return root.searchForNode2(key);
    }

    public BPTreeLeafNode searchNodeRef(T key, Ref ref) {
        return root.searchForNodeRef(key, ref, this);
    }

    public BPTreeLeafNode searchMinNode() {
        return root.searchForMin();
    }

    public BPTreeLeafNode searchGreaterthan(T key) {
        return root.searchGreaterthan(key);
    }

    public BPTreeLeafNode searchGreaterEqual(T key) {
        return root.searchGreatereEqual(key);
    }

    public BPTreeLeafNode searchGreaterEqualPoly(T key) {
        return root.searchGreatereEqualPoly(key);
    }

    public BPTreeLeafNode getLastLeaf() {
        BPTreeLeafNode curNode = this.searchMinNode();
        while (curNode.getNext() != null) {
            curNode = curNode.getNext();
        }
        return curNode;
    }

    /**
     * Delete a key and its associated record from the tree.
     *
     * @param key the key to be deleted
     * @return a boolean to indicate whether the key is successfully deleted or it
     *         was not in the tree
     */
//    public Ref insertRef(T key, int maxRows, String tableName, boolean empty) {
//        if (empty) {
//            return new Ref(tableName + "0" + ".class", 0);
//        } else {
//            BPTreeLeafNode b = this.searchGreaterthan(key);
//            if (b == null) {
//                Ref BeforeLast;
//                if (this.getLastLeaf().getOverflow(this.getLastLeaf().numberOfKeys - 1) != null) {
//                    if (this.getLastLeaf().getOverflow(this.getLastLeaf().numberOfKeys - 1).size() == 0) {
//                        BeforeLast = this.getLastLeaf().getLastRecord();
//                    } else {
//                        Vector<Ref> refs = this.getLastLeaf().getOverflow(this.getLastLeaf().numberOfKeys - 1);
//                        BeforeLast = refs.get(refs.size() - 1);
//                    }
//                } else {
//                    BeforeLast = this.getLastLeaf().getLastRecord();
//                }
//                System.err.println("Last ref is" + BeforeLast);
//                String pageNumber = BeforeLast.getPage();
//                String number = pageNumber.substring(tableName.length(), pageNumber.length() - 4);
//                int n = Integer.parseInt(number);
//                int rowsCurrent = BeforeLast.getIndexInPage() + 1;
//                System.err.println("will I enter?");
//                if (rowsCurrent >= maxRows) {
//                    System.err.println("I entered");
//                    int newRow = 0;
//                    n++;
//                    String nn = Integer.toString(n);
//                    String pageNew = tableName + nn + ".class";
//                    return new Ref(pageNew, newRow);
//
//                } else {
//                    return new Ref(pageNumber, rowsCurrent);
//
//                }
//
//            } else {
//                int indexInser = -1;
//                for (int i = 0; i < b.numberOfKeys; i++)
//                    if (b.getKey(i).compareTo(key) > 0) {
//                        indexInser = i;
//                        break;
//                    }
//                return b.getRecord(indexInser);
//            }
//
//        }
//
//    }

    public boolean deleteHelper(T key, Ref ref) {
        if (root.numberOfKeys == 0) {
            if (root instanceof BPTreeInnerNode)
                root = ((BPTreeInnerNode) root).getFirstChild();
        }
        boolean done = root.delete(key, null, -1, ref);
        // go down and find the new root in case the old root is deleted
        while (root instanceof BPTreeInnerNode && !root.isRoot())
            root = ((BPTreeInnerNode<T>) root).getFirstChild();
        return done;

    }

    //delete using references
    public boolean delete(T key, Ref ref) {
        boolean done = false;
        BPTreeLeafNode leaf = this.searchNode(key);
        if (leaf != null && leaf.hasDuplicates(key)) {
            System.out.println("I have duplicates so I entered here");
            done = leaf.deleteOverflow(key, ref);
            System.out.println("No matching tuple so I will look at main ref");

        }

//		if (ref == null) {
//			while (this.searchNode(key) != null) {
//				boolean done2 = false;
//				done = this.deleteL(key, ref);
//				done = done2 || done;
//			}
//		}
        if (ref != null) {
//			boolean done2 = false;
//			done = this.deleteL(key, ref);
//			done = done2 || done;
            done = this.deleteHelper(key, ref);
        }
        if(done )
            System.out.println("I will delete here now and it should succeed");
        return done;

    }

    public void printingRef(BPTreeLeafNode leaf) {
        if (leaf != null) {
            leaf.print();
        } else {
            System.out.println("null");
        }
    }

    public boolean deleteL(T key, Ref ref) {
        if (root.numberOfKeys == 0) {
            if (root instanceof BPTreeInnerNode)
                root = ((BPTreeInnerNode) root).getFirstChild();
        }
        return root.deleteLeft(key, null, -1, ref);
    }

    public boolean deleteAllRefs(ArrayList<Ref> refs, T key) {
        if (refs == null || refs.size() == 0)
            return false;
        for (int i = 0; i < refs.size(); i++) {
            this.delete(key, refs.get(i));
        }
        return false;
    }

    /**
     * Returns a string representation of the B+ tree.
     */
    public String toString() {

        // <For Testing>
        // node : (id)[k1|k2|k3|k4]{P1,P2,P3,}
        String s = "";
        Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
        cur.add(root);
        while (!cur.isEmpty()) {
            next = new LinkedList<BPTreeNode<T>>();
            while (!cur.isEmpty()) {
                BPTreeNode<T> curNode = cur.remove();
                System.out.print(curNode);
                if (curNode instanceof BPTreeLeafNode) {
                    this.printingRef((BPTreeLeafNode) curNode);
                    System.out.print("->");
                } else {
                    System.out.print("{");
                    BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
                    for (int i = 0; i <= parent.numberOfKeys; ++i) {
                        System.out.print(parent.getChild(i).index + ",");
                        next.add(parent.getChild(i));
                    }
                    System.out.print("} ");
                }

            }
            System.out.println();
            cur = next;
        }
        // </For Testing>
        return s;
    }
    public static ArrayList<Ref> intersection(ArrayList<Ref> list1, ArrayList<Ref> list2) {

        if(list1 == null)
            return list2;
        if(list2== null)
            return list1;
        if(list1 == null && list2 == null)
            return new ArrayList<>();

        // Create a HashSet to store unique elements of list1
        HashSet<Ref> set = new HashSet<Ref>(list1);

        // Create a result ArrayList to store the intersection
        ArrayList<Ref> result = new ArrayList<>();

        // Iterate through elements of list2
        for (Ref ref : list2) {
            // If the ref exists in the HashSet or if it's equal to any ref in list1, it's common to both lists
            if (set.contains(ref) || containsEqualRef(list1, ref)) {
                result.add(ref); // Add the common ref to the result list
                set.remove(ref); // Remove the ref from the HashSet to avoid duplicates
            }
        }

        return result; // Return the intersection ArrayList
    }

    // Helper method to check if any ref in the list is equal to the given ref
    private static boolean containsEqualRef(ArrayList<Ref> list, Ref ref) {
        for (Ref r : list) {
            if (r.isEqual(ref)) {
                return true;
            }
        }
        return false;
    }

    public void visualizeTree() {
        if (root == null) {
            System.out.println("The tree is empty.");
            return;
        }

        visualizeTreeHelper(root, "", true);
    }

    private void visualizeTreeHelper(BPTreeNode<T> node, String prefix, boolean isTail) {
        if (node == null)
            return;

        // Print keys in the node
        System.out.println(prefix + (isTail ? "└── " : "├── ") + node);

        // Print children recursively for inner nodes
        if (node instanceof BPTreeInnerNode) {
            BPTreeInnerNode<T> innerNode = (BPTreeInnerNode<T>) node;
            for (int i = 0; i < innerNode.numberOfKeys; i++) {
                boolean newIsTail = (i == innerNode.numberOfKeys - 1);
                visualizeTreeHelper(innerNode.children[i], prefix + (isTail ? "    " : "│   "), newIsTail);
            }
            visualizeTreeHelper(innerNode.children[innerNode.numberOfKeys], prefix + (isTail ? "    " : "│   "), true);
        }
    }

    public void insertingWithShifting(T key, Ref recordReference, int maxPageSize){

        int insertedIndex = recordReference.getIndexInPage();
        String insertedPage = recordReference.getPage();
        BPTreeLeafNode currLeaf = this.searchMinNode();

        while(currLeaf!=null ){

            for(int i = 0;i< currLeaf.numberOfKeys; i++){

                // Shifting keys' references without considering duplicates
                if(currLeaf.records[i].getPage().compareToIgnoreCase(insertedPage) > 0|| currLeaf.records[i].getPage().compareToIgnoreCase(insertedPage) == 0 && currLeaf.records[i].getIndexInPage()>=insertedIndex ){
                    int currIndex = currLeaf.records[i].getIndexInPage();
                    // Handling case where there is no room in the page
                    if(currIndex+1 > maxPageSize){
                        String[] x = currLeaf.records[i].getPage().split("_");
                        int pageNum = Integer.parseInt(x[1]);
                        String tableName = x[0];
                        currLeaf.records[i].setIndexInPage(1);
                        currLeaf.records[i].setPageName(tableName+"_"+ (pageNum+1));
                    }
                    else{
                        currLeaf.records[i].setIndexInPage(currLeaf.records[i].getIndexInPage()+1);
                    }
                }

                // Handling duplicates (if present)
                if (currLeaf.getOverflow(i)!= null && currLeaf.getOverflow(i).size()>0 ) {
                    int size = currLeaf.getOverflow(i).size();
                    // Traverse the duplicates and check if they should be changed or not
                    for(int j =0; j< size; j++){
                        int currentIndex = ((Ref)currLeaf.getOverflow(i).get(j)).getIndexInPage();
                        String currentPage =  ((Ref)currLeaf.getOverflow(i).get(j)).getPage();
                        if(currentPage.compareToIgnoreCase(insertedPage)> 0 || currentPage.compareToIgnoreCase(insertedPage) == 0 && currentIndex>= insertedIndex){
                            // Handling case where there is no room in the page
                            if(currentIndex+1 > maxPageSize){
                                String[] x = currentPage.split("_");
                                int pageNum = Integer.parseInt(x[1]);
                                String tableName = x[0];
                                currLeaf.getOverflow(i).remove(j);
                                currLeaf.getOverflow(i).add(j, new Ref(tableName+"_"+ (pageNum+1), 1));
                            }
                            else{
                                currLeaf.getOverflow(i).remove(j);
                                currLeaf.getOverflow(i).add(j, new Ref(currentPage, currentIndex+1));
                            }
                        }
                    }
                }
            }
            currLeaf = currLeaf.getNext();
        }
        this.insert( key, recordReference);

    }

    public void deletingWithShifting(T key, Ref recordReference){

        int deletedIndex = recordReference.getIndexInPage();
        String deletedPage = recordReference.getPage();
        BPTreeLeafNode currLeaf = this.searchMinNode();

        while(currLeaf!=null ){

            for(int i = 0;i< currLeaf.numberOfKeys; i++){

                // Shifting keys' references without considering duplicates
                if(currLeaf.records[i].getPage().compareToIgnoreCase(deletedPage) == 0 && currLeaf.records[i].getIndexInPage()>deletedIndex ){
                    currLeaf.records[i].setIndexInPage(currLeaf.records[i].getIndexInPage()-1);
                }

                // Handling duplicates (if present)
                if (currLeaf.getOverflow(i)!= null && currLeaf.getOverflow(i).size()>0 ) {
                    int size = currLeaf.getOverflow(i).size();
                    // Traverse the duplicates and check if they should be changed or not
                    for(int j =0; j< size; j++){
                        int currentIndex = ((Ref)currLeaf.getOverflow(i).get(j)).getIndexInPage();
                        String currentPage =  ((Ref)currLeaf.getOverflow(i).get(j)).getPage();
                        if( currentPage.compareToIgnoreCase(deletedPage) == 0 && currentIndex> deletedIndex){
                                currLeaf.getOverflow(i).remove(j);
                                currLeaf.getOverflow(i).add(j, new Ref(currentPage, currentIndex-1));
                        }
                    }
                }
            }
            currLeaf = currLeaf.getNext();
        }
        this.delete( key, recordReference);

    }

    public static ArrayList<Ref> getRefs(BPTreeLeafNode bnode){
        ArrayList<Ref> allRefs = new ArrayList<>();
        BPTreeLeafNode currLeaf = bnode;

        while(currLeaf!=null) {
            System.out.println(currLeaf);
            for (int i = 0; i < currLeaf.numberOfKeys; i++) {
                System.out.println(currLeaf.records[i].getPage()+": " +currLeaf.records[i].getIndexInPage());
                if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                    int size = currLeaf.getOverflow(i).size();
                    for (int j = 0; j < size; j++)
                        allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                }
            }
            currLeaf = currLeaf.getNext();
        }
        return allRefs;
    }

    public ArrayList<Ref> getRefsGreaterThan ( T key){
        ArrayList<Ref> allRefs = new ArrayList<>();
        BPTreeLeafNode currLeaf = this.searchGreaterthan(key);
        System.out.println(currLeaf.findIndex((Comparable) key));
        System.out.println(currLeaf);

        boolean firstLeaf = true;
        while(currLeaf!=null) {
            if(firstLeaf){
                for (int i = currLeaf.findIndex((Comparable) key); i < currLeaf.numberOfKeys; i++) {
                    allRefs.add(currLeaf.records[i]);
                    if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                        int size = currLeaf.getOverflow(i).size();
                        for (int j = 0; j < size; j++)
                            allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                    }
                }
                firstLeaf =false;
            }
            else{
                for (int i = 0; i < currLeaf.numberOfKeys; i++) {
                    allRefs.add(currLeaf.records[i]);
                    if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                        int size = currLeaf.getOverflow(i).size();
                        for (int j = 0; j < size; j++)
                            allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                    }
                }

            }

            currLeaf = currLeaf.getNext();
        }

        return allRefs;
    }

    public ArrayList<Ref> getRefsGreaterEqual ( T key){
        ArrayList<Ref> allRefs = new ArrayList<>();
        BPTreeLeafNode currLeaf = this.searchGreaterEqual(key);
        System.out.println(currLeaf.findIndex((Comparable) key));

        boolean firstLeaf = true;
        while(currLeaf!=null) {
            if(firstLeaf){
                for (int i = currLeaf.findIndex((Comparable) key)-1; i < currLeaf.numberOfKeys; i++) {
                    allRefs.add(currLeaf.records[i]);
                    if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                        int size = currLeaf.getOverflow(i).size();
                        for (int j = 0; j < size; j++)
                            allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                    }
                }
                firstLeaf =false;
            }
            else{
                for (int i = 0; i < currLeaf.numberOfKeys; i++) {
                    allRefs.add(currLeaf.records[i]);
                    if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                        int size = currLeaf.getOverflow(i).size();
                        for (int j = 0; j < size; j++)
                            allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                    }
                }

            }

            currLeaf = currLeaf.getNext();
        }

        return allRefs;
    }

    public ArrayList<Ref> getRefsLessThan ( T key){
        ArrayList<Ref> allRefs = new ArrayList<>();
        BPTreeLeafNode currLeaf = this.searchMinNode();

        while(currLeaf!=null ) {
            for (int i = 0; i < currLeaf.numberOfKeys ; i++) {
                if(currLeaf.keys[i].equals(key))
                    return allRefs;
                allRefs.add(currLeaf.records[i]);
                if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                    int size = currLeaf.getOverflow(i).size();
                    for (int j = 0; j < size; j++)
                        allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                }
            }
            currLeaf = currLeaf.getNext();
        }
        return allRefs;
    }

    public ArrayList<Ref> getRefsLessEqual ( T key){
        ArrayList<Ref> allRefs = new ArrayList<>();
        BPTreeLeafNode currLeaf = this.searchMinNode();
        boolean flag = false;

        while(currLeaf!=null && !flag ) {
            for (int i = 0; i < currLeaf.numberOfKeys && !flag ; i++) {
                if(currLeaf.keys[i].equals(key))
                    flag = true;
                allRefs.add(currLeaf.records[i]);
                if (currLeaf.getOverflow(i) != null && currLeaf.getOverflow(i).size() > 0) {
                    int size = currLeaf.getOverflow(i).size();
                    for (int j = 0; j < size; j++)
                        allRefs.add((Ref) currLeaf.getOverflow(i).get(j));
                }
            }
            currLeaf = currLeaf.getNext();
        }
        return allRefs;
    }



    public static void main(String[] args) {
//
//        BPTree<String> treename= new BPTree<>(2);
//
//        //first page inserts
//        treename.insert("salma", new Ref("p_1", 1));
//        treename.insert("ahmed", new Ref("p_1", 2));
//        treename.insert("farida", new Ref("p_1", 3));
//        treename.insert("alia", new Ref("p_1", 4));
//        treename.insert("karma", new Ref("p_1", 5));
//
//        // second page inserts
//        treename.insert("fady", new Ref("p_2", 1));
//        treename.insert("mai", new Ref("p_2", 2));
//        treename.insert("razan", new Ref("p_2", 3));
//        treename.insert("tala", new Ref("p_2", 4));
//        treename.insert("alia", new Ref("p_2", 5));
//
//        //third page inserts
//        treename.insert("alia", new Ref("p_3", 1));
//        treename.insert("alia", new Ref("p_3", 2));
//        treename.insert("alia", new Ref("p_3", 3));
//        System.out.println(treename);
//
//
//        int insertedIndex = 3;
//        String insertedPage = "p_2";
//        //inserting something in the middle requiring shifting
//        treename.deletingWithShifting("razan", new Ref(insertedPage,insertedIndex));
//        System.out.println(treename);
//
//
//
//        BPTreeLeafNode firstLeaf = treename.searchMinNode();
//        BPTreeLeafNode currLeaf = firstLeaf;
//
//        while(currLeaf!=null ){
//            for(int i = 0;i< currLeaf.numberOfKeys; i++){
//                System.out.println(currLeaf);
//                System.out.println(currLeaf.records[i].getPage());
//                System.out.println(currLeaf.records[i].getIndexInPage());
//                if (currLeaf.getOverflow(i)!= null && currLeaf.getOverflow(i).size()>0 ) {
//                    int size = currLeaf.getOverflow(i).size();
//                    // Traverse the duplicates and check if they should be changed or not
//                    for(int j =0; j< size; j++){
//                        int currentIndex = ((Ref)currLeaf.getOverflow(i).get(j)).getIndexInPage();
//                        String currentPage =  ((Ref)currLeaf.getOverflow(i).get(j)).getPage();
//                        System.out.println(currentPage);
//                        System.out.println(currentIndex);
//                    }
//                }
//            }
//            currLeaf = currLeaf.getNext();
//        }





        //handle case where ur search = null so not there



    }
}