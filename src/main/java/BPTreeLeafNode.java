import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class BPTreeLeafNode<T extends Comparable<T>> extends BPTreeNode<T> implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public Ref[] records;
    ArrayList<Vector<Ref>> overflow;
    private BPTreeLeafNode<T> next;

    @SuppressWarnings("unchecked")
    public BPTreeLeafNode(int n) {
        super(n);
        keys = new Comparable[n];
        records = new Ref[n];
        overflow = new ArrayList();
        for (int i = 0; i < n; i++) {
            overflow.add(i, null);
        }
    }

    /**
     * @return the next leaf node
     */
    public BPTreeLeafNode<T> getNext() {
        return this.next;
    }

    public Ref[] getRecords() {
        return records;
    }

    public boolean isEmpty() {
        if (keys[0] == null)
            return true;
        return false;
    }

    /**
     * sets the next leaf node
     *
     * @param node the next leaf node
     */
    public void setNext(BPTreeLeafNode<T> node) {
        this.next = node;
    }

    /**
     * @param index the index to find its record
     * @return the reference of the queried index
     */
    public Ref getRecord(int index) {
        return records[index];
    }

    /**
     * sets the record at the given index with the passed reference
     *
     * @param index           the index to set the value at
     * @param recordReference the reference to the record
     */
    public void setRecord(int index, Ref recordReference) {
        records[index] = recordReference;
    }

    /**
     * @return the reference of the last record
     */
    public Ref getFirstRecord() {
        return records[0];
    }

    /**
     * @return the reference of the last record
     */
    public Ref getLastRecord() {
        return records[numberOfKeys - 1];
    }

    /**
     * finds the minimum number of keys the current node must hold
     */
    public int minKeys() {
        if (this.isRoot())
            return 1;
        return (order + 1) / 2;
    }

    /**
     * insert the specified key associated with a given record refernce in the B+
     * tree
     */
    public PushUp<T> insert(T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr) {
        if (this.isFull()) {
            BPTreeNode<T> newNode = this.split(key, recordReference);
            Comparable<T> newKey = newNode.getFirstKey();
            return new PushUp<T>(newNode, newKey);
        } else {
            int index = 0;
            while (index < numberOfKeys && getKey(index).compareTo(key) <= 0)
                ++index;
            this.insertAt(index, key, recordReference, null);
            return null;
        }
    }

    public void setOverflow(int i, Vector<Ref> r) {
        // this.overflow.remove(i);
        this.overflow.add(i, r);
    }

    public Vector<Ref> getOverflow(int index) {
        return this.overflow.get(index);
    }

    /**
     * inserts the passed key associated with its record reference in the specified
     * index
     *
     * @param index           the index at which the key will be inserted
     * @param key             the key to be inserted
     * @param recordReference the pointer to the record associated with the key
     */
    private void insertAt(int index, Comparable<T> key, Ref recordReference, Vector<Ref> r) {
        for (int i = numberOfKeys - 1; i >= index; --i) {
            this.setKey(i + 1, getKey(i));
            this.setRecord(i + 1, getRecord(i));
//			if (this.getOverflow(i) != null && this.getOverflow(i).size() > 0) {
//				this.setOverflow(i + 1, this.getOverflow(i));
//			}
        }

        this.setKey(index, key);
        this.setRecord(index, recordReference);
        if (r != null && r.size() > 0)
            this.setOverflow(index, r);
        else
            this.setOverflow(index, null);
        ++numberOfKeys;
    }

    /**
     * splits the current node
     *
     * @param key             the new key that caused the split
     * @param recordReference the reference of the new key
     * @return the new node that results from the split
     */
    public BPTreeNode<T> split(T key, Ref recordReference) {
        int keyIndex = this.findIndex(key);
        int midIndex = numberOfKeys / 2;
        if ((numberOfKeys & 1) == 1 && keyIndex > midIndex) // split nodes evenly
            ++midIndex;

        int totalKeys = numberOfKeys + 1;
        // move keys to a new node
        BPTreeLeafNode<T> newNode = new BPTreeLeafNode<T>(order);
        for (int i = midIndex; i < totalKeys - 1; ++i) {
            newNode.insertAt(i - midIndex, this.getKey(i), this.getRecord(i), this.getOverflow(i));
            numberOfKeys--;
        }

        // insert the new key
        if (keyIndex < totalKeys / 2)
            this.insertAt(keyIndex, key, recordReference, null);
        else
            newNode.insertAt(keyIndex - midIndex, key, recordReference, null);

        // set next pointers
        newNode.setNext(this.getNext());
        this.setNext(newNode);
        return newNode;

    }

    /**
     * finds the index at which the passed key must be located
     *
     * @param key the key to be checked for its location
     * @return the expected index of the key
     */
    public int findIndex(T key) {
        for (int i = 0; i < numberOfKeys; ++i) {
            int cmp = getKey(i).compareTo(key);
            if (cmp > 0)
                return i;
        }
        return numberOfKeys;
    }

    /**
     * returns the record reference with the passed key and null if does not exist
     */
    @Override
    public Ref search(T key) {
        for (int i = 0; i < numberOfKeys; ++i)
            if (this.getKey(i).compareTo(key) == 0)
                return this.getRecord(i);
        return null;

    }

    /**
     * delete the passed key from the B+ tree
     */
    public boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, Ref ref) {
        for (int i = 0; i < numberOfKeys; i++) {
            if (keys[i].compareTo(key) == 0) {
                if (ref != null) {
                    if (this.getRecord(i) != null) {
                        if (ref != null && this.getRecord(i).isEqual(ref)) {
                            if (this.getOverflow(i) != null && this.getOverflow(i).size() > 0) {
                                Ref refs = this.getOverflow(i).get(0);
                                this.getOverflow(i).remove(0);
                                if (this.getOverflow(i).size() == 0) {
                                    this.overflow.remove(i);
                                    this.overflow.add(null);
                                }

                                this.setRecord(i, refs);
                                return true;
                            } else {
                                this.deleteAt(i);
                                if (i == 0 && ptr > 0) {
                                    // update key at parent
                                    parent.setKey(ptr - 1, this.getFirstKey());
                                }
                                // check that node has enough keys
                                if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                                    // 1.try to borrow
                                    if (borrow(parent, ptr))
                                        return true;
                                    // 2.merge
                                    merge(parent, ptr);
                                }
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * delete a key at the specified index of the node
     *
     * @param index the index of the key to be deleted
     */
    public void deleteAt(int index) {

        for (int i = index; i < numberOfKeys - 1; ++i) {
            keys[i] = keys[i + 1];
            records[i] = records[i + 1];
//			this.initializeVectors(i);
//			Vector<Ref> r = overflow.get(i);
//			for (int j = 0; j < r.size(); j++) {
//				this.overflow.get(i).add(j, r.get(j));
        }

        overflow.remove(index);

        numberOfKeys--;
    }

    /**
     * tries to borrow a key from the left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @return true if borrow is done successfully and false otherwise
     */
    public boolean borrow(BPTreeInnerNode<T> parent, int ptr) {
        // check left sibling
        if (ptr > 0) {
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr - 1);
            if (leftSibling.numberOfKeys > leftSibling.minKeys()) {
                this.insertAt(0, leftSibling.getLastKey(), leftSibling.getLastRecord(),
                        leftSibling.getOverflow(leftSibling.numberOfKeys));
                leftSibling.deleteAt(leftSibling.numberOfKeys - 1);
                parent.setKey(ptr - 1, keys[0]);
                return true;
            }
        }

        // check right sibling
        if (ptr < parent.numberOfKeys) {
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr + 1);
            if (rightSibling.numberOfKeys > rightSibling.minKeys()) {
                this.insertAt(numberOfKeys, rightSibling.getFirstKey(), rightSibling.getFirstRecord(),
                        rightSibling.getOverflow(0));
                rightSibling.deleteAt(0);
                parent.setKey(ptr, rightSibling.getFirstKey());
                return true;
            }
        }
        return false;
    }

    /**
     * merges the current node with its left or right sibling
     *
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     */
    public void merge(BPTreeInnerNode<T> parent, int ptr) {
        if (ptr > 0) {
            // merge with left
            BPTreeLeafNode<T> leftSibling = (BPTreeLeafNode<T>) parent.getChild(ptr - 1);
            leftSibling.merge(this);
            parent.deleteAt(ptr - 1);
        } else {
            // merge with right
            BPTreeLeafNode<T> rightSibling = (BPTreeLeafNode<T>) parent.getChild(ptr + 1);
            this.merge(rightSibling);
            parent.deleteAt(ptr);
        }
    }

    public boolean contains(T key) {
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.keys[i].compareTo(key) == 0) {
                return true;
            }
        }
        return false;
    }

    public int containsAt(T key) {
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.keys[i].compareTo(key) == 0) {
                return i;
            }
        }
        return -1;
    }

    // search for leaf node containing key
    // do has duplicates on it
    // needed in delete
    public boolean hasDuplicates(T key) {
        int location = 0;
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.getKey(i).compareTo(key) == 0) {
                location = i;
                break;
            }
        }
        if (this.overflow.get(location) == null)
            return false;
        else
            return true;
    }

    public void initializeVectors(int location) {
        for (int i = 0; i < this.order; i++) {
            if (i == location) {
                this.overflow.remove(i);
                this.overflow.add(i, new Vector<Ref>());
            }
        }

    }

    public void insertOverflow(T key, Ref ref) {
        int location = -1;
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.getKey(i).compareTo(key) == 0) {
                location = i;
                break;
            }
        }
        if (this.overflow.get(location) == null) {
            this.initializeVectors(location);
        }
        this.overflow.get(location).add(ref);

    }

    public boolean deleteOverflow(T key, Ref ref) {
        int location = -1;
        boolean done = false;
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.getKey(i).compareTo(key) == 0) {
                location = i;
                break;
            }
        }
        if (ref != null) {
            int x = -1;
            for (int i = 0; i < this.overflow.get(location).size(); i++)
                if (this.overflow.get(location).get(i).isEqual(ref))
                    x = i;
            if (x != -1) {
                this.overflow.get(location).removeElementAt(x);
                done = true;
            }
        }
        if (this.overflow.get(location).size() == 0) {
            this.overflow.remove(location);
            this.overflow.add(location, null);
        }

        return done;

    }

    public void print() {
        for (int i = 0; i < this.numberOfKeys; i++) {
            if (this.overflow.get(i) != null) {
                System.out.print(this.overflow.get(i));

            } else {
                System.out.print("null");
            }

        }

    }

    /**
     * merge the current node with the specified node. The foreign node will be
     * deleted
     *
     * @param foreignNode the node to be merged with the current node
     */
    public void merge(BPTreeLeafNode<T> foreignNode) {
        for (int i = 0; i < foreignNode.numberOfKeys; ++i)
            this.insertAt(numberOfKeys, foreignNode.getKey(i), foreignNode.getRecord(i), foreignNode.getOverflow(i));

        this.setNext(foreignNode.getNext());
    }

    @Override
    public boolean deleteLeft(T key, BPTreeInnerNode<T> parent, int ptr, Ref ref) {
        for (int i = 0; i < numberOfKeys; i++) {
            if (keys[i].compareTo(key) == 0) {

                if (ref == null) {
                    // delete regardless
                    this.deleteAt(i);
                    if (i == 0 && ptr > 0) {
                        // update key at parent
                        parent.setKey(ptr - 1, this.getFirstKey());
                    }
                    // check that node has enough keys
                    if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                        // 1.try to borrow
                        if (borrow(parent, ptr))
                            return true;
                        // 2.merge
                        merge(parent, ptr);
                    }
                    return true;
                } else {
                    // not null so I have to check
                    if (this.getRecord(i) != null) {
                        if (ref != null && this.getRecord(i).isEqual(ref)) {
                            this.deleteAt(i);
                            if (i == 0 && ptr > 0) {
                                // update key at parent
                                parent.setKey(ptr - 1, this.getFirstKey());
                            }
                            // check that node has enough keys
                            if (!this.isRoot() && numberOfKeys < this.minKeys()) {
                                // 1.try to borrow
                                if (borrow(parent, ptr))
                                    return true;
                                // 2.merge
                                merge(parent, ptr);
                            }
                            return true;
                        }
                    }
                }

            }
        }

        return false;

    }
}