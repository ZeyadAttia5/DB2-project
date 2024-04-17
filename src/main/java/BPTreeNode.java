import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;

public abstract class BPTreeNode<T extends Comparable<T>> implements Serializable {

    /**
     * Abstract class that collects the common functionalities of the inner and leaf
     * nodes
     */
    private static final long serialVersionUID = 1L;
    protected Comparable<T>[] keys;
    protected int numberOfKeys;

    protected int order;
    protected int index; // for printing the tree
    private boolean isRoot;
    private static int nextIdx = 0;
    private String filename;

    public BPTreeNode(int order) {
        index = nextIdx++;
        numberOfKeys = 0;
        this.order = order;
        if (!this.isRoot()) {
        }
    }

    /**
     * @return a boolean indicating whether this node is the root of the B+ tree
     */
    public boolean isRoot() {
        return isRoot;
    }

    /**
     * set this node to be a root or unset it if it is a root
     *
     * @param isRoot the setting of the node
     */
    public void setRoot(boolean isRoot) {
        this.isRoot = isRoot;
    }

    /**
     * find the key at the specified index
     *
     * @param index the index at which the key is located
     * @return the key which is located at the specified index
     */
    public Comparable<T> getKey(int index) {
        return keys[index];
    }

    /**
     * sets the value of the key at the specified index
     *
     * @param index the index of the key to be set
     * @param key   the new value for the key
     */
    public void setKey(int index, Comparable<T> key) {
        keys[index] = key;
    }

    /**
     * @return a boolean whether this node is full or not
     */
    public boolean isFull() {
        return numberOfKeys == order;
    }

    /**
     * @return the last key in this node
     */
    public Comparable<T> getLastKey() {
        return keys[numberOfKeys - 1];
    }

    /**
     * @return the first key in this node
     */
    public Comparable<T> getFirstKey() {
        return keys[0];
    }

    /**
     * @return the minimum number of keys this node can hold
     */
    public abstract int minKeys();

    /**
     * insert a key with the associated record reference in the B+ tree
     *
     * @param key             the key to be inserted
     * @param recordReference a pointer to the record on the hard disk
     * @param parent          the parent of the current node
     * @param ptr             the index of the parent pointer that points to this
     *                        node
     * @return a key and a new node in case of a node splitting and null otherwise
     */
    public abstract PushUp<T> insert(T key, Ref recordReference, BPTreeInnerNode<T> parent, int ptr);

    public abstract Ref search(T key);

    public int getNumberOfKeys() {
        return numberOfKeys;
    }

    public BPTreeLeafNode searchForMin() {
        if (this instanceof BPTreeLeafNode)
            return (BPTreeLeafNode) this;
        BPTreeInnerNode n = (BPTreeInnerNode) this;
        return n.getChild(0).searchForMin();
    }

    public BPTreeLeafNode searchForNode2(T key) {
        if (this instanceof BPTreeLeafNode) {

            BPTreeLeafNode b = (BPTreeLeafNode) this;
            while (b != null) {
                if (b.contains(key)) {
                    return b;
                } else
                    b = b.getNext();
            }
            return null;
        }
        int i = 0;

        if (this.getKey(i).compareTo(key) > 0) {
            // I am less than first element
            BPTreeInnerNode n = (BPTreeInnerNode) this;
            BPTreeNode node = n.getChild(i);
            return node.searchForNode2(key);

        } else {
            // I am either greater than first element but less than second or greater than
            // secon
            if (this.getKey(i).compareTo(key) == 0) {
                BPTreeInnerNode n = (BPTreeInnerNode) this;
                BPTreeNode nodeRight = n.getChild(i + 1);
                BPTreeNode nodeLeft = n.getChild(i);
                BPTreeLeafNode out = nodeLeft.searchForNode2(key);
                if (out != null)
                    return out;
                else
                    return nodeRight.searchForNode2(key);

            } else {
                if (this.numberOfKeys > 1) {
                    if (this.getKey(i + 1).compareTo(key) > 0) {
                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode node = n.getChild(i + 1);
                        return node.searchForNode2(key);

                    } else if (this.getKey(i + 1).compareTo(key) == 0) {
                        int k = i + 1;
                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode nodeRight = n.getChild(k + 1);
                        BPTreeNode nodeLeft = n.getChild(k);
                        BPTreeLeafNode out = nodeLeft.searchForNode2(key);
                        if (out != null)
                            return out;
                        else
                            return nodeRight.searchForNode2(key);

                    }

                    else {
                        for (int j = i; j < this.numberOfKeys - 1; j++) {
                            if (this.getKey(j + 1).compareTo(key) > 0) {
                                BPTreeInnerNode n = (BPTreeInnerNode) this;
                                BPTreeNode node = n.getChild(j + 1);
                                return node.searchForNode2(key);

                            }
                        }

                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode node = n.getChild(this.numberOfKeys);
                        return node.searchForNode2(key);

                    }
                } else {
                    BPTreeInnerNode n = (BPTreeInnerNode) this;
                    BPTreeNode node = n.getChild(i + 1);
                    return node.searchForNode2(key);

                }
            }
        }

    }

    public BPTreeLeafNode searchForNodeRef(T key, Ref ref, BPTree tree) {
        if (this instanceof BPTreeLeafNode) {
            BPTreeLeafNode b = (BPTreeLeafNode) this;
            while (b != null) {
                if (b.contains(key)) {
                    int i = b.containsAt(key);
                    if (b.getRecord(i) != null) {
                        if (ref != null && b.getRecord(i).isEqual(ref))
                            return b;
                    } else {
                        if (ref == null)
                            return b;
                    }
                }
                b = b.getNext();
            }
            return null;
        }
        int i = 0;

        if (this.getKey(i).compareTo(key) > 0) {
            // I am less than first element
            BPTreeInnerNode n = (BPTreeInnerNode) this;
            BPTreeNode node = n.getChild(i);
            return node.searchForNodeRef(key, ref, tree);

        } else {
            // I am either greater than first element but less than second or greater than
            // secon
            if (this.getKey(i).compareTo(key) == 0) {
                BPTreeInnerNode n = (BPTreeInnerNode) this;
                BPTreeNode nodeRight = n.getChild(i + 1);
                BPTreeNode nodeLeft = n.getChild(i);
                BPTreeLeafNode out = nodeLeft.searchForNodeRef(key, ref, tree);
                if (out != null) {
                    return out;

                } else {
                    return nodeRight.searchForNodeRef(key, ref, tree);
                }

            } else {
                if (this.numberOfKeys > 1) {
                    if (this.getKey(i + 1).compareTo(key) > 0) {
                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode node = n.getChild(i + 1);
                        return node.searchForNodeRef(key, ref, tree);

                    } else if (this.getKey(i + 1).compareTo(key) == 0) {
                        int k = i + 1;
                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode nodeRight = n.getChild(k + 1);
                        BPTreeNode nodeLeft = n.getChild(k);
                        BPTreeLeafNode out = nodeLeft.searchForNodeRef(key, ref, tree);
                        if (out != null) {
                            return out;
                        } else {
                            return nodeRight.searchForNodeRef(key, ref, tree);
                        }
                    }

                    else {
                        for (int j = i; j < this.numberOfKeys - 1; j++) {
                            if (this.getKey(j + 1).compareTo(key) > 0) {
                                BPTreeInnerNode n = (BPTreeInnerNode) this;
                                BPTreeNode node = n.getChild(j + 1);
                                return node.searchForNodeRef(key, ref, tree);

                            }
                        }

                        BPTreeInnerNode n = (BPTreeInnerNode) this;
                        BPTreeNode node = n.getChild(this.numberOfKeys);
                        return node.searchForNodeRef(key, ref, tree);

                    }
                } else {
                    BPTreeInnerNode n = (BPTreeInnerNode) this;
                    BPTreeNode node = n.getChild(i + 1);
                    return node.searchForNodeRef(key, ref, tree);

                }
            }
        }

    }

    public abstract boolean deleteLeft(T key, BPTreeInnerNode<T> parent, int ptr, Ref ref);

    /**
     * searches for all references of this key handling duplicates
     */

    public ArrayList<Ref> searchFromNodesDuplicates(T key) {
        ArrayList<Ref> output = new ArrayList<Ref>();
        BPTreeLeafNode firstLeaf = this.searchForNode2(key);
        if (firstLeaf == null)
            return null;
        for (int i = 0; i < firstLeaf.numberOfKeys; i++) {
            if (firstLeaf.getKey(i).compareTo(key) == 0) {
                output.add(firstLeaf.getRecord(i));
                if (firstLeaf.getOverflow(i) != null) {
                    for (int m = 0; m < firstLeaf.getOverflow(i).size(); m++) {
                        output.add((Ref) firstLeaf.getOverflow(i).get(m));
                    }
                }
            }
        }
        return output;

    }

    public Ref searchHelper(T key) {

        return null;

    }

    /**
     * delete a key from the B+ tree recursively
     *
     * @param key    the key to be deleted from the B+ tree
     * @param parent the parent of the current node
     * @param ptr    the index of the parent pointer that points to this node
     * @return true if this node was successfully deleted and false otherwise
     */
    public abstract boolean delete(T key, BPTreeInnerNode<T> parent, int ptr, Ref ref);

    /*
     * REturns the leadnode of the key that is just greater than the key
     */
    public BPTreeLeafNode searchGreaterthan(T key) {
        if (this instanceof BPTreeLeafNode) {
            BPTreeLeafNode b = (BPTreeLeafNode) this;
            for (int i = 0; i < b.numberOfKeys; i++) {
                if (keys[i].compareTo(key) > 0) {
                    return b;
                }
            }
            return null;
        }
        int i = 0;
        // I am less than first elemen
        BPTreeInnerNode n = (BPTreeInnerNode) this;
        if (n.keys[i].compareTo(key) > 0) {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreaterthan(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreaterthan(key);
            }
        } else if (n.keys[i].compareTo(key) < 0) {
            if (n.numberOfKeys > 1) {
                if (n.keys[i + 1].compareTo(key) > 0) {
                    BPTreeNode leftNode = n.getChild(i + 1);
                    BPTreeNode rightNode = n.getChild(i + 2);
                    BPTreeLeafNode out = leftNode.searchGreaterthan(key);
                    if (out != null)
                        return out;
                    else {
                        return rightNode.searchGreaterthan(key);
                    }
                } else {
                    for (int j = i; j < this.numberOfKeys - 1; j++) {
                        if (this.getKey(j + 1).compareTo(key) > 0) {
                            BPTreeInnerNode nn = (BPTreeInnerNode) this;
                            BPTreeNode leftNode = nn.getChild(j + 1);
                            BPTreeNode rightNode = n.getChild(j + 2);
                            BPTreeLeafNode out = leftNode.searchGreaterthan(key);
                            if (out != null)
                                return out;
                            else {
                                return rightNode.searchGreaterthan(key);
                            }

                        }
                    }

                    BPTreeInnerNode no = (BPTreeInnerNode) this;
                    BPTreeNode node = no.getChild(this.numberOfKeys);
                    return node.searchGreaterthan(key);

                }
            } else {
                BPTreeInnerNode nn = (BPTreeInnerNode) this;
                BPTreeNode nnode = nn.getChild(i + 1);
                return nnode.searchGreaterthan(key);
            }

        } else {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreaterthan(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreaterthan(key);
            }
        }

    }

    public BPTreeLeafNode searchGreatereEqualPoly(T key) {
        if (this instanceof BPTreeLeafNode) {
            BPTreeLeafNode b = (BPTreeLeafNode) this;
            for (int i = 0; i < b.numberOfKeys; i++) {
                if (keys[i].compareTo(key) >= 0 || keys[i].compareTo(key) == -2) {
                    return b;
                }
            }
            return null;
        }
        int i = 0;
        // I am less than first elemen
        BPTreeInnerNode n = (BPTreeInnerNode) this;
        if (n.keys[i].compareTo(key) > 0) {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreatereEqualPoly(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreatereEqualPoly(key);
            }
        } else if (n.keys[i].compareTo(key) ==-1) {
            if (n.numberOfKeys > 1) {
                if (n.keys[i + 1].compareTo(key) > 0) {
                    BPTreeNode leftNode = n.getChild(i + 1);
                    BPTreeNode rightNode = n.getChild(i + 2);
                    BPTreeLeafNode out = leftNode.searchGreatereEqualPoly(key);
                    if (out != null)
                        return out;
                    else {
                        return rightNode.searchGreatereEqualPoly(key);
                    }
                } else {
                    for (int j = i; j < this.numberOfKeys - 1; j++) {
                        if (this.getKey(j + 1).compareTo(key) > 0) {
                            BPTreeInnerNode nn = (BPTreeInnerNode) this;
                            BPTreeNode leftNode = nn.getChild(j + 1);
                            BPTreeNode rightNode = n.getChild(j + 2);
                            BPTreeLeafNode out = leftNode.searchGreatereEqualPoly(key);
                            if (out != null)
                                return out;
                            else {
                                return rightNode.searchGreatereEqualPoly(key);
                            }

                        }
                    }

                    BPTreeInnerNode no = (BPTreeInnerNode) this;
                    BPTreeNode node = no.getChild(this.numberOfKeys);
                    return node.searchGreatereEqualPoly(key);

                }
            } else {
                BPTreeInnerNode nn = (BPTreeInnerNode) this;
                BPTreeNode nnode = nn.getChild(i + 1);
                return nnode.searchGreatereEqualPoly(key);
            }

        } else {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreatereEqualPoly(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreatereEqualPoly(key);
            }
        }

    }

    public BPTreeLeafNode searchGreatereEqual(T key) {
        if (this instanceof BPTreeLeafNode) {
            BPTreeLeafNode b = (BPTreeLeafNode) this;
            for (int i = 0; i < b.numberOfKeys; i++) {
                if (keys[i].compareTo(key) > 0 || keys[i].compareTo(key) == 0) {
                    return b;
                }
            }
            return null;
        }
        int i = 0;
        // I am less than first elemen
        BPTreeInnerNode n = (BPTreeInnerNode) this;
        if (n.keys[i].compareTo(key) > 0) {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreatereEqual(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreatereEqual(key);
            }
        } else if (n.keys[i].compareTo(key) < 0) {
            if (n.numberOfKeys > 1) {
                if (n.keys[i + 1].compareTo(key) > 0) {
                    BPTreeNode leftNode = n.getChild(i + 1);
                    BPTreeNode rightNode = n.getChild(i + 2);
                    BPTreeLeafNode out = leftNode.searchGreatereEqual(key);
                    if (out != null)
                        return out;
                    else {
                        return rightNode.searchGreatereEqual(key);
                    }
                } else {
                    for (int j = i; j < this.numberOfKeys - 1; j++) {
                        if (this.getKey(j + 1).compareTo(key) > 0) {
                            BPTreeInnerNode nn = (BPTreeInnerNode) this;
                            BPTreeNode leftNode = nn.getChild(j + 1);
                            BPTreeNode rightNode = n.getChild(j + 2);
                            BPTreeLeafNode out = leftNode.searchGreatereEqual(key);
                            if (out != null)
                                return out;
                            else {
                                return rightNode.searchGreatereEqual(key);
                            }

                        }
                    }

                    BPTreeInnerNode no = (BPTreeInnerNode) this;
                    BPTreeNode node = no.getChild(this.numberOfKeys);
                    return node.searchGreatereEqual(key);

                }
            } else {
                BPTreeInnerNode nn = (BPTreeInnerNode) this;
                BPTreeNode nnode = nn.getChild(i + 1);
                return nnode.searchGreatereEqual(key);
            }

        } else {
            BPTreeNode leftNode = n.getChild(i);
            BPTreeNode rightNode = n.getChild(i + 1);
            BPTreeLeafNode out = leftNode.searchGreatereEqual(key);
            if (out != null)
                return out;
            else {
                return rightNode.searchGreatereEqual(key);
            }
        }

    }

    /**
     * A string represetation for the node
     */
    public String toString() {
        String s = "(" + index + ")";

        s += "[";
        for (int i = 0; i < order; i++) {
            String key = " ";
            if (i < numberOfKeys)
                key = keys[i].toString();

            s += key;
            if (i < order - 1)
                s += "|";
        }
        s += "]";
        return s;
    }

}