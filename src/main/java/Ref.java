import java.io.Serializable;

public class Ref implements Serializable {

    /**
     * This class represents a pointer to the record. It is used at the leaves of
     * the B+ tree
     */
    private static final long serialVersionUID = 1L;
    private int indexInPage;
    private Page page;

    public Ref(Page page, int indexInPage) {
        this.page = page;
        this.indexInPage = indexInPage;
    }

    /**
     * @return the page at which the record is saved on the hard disk
     */
    public Page getPage() {
        return page;
    }

    /**
     * @return the index at which the record is saved in the page
     */
    public int getIndexInPage() {
        return indexInPage;
    }


    public Boolean isEqual(Ref ref) {
        if (this.page == (ref.page) && this.indexInPage == ref.indexInPage)
            return true;

        return false;
    }

//    public String toString() {
//        String s = "";
//        s += "PageName:" + this.getFileName() + "  RowIndex:" + this.getIndexInPage();
//        return s;
//    }
}