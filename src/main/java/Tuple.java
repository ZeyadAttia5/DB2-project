import java.io.Serializable;
import java.util.Vector;

public class Tuple implements Serializable {
    public Vector<String> values;


    public Tuple(Object... args) {
        this.values = new Vector<String>();
        for (Object arg : args) {
            this.values.add(String.valueOf(arg));
        }
    }

    public Vector<String> getValues() {
        return values;
    }

    public String toString() {
        String result = "";
        for (int i = 0; i < values.size(); i++) {
            result += values.get(i);
            if (i < values.size() - 1) {
                result += ", ";
            }
        }
        return result;
    }



/*  "testing"
    public static void main(String[] args) {
        Tuple tuple = new Tuple(1, "Hello", 3.14);
        Vector<String > tupleValues = tuple.getValues();
        System.out.println(tuple);
    }
*/
}
