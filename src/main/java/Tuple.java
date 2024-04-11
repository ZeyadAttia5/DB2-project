import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    public Hashtable<String, Object> values;

    public Tuple(Hashtable<String, Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Object key : this.values.keySet()) {
            result.append(this.values.get(key) + ",");
        }
        result.setLength(result.length() - 1);
        return result.toString();
    }

    public Hashtable getValues() {
        return values;
    }


//    public static void main(String[] args) {
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", new Integer(2343432));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        Tuple t = new Tuple(htblColNameValue);
//        System.out.println(t.values.get("id").getClass().toString().equals(" java.lang.Integer"));
//        Object x = (Object) t.values.get("id");
//        System.out.println((int)x==2343432);

//    }
}
