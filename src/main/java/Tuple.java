import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
    public Hashtable<String, Object> values;

    public Tuple(Hashtable<String, Object> values) {
        this.values = values;
    }

    public Hashtable getValues() {
        return values;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Object key : this.values.keySet()) {
            result.append(this.values.get(key) + ",");
        }
        result.setLength(result.length() - 1);
        return result.toString()+"\n";
    }
}
