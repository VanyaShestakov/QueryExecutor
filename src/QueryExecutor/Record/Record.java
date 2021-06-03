package QueryExecutor.Record;

import QueryExecutor.Record.Exceptions.FieldNameDoesNotExistsException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Record {
    private final Map<String, Object> data;
    public Record () {
        data = new HashMap<>();
    }

    public Record (Map<String, Object> data) {
        this.data = data;
    }

    public boolean isEmpty() {
        return data.size() == 0;
    }

    public boolean contains(String fieldName) {
        return data.containsKey(fieldName);
    }

    public void addField(String fieldName, Object value) {
        data.put(fieldName, value);
    }

    public Object getValue(String fieldName) {
        if (data.containsKey(fieldName)) {
            return data.get(fieldName);
        } else {
            throw new FieldNameDoesNotExistsException("Record does not contain the field with name: " + fieldName);
        }
    }

    public Set<String> getKeySet() {
        return data.keySet();
    }

    public Map<String, Object> getMap() {
        return data;
    }

    public Set<String> getFieldNames() {
        return data.keySet();
    }

    public String toString () {
        return data.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Record record = (Record) o;
        Set<String> keys = getKeySet();
        for (String key: keys) {
            if (!record.contains(key)) {
                return false;
            }
            Object val = record.getValue(key);
            Object comparableVal = val instanceof java.sql.Date ? val.toString() : val;
            if (!data.get(key).equals(comparableVal)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 31;
        Set<String> keys = getKeySet();
        Object val;
        for (String key: keys) {
            val = data.get(key);
            val = val instanceof java.sql.Date ? val.toString() : val;
            hash *= val.hashCode();
        }
        return hash;
    }

    public static void main(String[] args) {
        Record rec = new Record();
        rec.addField("q", 1);
        rec.addField("r", null);

        Record rec1 = new Record();
        rec1.addField("r", null);
        rec1.addField("q", 1);
        System.out.println(rec.equals(rec1));
    }
}
