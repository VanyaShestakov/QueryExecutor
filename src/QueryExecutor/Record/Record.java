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
        return data.equals(record.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
