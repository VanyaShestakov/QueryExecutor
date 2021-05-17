package QueryExecutor.Record;

import QueryExecutor.Record.Exceptions.FieldNameDoesNotExistsException;

import java.util.HashMap;
import java.util.Map;

public class Record {
    private Map<String, Object> data;
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



}
