package Tools;

import QueryExecutor.Recordable;

public class TestDBRecord implements Recordable {
    public int id;
    public String name;
    public double temperature;
    public String date;

    public TestDBRecord(int id, String name, double temperature, String date) {
        this.id = id;
        this.name = name;
        this.temperature = temperature;
        this.date = date;
    }
}
