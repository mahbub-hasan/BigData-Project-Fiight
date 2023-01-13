package utilities;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;

import java.util.List;

public interface DataTransfer {
    void setBroadcast(Broadcast<List<Row>> broadcast);
}
