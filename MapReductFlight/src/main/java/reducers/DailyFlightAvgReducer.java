package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyFlightAvgReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (Text text: values){
            String[] data = text.toString().split(" ");
            sum = Integer.parseInt(data[0]);
            count = Integer.parseInt(data[1]);
        }

        double avg = (double) sum/count;

        context.write(key, new DoubleWritable(avg));
    }
}
