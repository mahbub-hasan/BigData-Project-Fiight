package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyAllFlightAvgReducer extends Reducer<Text, Text, Text, DoubleWritable> {
    int sum = 0;
    int count = 0;
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

        for (Text text: values){
            String[] data = text.toString().split(" ");
            sum += Integer.parseInt(data[0]);
            count += Integer.parseInt(data[1]);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        System.out.println("inside cleanup function");
        double avg = (double) sum/count;
        context.write(new Text("Avg"), new DoubleWritable(avg));
    }
}
