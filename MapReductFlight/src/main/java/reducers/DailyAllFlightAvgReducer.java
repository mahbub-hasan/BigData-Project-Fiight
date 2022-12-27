package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyAllFlightAvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable iw: values){
            sum += iw.get();
            count++;
        }

        double avg = (double) sum/count;

        context.write(new Text("Avg"), new DoubleWritable(avg));
    }
}
