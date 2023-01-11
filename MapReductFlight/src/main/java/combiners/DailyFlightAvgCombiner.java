package combiners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyFlightAvgCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (Text text: values){
            sum += Integer.parseInt(text.toString());
            count++;
        }

        context.write(key, new Text(sum+" "+count));
    }
}
