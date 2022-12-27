package maps;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DailyAllFlightAvgMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");
        if(data.length == 2){
            int outputValue = Integer.parseInt(data[1]);

            context.write(new Text("1"), new IntWritable(outputValue));
        }
    }
}
