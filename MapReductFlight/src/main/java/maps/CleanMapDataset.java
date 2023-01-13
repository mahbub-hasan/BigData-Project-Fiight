package maps;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilites.FileMapper;

import java.io.IOException;

public class CleanMapDataset extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(";");
        if(data.length == FileMapper.TOTAL_LENGTH){
            if(!data[0].equals("Airline")){
                String st=value.toString();
                context.write(new Text(st),new Text(""));
            }
        }
    }
}
