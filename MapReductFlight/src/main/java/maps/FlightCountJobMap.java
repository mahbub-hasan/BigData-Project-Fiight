package maps;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utilites.FileMapper;

import java.io.IOException;

/**
 *  Date, Airport, Count
 *  Day-1, BLR , 5
 */
public class FlightCountJobMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(";");
        if(data.length == FileMapper.TOTAL_LENGTH){
            if(!data[0].equals("Airline")){
                String date = data[FileMapper.DATA_OF_JURNEY];

                String route = data[FileMapper.ROUTE];
                String[] routeSplitter = route.split(" → ");
                for(String r : routeSplitter){
                    String outputKey = String.format("%s,%s", date, r);
                    context.write(new Text(outputKey),ONE);
                }

            }
        }
    }
}
