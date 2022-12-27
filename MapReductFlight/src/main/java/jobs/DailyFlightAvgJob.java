package jobs;

import maps.DailyFlightAvgMap;
import maps.FlightCountJobMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.DailyFlightAvgReducer;
import reducers.FlightCountJobReducer;

public class DailyFlightAvgJob implements GenericJob{
    @Override
    public Job buildJob() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://master:9000");

            Job job = Job.getInstance(configuration, "DailyFlightAvgJob");

            job.setMapperClass(DailyFlightAvgMap.class);
            job.setReducerClass(DailyFlightAvgReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            Path inputPath = new Path("/user/mahbubhasan/project/output/map-reduce-1/part-r-00000");
            Path outputPath = new Path("/user/mahbubhasan/project/output/map-reduce-2");
            FileSystem fs = FileSystem.get(configuration);
            if(fs.exists(outputPath))
                fs.delete(outputPath, true);

            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            return job;
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }

        return null;
    }
}
