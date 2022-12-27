package jobs;

import maps.FlightCountJobMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.FlightCountJobReducer;

public class FlightCountJob implements GenericJob{

    @Override
    public Job buildJob() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://master:9000");

            Job job = Job.getInstance(configuration, "FlightCountJob");

            job.setMapperClass(FlightCountJobMap.class);
            job.setReducerClass(FlightCountJobReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            Path inputPath = new Path("/user/bigdata/project/input/DataSet_Train.csv");
            Path outputPath = new Path("/user/mahbubhasan/project/output/map-reduce-1");
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
