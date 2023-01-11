package jobs;

import maps.CleanMapDataset;
import maps.FlightCountJobMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.CleanReduceDataset;
import reducers.FlightCountJobReducer;
import utilites.Commons;

public class CleanDatasetJob implements GenericJob{

    @Override
    public Job buildJob() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://master:9000");

            Job job = Job.getInstance(configuration, "CleanDatasetJob");

            job.setMapperClass(CleanMapDataset.class);
            job.setCombinerClass(CleanReduceDataset.class);
            job.setReducerClass(CleanReduceDataset.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path inputPath = new Path(Commons.DATA_INPUT_CLEANING_PROCESS);
            Path outputPath = new Path(Commons.DATA_OUTPUT_CLEANING_PROCESS);

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
