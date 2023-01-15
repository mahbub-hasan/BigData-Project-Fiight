package jobs;

import combiners.DailyFlightAvgCombiner;
import maps.DailyAllFlightAvgMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.DailyAllFlightAvgReducer;
import utilites.Commons;

public class DailyAllFlightAvgJob implements GenericJob{
    @Override
    public Job buildJob() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://master:9000");

            Job job = Job.getInstance(configuration, "DailyAllFlightAvgJob");

            job.setMapperClass(DailyAllFlightAvgMap.class);
            job.setCombinerClass(DailyFlightAvgCombiner.class);
            job.setReducerClass(DailyAllFlightAvgReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // job.setOutputKeyClass(Text.class);
            // job.setOutputValueClass(DoubleWritable.class);

            Path inputPath = new Path(Commons.DATA_INPUT_PROBLEM_3);
            Path outputPath = new Path(Commons.DATA_OUTPUT_PROBLEM_3);
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
