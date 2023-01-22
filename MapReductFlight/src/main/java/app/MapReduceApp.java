package app;

import jobs.CleanDatasetJob;
import jobs.DailyAllFlightAvgJob;
import jobs.DailyFlightAvgJob;
import jobs.FlightCountJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class MapReduceApp {
    public static void main(String[] args) {
        try {
            Job cleanDataJob = new CleanDatasetJob().buildJob();
            Job flightCountJob = new FlightCountJob().buildJob();
            Job DailyFlightAvgJob = new DailyFlightAvgJob().buildJob();
            Job DailyAllFlightAvgJob = new DailyAllFlightAvgJob().buildJob();

            cleanDataJob.setJarByClass(MapReduceApp.class);
            flightCountJob.setJarByClass(MapReduceApp.class);
            DailyFlightAvgJob.setJarByClass(MapReduceApp.class);
            DailyAllFlightAvgJob.setJarByClass(MapReduceApp.class);

            ControlledJob job1 = new ControlledJob(cleanDataJob.getConfiguration());
            ControlledJob job2 = new ControlledJob(flightCountJob.getConfiguration());
            ControlledJob job3 = new ControlledJob(DailyFlightAvgJob.getConfiguration());
            ControlledJob job4 = new ControlledJob(DailyAllFlightAvgJob.getConfiguration());

            JobControl jobControl = new JobControl("Flight Price MapReduce Jobs");
            jobControl.addJob(job1);
            jobControl.addJob(job2);
            jobControl.addJob(job3);
            jobControl.addJob(job4);

            job2.addDependingJob(job1);
            job3.addDependingJob(job2);
            job4.addDependingJob(job2);

            jobControl.run();

            while (!jobControl.allFinished()){
                try{
                    Thread.sleep(5000);
                }catch (Exception ex){
                    System.err.println(ex.getMessage());
                }
            }

            jobControl.stop();
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
    }
}
