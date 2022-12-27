package jobs;

import org.apache.hadoop.mapreduce.Job;

public interface GenericJob {
    public Job buildJob();
}
