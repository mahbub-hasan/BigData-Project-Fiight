package app;

import jobs.CleanDatasetJob;
import jobs.DailyAllFlightAvgJob;
import jobs.DailyFlightAvgJob;
import jobs.FlightCountJob;
import maps.FlightCountJobMap;
import org.apache.hadoop.mapreduce.Job;

import java.util.Scanner;

public class MapReduceApp {
    public static void main(String[] args) {
        try {
            Scanner scanner = new Scanner(System.in);


            while (true){
                System.out.println("1: Compute the clean of dataset");
                System.out.println("2: Compute the count of Flight that arrive, dept or stop");
                System.out.println("3: Compute the daily avg of Flight that arrive, dept or stop");
                System.out.println("4: Compute the daily avg of all flight");
                try {

                    int choice = Integer.parseInt(scanner.nextLine());
                    switch (choice){
                        case 1:
                            cleanDataset();
                            break;
                        case 2:
                            countOfFlight();
                            break;
                        case 3:
                            dailyAvgOfFlight();
                            break;
                        case 4:
                            dailyAvgOfAllFlight();
                            break;
                        case -1:
                            scanner.close();
                            return;
                        default:
                            break;
                    }
                }catch (Exception ex){
                    System.err.println(ex.getMessage());
                }
            }
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }

    }

    private static void cleanDataset(){
        try {
            Job job = new CleanDatasetJob().buildJob();
            job.setJarByClass(MapReduceApp.class);
            job.waitForCompletion(true);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
    }

    private static void dailyAvgOfAllFlight() {
        try {
            Job job = new DailyAllFlightAvgJob().buildJob();
            job.setJarByClass(MapReduceApp.class);
            job.waitForCompletion(true);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
    }

    private static void dailyAvgOfFlight() {
        try {
            Job job = new DailyFlightAvgJob().buildJob();
            job.setJarByClass(MapReduceApp.class);
            job.waitForCompletion(true);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
    }

    private static void countOfFlight() {
        try {
            Job job = new FlightCountJob().buildJob();
            job.setJarByClass(MapReduceApp.class);
            job.waitForCompletion(true);
        }catch (Exception ex){
            System.err.println(ex.getMessage());
        }
    }
}
