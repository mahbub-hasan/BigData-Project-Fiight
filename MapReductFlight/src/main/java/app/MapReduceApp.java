package app;

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
                System.out.println("1: Compute the count of Flight that arrive, dept or stop");
                System.out.println("2: Compute the daily avg of Flight that arrive, dept or stop");
                System.out.println("3: Compute the daily avg of all flight");
                try {

                    int choice = Integer.parseInt(scanner.nextLine());
                    switch (choice){
                        case 1:
                            countOfFlight();
                            break;
                        case 2:
                            dailyAvgOfFlight();
                            break;
                        case 3:
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

    private static void dailyAvgOfAllFlight() {

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
