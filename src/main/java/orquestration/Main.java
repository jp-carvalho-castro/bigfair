/* ----------
Author: João Pedro de Carvalho Castro
Contact: jp.carvalhocastro@alumni.usp.br
---------- */

package orquestration;

import query.QueryingBigFAIR;
import etl.LoadingBigFAIR;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import util.ReadingLogs;

public class Main {
        
    public static void main(String[] args) throws Exception {
        
        //Creating the SparkSession that is going to be used in every job
        
        SparkSession spark = SparkSession.builder()
        .appName("BigFAIR")
        .getOrCreate();

        //Starting log ArrayList
        
        List<String> log = new ArrayList<>();
        
        //Including information of the time of execution in the log

        log.add("------------------------------------------------------");
        log.add("Execution in: " + Calendar.getInstance().getTime());
        log.add("------------------------------------------------------");

        System.out.println("------------------------------------------------------");
        System.out.println("Execution in: " + Calendar.getInstance().getTime());
        System.out.println("------------------------------------------------------");

        //Starting the job

        log.add("[orquestration] Iniciando job.");
        System.out.println("[orquestration] Iniciando job.");
        
        //Checking arguments to find out which job to execute 
        
        if (args[0].length() > 0) {

            //Goes through the arguments array to find out which jobs are to be executed
            
            for (String job : args) {

                if (job.contains("full_job")) {
                    
                    log.add("[orquestration] Selected job: " + job + ".");
                    System.out.println("[orquestration] Selected job: " + job + ".");
                    log.addAll(LoadingBigFAIR.job(spark));
                    log.addAll(QueryingBigFAIR.job(spark));

                } else if (job.contains("reading_logs")) {

                    log.add("[orquestration] Selected job: " + job + ".");
                    System.out.println("[orquestration] Selected job: " + job + ".");
                    log.addAll(ReadingLogs.job(spark, StringUtils.substringAfterLast(job, "reading_logs")));
                
                } else if (job.contains("loading_bigfair")) {
                    
                    log.add("[orquestration] Selected job: " + job + ".");
                    System.out.println("[orquestration] Selected job: " + job + ".");
                    log.addAll(LoadingBigFAIR.job(spark));

                } else if (job.contains("querying_bigfair")) {
                    
                    log.add("[orquestration] Selected job: " + job + ".");
                    System.out.println("[orquestration] Selected job: " + job + ".");
                    log.addAll(QueryingBigFAIR.job(spark));
                    
                } else {
                    
                    log.add("[orquestration] Job " + job + " not identified.");
                    System.out.println("[orquestration] Job " + job + " not identified.");
                    
                }
                
            }

        } else {

            log.add("[orquestration] Please specify the job as an argument.");
            System.out.println("[orquestration] Please specify the job as an argument.");

        }
        
        //End message
        
        log.add("[orquestration] Execution finished.");
        System.out.println("[orquestration] Execution finished.");
        
        //Recording a copy of the log in HDFS
        
        Dataset<Row> logDF = spark.createDataset(log, Encoders.STRING()).toDF();
        logDF.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save("(removed_for_anonymization)/logs/job_day_" + Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
        
        //Closing the SparkSession to free cluster resources
        
        spark.close();
 
    }
    
}
