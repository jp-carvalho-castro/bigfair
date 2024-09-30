/* ----------
Author: João Pedro de Carvalho Castro
Contact: jp.carvalhocastro@alumni.usp.br
---------- */

package util;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class ReadingLogs {
            
    public static List<String> job(SparkSession spark, String day) throws Exception {
        
        //Starting log ArrayList
        
        List<String> log = new ArrayList<>();
        
        //Start message
        
        log.add("[reading_logs] Starting job.");
        System.out.println("[reading_logs] Starting job.");
        
        //Loading the CSV files from the source to Spark
        
        long jobTempoInicial = System.currentTimeMillis();
        
        Dataset<Row> logParaImprimir = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .load("hdfs:/(removed_for_anonymization)/logs/job_day_" + day);
       
        logParaImprimir.show((int)logParaImprimir.count(), false);
        
        //Finishing JOB
                
        long jobTempoFinal = System.currentTimeMillis();
        
        log.add("[reading_logs] Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        System.out.println("[reading_logs]Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        
        return log;
        
    }
    
}
