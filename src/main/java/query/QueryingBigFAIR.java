/* ----------
Author: João Pedro de Carvalho Castro
Contact: jp.carvalhocastro@alumni.usp.br
---------- */

package query;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class QueryingBigFAIR {
            
    public static List<String> job(SparkSession spark) throws Exception {
    
        //Starting log ArrayList
        
        List<String> log = new ArrayList<>();
        
        //Start message
        
        log.add("[querying_bigfair] Starting job.");
        System.out.println(log.get(0));
                
        //Storing start time for later calculation of execution time
        
        long jobTempoInicial = System.currentTimeMillis();
                
        //Reading the necessary metadata files from HDFS to Spark 
        
        Dataset<Row> dimDataProvider = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/dw/dim_dataprovider");
        Dataset<Row> dimDataRepository = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/dw/dim_datarepository");
        Dataset<Row> dimDate = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/dw/dim_date");
        Dataset<Row> dimDataCell = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/dw/dim_datacell");
        Dataset<Row> factTable = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/dw/fact_table");
        Dataset<Row> cdcOutput = spark.read().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").load("(removed_for_anonymization)/cdc/datalake/cdc_meta_final");
        
        log.add("[querying_bigfair] Done reading metadata from HDFS to Spark.");
        System.out.println("[querying_bigfair] Done reading metadata from HDFS to Spark.");

        //Creating temporary views
        
        dimDataProvider.createOrReplaceTempView("DIM_DATAPROVIDER");
        dimDataRepository.createOrReplaceTempView("DIM_DATAREPOSITORY");
        dimDate.createOrReplaceTempView("DIM_DATE");
        dimDataCell.createOrReplaceTempView("DIM_DATACELL");
        factTable.createOrReplaceTempView("FACT");
        cdcOutput.createOrReplaceTempView("CDC_META");
        
        log.add("[querying_bigfair] Temporary views created for the DW and DL.");
        System.out.println("[querying_bigfair] Temporary views created for the DW and DL.");
        
        // Queries

        // FAPESP METADATA ARE IN THE METADATA WAREHOUSE, CDC METADATA ARE IN THE METADATA LAKE

        // METADATA RETRIEVAL

        // --Query 1-- Metadata Warehouse + Lake Retrieval
        // Analyzing the evolution of data size in the repository over time, grouped by data provider country.

        log.add("[querying_bigfair] Metadata Retrieval 1: Metadata Warehouse + Lake Retrieval.");
        System.out.println("[querying_bigfair] Metadata Retrieval 1: Metadata Warehouse + Lake Retrieval.");

        Dataset<Row> consulta = spark.sql(
                
                "SELECT year, month, country, " +
                "SUM(size) AS size " +
                "FROM (SELECT DIM_DATE.year, " +
                             "DIM_DATE.month, " +
                             "DIM_DATAPROVIDER.country, " +
                             "FACT.size " +
                      "FROM FACT " +
                      "INNER JOIN DIM_DATE ON (FACT.sk_date = DIM_DATE.sk_date) " +
                      "INNER JOIN DIM_DATAPROVIDER ON (FACT.sk_dataprovider = DIM_DATAPROVIDER.sk_dataprovider) " +
                      "UNION ALL " +
                      "SELECT CDC_META.year, " +
                             "CDC_META.month, " +
                             "CDC_META.provider_country AS country, " +
                             "CDC_META.size " +
                      "FROM CDC_META) " +
                "GROUP BY year, month, country " +
                "ORDER BY SUM(size) DESC"
        
        );

        consulta.show(1000, false);

        consulta.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/results/query1.csv");

        // SOURCE DATA RETRIEVAL > 3 queries

        // --Query 2-- Metadata Warehouse Retrieval
        // Analyzing the average numeric results of leukocytes exams in patients from the BPSP provider, grouped by patient birth year.

        //Querying connection information from data provider

        log.add("[querying_bigfair] Source Retrieval 1: Metadata Warehouse Retrieval.");
        System.out.println("[querying_bigfair] Source Retrieval 1: Metadata Warehouse Retrieval.");

        Dataset<Row> conexaoBPSP = spark.sql(
                
                "SELECT DISTINCT "
                        
                    + "DIM_DATAREPOSITORY.connection_string "
                        
                + "FROM "
                        
                    + "FACT "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                    + "ON (FACT.sk_datarepository = DIM_DATAREPOSITORY.sk_datarepository) "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                    + "ON (FACT.sk_dataprovider = DIM_DATAPROVIDER.sk_dataprovider) "
                        
                + "WHERE DIM_DATAPROVIDER.name = 'BPSP'"
        
        );
        
        conexaoBPSP.show(1000, false);

        //Mapping connection information to strings
        
        String bpspConnectionString = conexaoBPSP
                .select(conexaoBPSP.col("connection_string"))
                .toJavaRDD().map((Row r) -> {return r.getString(0);}).collect().get(0);

        //Loading source data in spark dataframes

        Dataset<Row> bpspPatients = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", "|")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load(bpspConnectionString + "/bpsp_pacientes_01.csv");
        
        Dataset<Row> bpspExaminations = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", "|")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load(bpspConnectionString + "/bpsp_exames_01.csv");        

        //Creating temporary views for the dataframes

        bpspPatients.createOrReplaceTempView("BPSP_PATIENTS");
        bpspExaminations.createOrReplaceTempView("BPSP_EXAMINATIONS");

        //Query execution
        
        consulta = spark.sql(
                
                "SELECT "
                        
                    + "BPSP_PATIENTS.aa_nascimento AS aa_nascimento, "
                    + "AVG(DOUBLE(BPSP_EXAMINATIONS.de_resultado)) AS average_results "
                        
                + "FROM "
                        
                    + "BPSP_PATIENTS "
                        
                    + "INNER JOIN BPSP_EXAMINATIONS "
                    + "ON (BPSP_PATIENTS.id_paciente = BPSP_EXAMINATIONS.id_paciente) "
                        
                + "WHERE BPSP_EXAMINATIONS.de_analito IN ('Leucócitos', 'Leucocitos') "
                        
                + "GROUP BY BPSP_PATIENTS.aa_nascimento "
                       
                + "ORDER BY AVG(BPSP_EXAMINATIONS.de_resultado) DESC"
        
        );

        consulta.show(1000, false);

        consulta.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/results/query2.csv");

        // --Query 3-- Metadata Lake Retrieval
        // Analyzing the amount of COVID-19 related deaths from the CDC provider over time, grouped by patient race and ethnicity.

        log.add("[querying_bigfair] Source Retrieval 2: Metadata Lake Retrieval.");
        System.out.println("[querying_bigfair] Source Retrieval 2: Metadata Lake Retrieval.");

        //Querying connection information from data provider

        Dataset<Row> conexaoCDC = spark.sql (

                "SELECT DISTINCT "

                    + "CDC_META.connection_string "

                + "FROM CDC_META "

                + "WHERE CDC_META.provider_name = 'CDC'"        

        );

        conexaoCDC.show();

        //Mapping connection information to strings
        
        String cdcConnectionString = conexaoCDC
                .select(conexaoCDC.col("connection_string"))
                .toJavaRDD().map((Row r) -> {return r.getString(0);}).collect().get(0);

        //Loading source data in spark dataframes

        Dataset<Row> cdcDataset = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", ",")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load(cdcConnectionString + "/COVID-19_Case_Surveillance_Public_Use_Data.csv");

        //Creating temporary views for the dataframes

        cdcDataset.createOrReplaceTempView("CDC");

        //Query execution

        consulta = spark.sql(

                "SELECT "
 
                    + "CASE WHEN SUBSTRING(`cdc_case_earliest_dt `, 6, 2) BETWEEN '01' AND '06' "
                              + "THEN SUBSTRING(`cdc_case_earliest_dt `, 0, 4) || '/' || '1' "
                         + "WHEN SUBSTRING(`cdc_case_earliest_dt `, 6, 2) BETWEEN '07' AND '12' "
                              + "THEN SUBSTRING(`cdc_case_earliest_dt `, 0, 4) || '/' || '2' "
                    + "END AS year_semester, "
                    + "race_ethnicity_combined AS race_and_ethnicity, "
                    + "COUNT(*) AS amount "
                
                + "FROM CDC "
 
                + "WHERE current_status = 'Laboratory-confirmed case' "
                + "AND death_yn = 'Yes' "
 
                + "GROUP BY year_semester, CDC.race_ethnicity_combined "
                
                + "ORDER BY year_semester ASC"

        );

        consulta.show(1000, false);

        consulta.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/results/query3.csv");

        // --Query 4-- Metadata Warehouse + Lake Retrieval
        // Analyzing the amount of patients from all data providers that were hospitalized over time, aggregated by their sex.

        log.add("[querying_bigfair] Source Retrieval 3: Metadata Warehouse + Lake Retrieval.");
        System.out.println("[querying_bigfair] Source Retrieval 3: Metadata Warehouse + Lake Retrieval.");

        //Loading source data not previously loaded
        //Skipping step to retrieve connection information since previous queries already prove its effectiveness

        Dataset<Row> bpspDesfechos = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", "|")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:/(removed_for_anonymization)/fapesp/origem/bpsp_desfecho_01.csv");

        bpspDesfechos.createOrReplaceTempView("BPSP_OUTCOMES");

        Dataset<Row> hslDesfechos = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", "|")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:/(removed_for_anonymization)/fapesp/origem/HSL_Desfechos_4.csv");

        hslDesfechos.createOrReplaceTempView("SL_OUTCOMES");

        Dataset<Row> hslPacientes = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("delimiter", "|")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:/(removed_for_anonymization)/fapesp/origem/HSL_Pacientes_4.csv");

        hslPacientes.createOrReplaceTempView("SL_PATIENTS");
                
        //Query execution 

        consulta = spark.sql(

                "SELECT "

                    + "TB_RESULT.year_month, "
                    + "TB_RESULT.patient_sex, "
                    + "COUNT(DISTINCT TB_RESULT.patient_id) "

                + "FROM "

                    //CDC Provider
                    + "(SELECT "
                        + "SUBSTRING(`cdc_case_earliest_dt `, 0, 7) AS year_month, "
                        + "CDC.sex AS patient_sex, "
                        + "MONOTONICALLY_INCREASING_ID() AS patient_id "
                    + "FROM CDC "
                    + "WHERE CDC.hosp_yn = 'Yes' "

                    + "UNION ALL "

                    //BPSP Provider
                    + "SELECT "
                        + "CASE WHEN SUBSTRING(BPSP_OUTCOMES.dt_atendimento, 6, 1) = '/' THEN "
                                + "SUBSTRING(BPSP_OUTCOMES.dt_atendimento, 7, 4) || '/' "
                                    + "|| SUBSTRING(BPSP_OUTCOMES.dt_atendimento, 4, 2) "
                            + "ELSE SUBSTRING(BPSP_OUTCOMES.dt_atendimento, 6, 4) || '/' "
                                + "|| SUBSTRING(BPSP_OUTCOMES.dt_atendimento, 3, 2) "
                        + "END AS year_month, "
                        + "CASE WHEN BPSP_PATIENTS.ic_sexo = 'M' THEN 'Male' "
                             + "WHEN BPSP_PATIENTS.ic_sexo = 'F' THEN 'Female' "
                             + "ELSE 'Unknown' "
                        + "END AS patient_sex, "
                        + "BPSP_PATIENTS.id_paciente AS patient_id "
                    + "FROM BPSP_PATIENTS "
                    + "INNER JOIN BPSP_OUTCOMES "
                    + "ON BPSP_PATIENTS.id_paciente = BPSP_OUTCOMES.id_paciente "
                    + "WHERE BPSP_OUTCOMES.de_tipo_atendimento = 'Internado'"

                    + "UNION ALL "

                    //SL Provider
                    + "SELECT "
                        + "CASE WHEN SUBSTRING(SL_OUTCOMES.dt_atendimento, 6, 1) = '/' THEN "
                                + "SUBSTRING(SL_OUTCOMES.dt_atendimento, 7, 4) || '/' "
                                    + "|| SUBSTRING(SL_OUTCOMES.dt_atendimento, 4, 2) "
                            + "ELSE SUBSTRING(SL_OUTCOMES.dt_atendimento, 6, 4) || '/' "
                                + "|| SUBSTRING(SL_OUTCOMES.dt_atendimento, 3, 2) "
                        + "END AS year_month, "
                        + "CASE WHEN SL_PATIENTS.ic_sexo = 'M' THEN 'Male' "
                             + "WHEN SL_PATIENTS.ic_sexo = 'F' THEN 'Female' "
                             + "ELSE 'Unknown' "
                        + "END AS patient_sex, "
                        + "SL_PATIENTS.id_paciente AS patient_id "
                    + "FROM SL_PATIENTS "
                    + "INNER JOIN SL_OUTCOMES "
                    + "ON SL_PATIENTS.id_paciente = SL_OUTCOMES.id_paciente "
                    + "WHERE SL_OUTCOMES.de_tipo_atendimento = 'Internado') TB_RESULT "

                + "GROUP BY TB_RESULT.year_month, TB_RESULT.patient_sex "
 
                + "ORDER BY TB_RESULT.year_month ASC"
        );

        consulta.show(1000, false);

        consulta.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/results/query4.csv");

        // METADATA + SOURCE DATA RETRIEVAL

        // --Query 5-- Metadata Warehouse + Lake Retrieval
        // Analyzing the data size occupied by patients that were hospitalized, grouped by data provider country and name. 

        log.add("[querying_bigfair] Meta+Source Retrieval 1: Metadata Warehouse + Lake Retrieval.");
        System.out.println("[querying_bigfair] Meta+Source Retrieval 1: Metadata Warehouse + Lake Retrieval.");

        Dataset<Row> cdc_tratado = spark.sql(

                "SELECT "
                    + "*, "
                    + "SHA1"
                        + "("
                            + "NVL(`cdc_case_earliest_dt `, '') || ',' || "
                            + "NVL(cdc_report_dt, '') || ',' || "
                            + "NVL(pos_spec_dt, '') || ',' || "
                            + "NVL(onset_dt, '') || ',' || "
                            + "NVL(current_status, '') || ',' || "
                            + "NVL(sex, '') || ',' || "
                            + "NVL(age_group, '') || ',' || "
                            + "NVL(race_ethnicity_combined, '') || ',' || "
                            + "NVL(hosp_yn, '') || ',' || "
                            + "NVL(icu_yn, '') || ',' || "
                            + "NVL(death_yn, '') || ',' || "
                            + "NVL(medcond_yn, '') || "
                            + "\"\\n\" || "
                            + "ROW_NUMBER() OVER (ORDER BY MONOTONICALLY_INCREASING_ID())"
                        + ") "
                    + "AS data_instance_unique_source_id_01 "

                + "FROM CDC"

        );
        
        cdc_tratado.createOrReplaceTempView("CDC_TRATADO");

        spark.catalog().cacheTable("CDC_TRATADO");

        consulta = spark.sql(

                "SELECT "
 
                    + "TB_RESULT.country, "
                    + "TB_RESULT.name, "
                    + "SUM(TB_RESULT.size) AS size "
 
                + "FROM "

                    //Data providers on MetaDW

                    + "(SELECT "

                        + "DIM_DATAPROVIDER.name, "
                        + "DIM_DATAPROVIDER.country, "
                        + "FACT.size "

                    + "FROM FACT "

                    + "INNER JOIN DIM_DATAPROVIDER "
                    + "ON (FACT.sk_dataprovider = DIM_DATAPROVIDER.sk_dataprovider) "

                    + "INNER JOIN DIM_DATACELL "
                    + "ON (FACT.sk_datacell = DIM_DATACELL.sk_datacell) "

                    + "INNER JOIN "

                    + "("

                            + "SELECT DISTINCT "
                                + "BPSP_OUTCOMES.id_paciente AS id_paciente, "
                                + "BPSP_OUTCOMES.id_atendimento AS id_atendimento, "
                                + "'bpsp_desfecho_01.csv' AS data_object_title "
                            + "FROM BPSP_OUTCOMES "
                            + "WHERE BPSP_OUTCOMES.de_tipo_atendimento = 'Internado' "

                            + "UNION ALL "

                            + "SELECT DISTINCT "
                                + "SL_OUTCOMES.id_paciente AS id_paciente, "
                                + "SL_OUTCOMES.id_atendimento AS id_atendimento, "
                                + "'HSL_Desfechos_4.csv' AS data_object_title "
                            + "FROM SL_OUTCOMES "
                            + "WHERE SL_OUTCOMES.de_tipo_atendimento = 'Internado'"

                    + ") OUTCOMES "

                    + "ON (OUTCOMES.id_paciente = DIM_DATACELL.data_instance_unique_source_id_01 "
                    + "AND OUTCOMES.id_atendimento = DIM_DATACELL.data_instance_unique_source_id_02 "
                    + "AND OUTCOMES.data_object_title = DIM_DATACELL.data_object_title) "

                    + "UNION ALL "

                    //Data providers on MetaDL 

                    +"SELECT "
                        + "CDC_META.provider_name AS name, "
                        + "CDC_META.provider_country AS country, "
                        + "CDC_META.size "

                    + "FROM CDC_TRATADO "

                    + "INNER JOIN CDC_META "
                    + "ON (CDC_META.data_instance_unique_source_id_01 = "
                        + "CDC_TRATADO.data_instance_unique_source_id_01) "

                    + "WHERE CDC_TRATADO.hosp_yn = 'Yes') TB_RESULT "
                
                + "GROUP BY TB_RESULT.country, TB_RESULT.name "
 
                + "ORDER BY SUM(TB_RESULT.size) DESC"

        );

        consulta.show(1000, false);

        consulta.coalesce(1).write().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/results/query5.csv");

        //Finishing JOB
                
        long jobTempoFinal = System.currentTimeMillis();
        
        log.add("[querying_bigfair] Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        System.out.println("[querying_bigfair] Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        
        return log;
        
    }
    
}
