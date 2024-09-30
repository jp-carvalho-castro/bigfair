/* ----------
Author: João Pedro de Carvalho Castro
Contact: jp.carvalhocastro@alumni.usp.br
---------- */

package etl;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;


public class LoadingBigFAIR {
            
    public static List<String> job(SparkSession spark) throws Exception {
        
        //Starting log ArrayList
        
        List<String> log = new ArrayList<>();
        
        //Start message
        
        log.add("[loading_bigfair] Starting job.");
        System.out.println("[loading_bigfair] Starting job.");
        
        //Loading the CSV files from the data lake to Spark
        
        long jobTempoInicial = System.currentTimeMillis();
        
        //BPSP

        Dataset<Row> bpspOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/bpsp_meta-001.csv");
        
        bpspOutput.show();

        Dataset<Row> bpspOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/bpsp_desc.csv");
        
        bpspOutputDesc.show();
        
        //HC

        Dataset<Row> hcOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/hc_meta-002.csv");
        
        hcOutput.show();

        Dataset<Row> hcOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/hc_desc.csv");
        
        hcOutputDesc.show();

        //HSL
        
        Dataset<Row> hslOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/hsl_meta-003.csv");
        
        hslOutput.show();

        Dataset<Row> hslOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/hsl_desc.csv");
        
        hslOutputDesc.show();

        //EINSTEIN
        
        Dataset<Row> einsteinOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/einstein_meta-005.csv");
        
        einsteinOutput.show();

        Dataset<Row> einsteinOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/einstein_desc.csv");
        
        einsteinOutputDesc.show();

        //FLEURY
        
        Dataset<Row> fleuryOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/fleury_meta-004.csv");
                
        fleuryOutput.show();

        Dataset<Row> fleuryOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/fapesp/datalake/fleury_desc.csv");
                
        fleuryOutputDesc.show();

        //CDC

        Dataset<Row> cdcOutput = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/cdc/datalake/cdc_meta-001.csv");
                
        cdcOutput.show();

        Dataset<Row> cdcOutputDesc = spark.read().format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
                .option("header", "true")
                .option("wholeFile", "true")
                .option("multiline", "true")
                .option("inferSchema", "true")
                .load("hdfs:(removed_for_anonymization)/cdc/datalake/cdc_desc.csv");
                
        cdcOutputDesc.show();
        
        log.add("[loading_bigfair] Arquivos CSV das ETLs lidos com sucesso.");
        System.out.println("[loading_bigfair] Arquivos CSV das ETLs lidos com sucesso.");      
                
        //Creating temporary views for each CSV
        
        bpspOutput.createOrReplaceTempView("BPSP");
        bpspOutputDesc.createOrReplaceTempView("BPSP_DESC");
        hcOutput.createOrReplaceTempView("USP");
        hcOutputDesc.createOrReplaceTempView("USP_DESC");
        hslOutput.createOrReplaceTempView("SL");
        hslOutputDesc.createOrReplaceTempView("SL_DESC");
        einsteinOutput.createOrReplaceTempView("AE");
        einsteinOutputDesc.createOrReplaceTempView("AE_DESC");
        fleuryOutput.createOrReplaceTempView("FG");
        fleuryOutputDesc.createOrReplaceTempView("FG_DESC");
        cdcOutput.createOrReplaceTempView("CDC");
        cdcOutputDesc.createOrReplaceTempView("CDC_DESC");
        
        log.add("[loading_bigfair] Temporary views successfully created.");
        System.out.println("[loading_bigfair] Temporary views successfully created.");
        
        //Enriching CDC metadata

        Dataset<Row> cdcFinal = spark.sql
                ( "SELECT "
                
                    + "*, "
                    + "'CDC' AS provider_name, "
                    + "'Centers for Disease Control and Prevention' AS provider_description, "
                    + "'National Organization' AS provider_type, "
                    + "'Washington' AS provider_city, "
                    + "'District of Columbia' AS provider_state, "
                    + "'United States of America' AS provider_country, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/cdc/origem' AS connection_string, "
                    + "'' AS connection_username, "
                    + "'' AS connection_password "
                
                + "FROM CDC"

                );

        cdcFinal.show();
        
        cdcFinal.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/cdc/datalake/cdc_meta_final");

        cdcFinal.createOrReplaceTempView("CDC");

        //Creating DUAL dataframe
        
        List<String> dual = new ArrayList<>();
        dual.add("");
        Dataset<Row> dualDF = spark.createDataset(dual, Encoders.STRING()).toDF();
        dualDF.createOrReplaceTempView("DUAL");
        
        log.add("[loading_bigfair] DUAL dataframe successfully created.");
        System.out.println("[loading_bigfair] DUAL dataframe successfully created.");
        
        //Loading LICENSE dimension
        
        Dataset<Row> dimLicense = spark.sql
                ( "SELECT "
                
                    + "1 AS sk_license, "
                    + "'CC-BY' AS type, "
                    + "'Creative Commons Brasil 2.0' AS description "
                
                + "FROM DUAL"
                        
                );
        
        dimLicense.show();
        
        dimLicense.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_license");
        
        log.add("[loading_bigfair] Dimension LICENSE successfully created.");
        System.out.println("[loading_bigfair] Dimension LICENSE successfully created.");
        
        //Loading PERMISSIONS dimension
        
        Dataset<Row> dimPermissions = spark.sql
                ( "SELECT "
                
                    + "1 AS sk_permissions, "
                    + "'Admin' AS required_access_role, "
                    + "'False' AS anonymization_required "
                
                + "FROM DUAL"
                        
                );
        
        dimPermissions.show();
        
        dimPermissions.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_permissions");
        
        log.add("[loading_bigfair] Dimension PERMISSIONS successfully created.");
        System.out.println("[loading_bigfair] Dimension PERMISSIONS successfully created.");
        
        //Loading STATUS dimension
        
        Dataset<Row> dimStatus = spark.sql
                ( "SELECT "
                
                    + "1 AS sk_status, "
                    + "'Alive' AS source_status "
                
                + "FROM DUAL"
                        
                );
        
        dimStatus.show();
        
        dimStatus.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_status");
        
        log.add("[loading_bigfair] Dimension STATUS successfully created.");
        System.out.println("[loading_bigfair] Dimension STATUS successfully created.");
        
        //Loading DATAPROVIDER dimension
        
        Dataset<Row> dimDataProvider = spark.sql
                ( "SELECT "
                
                    + "1 AS sk_dataprovider, "
                    + "'BPSP' AS name, "
                    + "'Beneficência Portuguesa de São Paulo' AS description, "
                    + "'Hospital' AS type, "
                    + "'São Paulo' AS city, "
                    + "'São Paulo' AS state, "
                    + "'Brazil' AS country "

                + "FROM DUAL "
                    
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "2 AS sk_dataprovider, "
                    + "'USP' AS name, "
                    + "'University of São Paulo Clinics Hospital' AS description, "
                    + "'Hospital' AS type, "
                    + "'São Paulo' AS city, "
                    + "'São Paulo' AS state, "
                    + "'Brazil' AS country "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "3 AS sk_dataprovider, "
                    + "'SL' AS name, "
                    + "'Syrian-Lebanese Hospital' AS description, "
                    + "'Hospital' AS type, "
                    + "'São Paulo' AS city, "
                    + "'São Paulo' AS state, "
                    + "'Brazil' AS country "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "4 AS sk_dataprovider, "
                    + "'AE' AS name, "
                    + "'Albert Einstein Hospital' AS description, "
                    + "'Hospital' AS type, "
                    + "'São Paulo' AS city, "
                    + "'São Paulo' AS state, "
                    + "'Brazil' AS country "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "5 AS sk_dataprovider, "
                    + "'FG' AS name, "
                    + "'Fleury Group' AS description, "
                    + "'Laboratory' AS type, "
                    + "'São Paulo' AS city, "
                    + "'São Paulo' AS state, "
                    + "'Brazil' AS country "

                + "FROM DUAL "

                + "UNION ALL "
                        
                + "SELECT "
                
                    + "6 AS sk_dataprovider, "
                    + "'CDC' AS name, "
                    + "'Centers for Disease Control and Prevention' AS description, "
                    + "'National Organization' AS type, "
                    + "'Washington' AS city, "
                    + "'District of Columbia' AS state, "
                    + "'United States of America' AS country "

                + "FROM DUAL "
                        
                );
        
        dimDataProvider.show();
        
        dimDataProvider.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_dataprovider");
        
        log.add("[loading_bigfair] Dimension DATAPROVIDER successfully created.");
        System.out.println("[loading_bigfair] Dimension DATAPROVIDER successfully created.");
        
        //Loading DATAREPOSITORY dimension
        
        Dataset<Row> dimDataRepository = spark.sql
                ( "SELECT "
                
                    + "1 AS sk_datarepository, "
                    + "'BPSP' AS name, "
                    + "'Beneficência Portuguesa de São Paulo' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/fapesp/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "
                    
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "2 AS sk_datarepository, "
                    + "'USP' AS name, "
                    + "'University of São Paulo Clinics Hospital' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/fapesp/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "3 AS sk_datarepository, "
                    + "'SL' AS name, "
                    + "'Syrian-Lebanese Hospital' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/fapesp/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "4 AS sk_datarepository, "
                    + "'AE' AS name, "
                    + "'Albert Einstein Hospital' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/fapesp/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "
                        
                + "UNION ALL "
                        
                + "SELECT "
                
                    + "5 AS sk_datarepository, "
                    + "'FG' AS name, "
                    + "'Fleury Group' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/fapesp/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "

                + "UNION ALL "
                        
                + "SELECT "
                
                    + "6 AS sk_datarepository, "
                    + "'CDC' AS name, "
                    + "'Centers for Disease Control and Prevention' AS description, "
                    + "'hdfs' AS storage_type, "
                    + "'hdfs:(removed_for_anonymization)/cdc/origem' AS connection_string, "
                    + "'(removed_for_anonymization)' AS connection_username, "
                    + "'(removed_for_anonymization)' AS connection_password "

                + "FROM DUAL "
                        
                );
        
        dimDataRepository.show();
        
        dimDataRepository.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_datarepository");
        
        log.add("[loading_bigfair] Dimension DATAREPOSITORY successfully created.");
        System.out.println("[loading_bigfair] Dimension DATAREPOSITORY successfully created.");
        
        //Loading DATE dimension
        
        Dataset<Row> dimDate = spark.sql
                ( "SELECT "
                
                    + "MONOTONICALLY_INCREASING_ID() as sk_date, "
                    + "day, month, semester, year "
                
                + "FROM "
                
                    + "(SELECT DISTINCT day, month, semester, year FROM " 
                    + "(SELECT day, month, semester, year FROM BPSP "
                    + "UNION ALL "
                    + "SELECT day, month, semester, year FROM USP "
                    + "UNION ALL "
                    + "SELECT day, month, semester, year FROM SL "
                    + "UNION ALL "
                    + "SELECT day, month, semester, year FROM AE "
                    + "UNION ALL "
                    + "SELECT day, month, semester, year FROM FG "                    
                    + "UNION ALL "
                    + "SELECT day, month, semester, year FROM CDC))"
                
                );
                
        dimDate.show();
        
        dimDate.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_date");

        log.add("[loading_bigfair] Dimension DATE successfully created.");
        System.out.println("[loading_bigfair] Dimension DATE successfully created.");
        
        //Loading TIME dimension
        
        Dataset<Row> dimTime = spark.sql
                ( "SELECT "

                    + "MONOTONICALLY_INCREASING_ID() as sk_time, "
                    + "hour, minute, second "
                
                + "FROM "
                
                    + "(SELECT DISTINCT hour, minute, second FROM "
                    + "(SELECT hour, minute, second FROM BPSP "
                    + "UNION ALL "
                    + "SELECT hour, minute, second FROM USP "
                    + "UNION ALL "
                    + "SELECT hour, minute, second FROM SL "
                    + "UNION ALL "
                    + "SELECT hour, minute, second FROM AE "
                    + "UNION ALL "
                    + "SELECT hour, minute, second FROM FG "
                    + "UNION ALL "
                    + "SELECT hour, minute, second FROM CDC))"
                
                );
                
        dimTime.show();
        
        dimTime.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_time");
                
        log.add("[loading_bigfair] Dimension TIME successfully created.");
        System.out.println("[loading_bigfair] Dimension TIME successfully created.");

        //Loading DATACELL dimension
        
        Dataset<Row> dimDataCell = spark.sql
                ( "SELECT "
                
                    + "MONOTONICALLY_INCREASING_ID() as sk_datacell, "
                    + "data_object_title, data_object_description, data_object_type, data_attribute_title, "
                    + "data_attribute_description, data_attribute_type, data_attribute_possible_content, "
                    + "data_instance_unique_source_id_01, data_instance_unique_source_id_02 "
                
                + "FROM "
                
                    + "("
                        + "SELECT DISTINCT "
                            + "data_object_title, data_object_description, data_object_type, data_attribute_title, " 
                            + "data_attribute_description, data_attribute_type, data_attribute_possible_content, " 
                            + "data_instance_unique_source_id_01, "
                            + "NVL(data_instance_unique_source_id_02, 0) AS data_instance_unique_source_id_02 "
                        + "FROM "
                        
                        + "("
                            + "SELECT "
                                + "BPSP_DESC.data_object_title, BPSP_DESC.data_object_description, "
                                + "BPSP.data_object_type, BPSP_DESC.data_attribute_title, " 
                                + "BPSP_DESC.data_attribute_description, BPSP.data_attribute_type, "
                                + "BPSP_DESC.data_attribute_possible_content, " 
                                + "BPSP.data_instance_unique_source_id_01, BPSP.data_instance_unique_source_id_02 "
                            + "FROM BPSP "
                            + "INNER JOIN BPSP_DESC ON (BPSP.description_file_id = BPSP_DESC.id) "
                        
                            + "UNION ALL "
                        
                            + "SELECT "
                                + "USP_DESC.data_object_title, USP_DESC.data_object_description, "
                                + "USP.data_object_type, USP_DESC.data_attribute_title, " 
                                + "USP_DESC.data_attribute_description, USP.data_attribute_type, "
                                + "USP_DESC.data_attribute_possible_content, " 
                                + "USP.data_instance_unique_source_id_01, USP.data_instance_unique_source_id_02 "
                            + "FROM USP "
                            + "INNER JOIN USP_DESC ON (USP.description_file_id = USP_DESC.id) "

                            + "UNION ALL "

                            + "SELECT "
                                + "SL_DESC.data_object_title, SL_DESC.data_object_description, "
                                + "SL.data_object_type, SL_DESC.data_attribute_title, " 
                                + "SL_DESC.data_attribute_description, SL.data_attribute_type, "
                                + "SL_DESC.data_attribute_possible_content, " 
                                + "SL.data_instance_unique_source_id_01, SL.data_instance_unique_source_id_02 "
                            + "FROM SL "
                            + "INNER JOIN SL_DESC ON (SL.description_file_id = SL_DESC.id) "

                            + "UNION ALL "

                            + "SELECT "
                                + "AE_DESC.data_object_title, AE_DESC.data_object_description, "
                                + "AE.data_object_type, AE_DESC.data_attribute_title, " 
                                + "AE_DESC.data_attribute_description, AE.data_attribute_type, "
                                + "AE_DESC.data_attribute_possible_content, " 
                                + "AE.data_instance_unique_source_id_01, AE.data_instance_unique_source_id_02 "
                            + "FROM AE "
                            + "INNER JOIN AE_DESC ON (AE.description_file_id = AE_DESC.id) "

                            + "UNION ALL "

                            + "SELECT "
                                + "FG_DESC.data_object_title, FG_DESC.data_object_description, "
                                + "FG.data_object_type, FG_DESC.data_attribute_title, " 
                                + "FG_DESC.data_attribute_description, FG.data_attribute_type, "
                                + "FG_DESC.data_attribute_possible_content, " 
                                + "FG.data_instance_unique_source_id_01, FG.data_instance_unique_source_id_02 "
                            + "FROM FG "
                            + "INNER JOIN FG_DESC ON (FG.description_file_id = FG_DESC.id) "

                            + "UNION ALL "

                            + "SELECT "
                                + "CDC_DESC.data_object_title, CDC_DESC.data_object_description, "
                                + "CDC.data_object_type, CDC_DESC.data_attribute_title, " 
                                + "CDC_DESC.data_attribute_description, CDC.data_attribute_type, "
                                + "CDC_DESC.data_attribute_possible_content, " 
                                + "CDC.data_instance_unique_source_id_01, CDC.data_instance_unique_source_id_02 "
                            + "FROM CDC "
                            + "INNER JOIN CDC_DESC ON (CDC.description_file_id = CDC_DESC.id)"
                        + ")"
                    + ")"
                
                );
                
        dimDataCell.show();
        
        dimDataCell.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/dim_datacell");
        
        log.add("[loading_bigfair] Dimension DATACELL successfully created.");
        System.out.println("[loading_bigfair] Dimension DATACELL successfully created.");
        
        //Creating temporary views for the dimensions
        
        dimLicense.createOrReplaceTempView("DIM_LICENSE");
        dimPermissions.createOrReplaceTempView("DIM_PERMISSIONS");
        dimStatus.createOrReplaceTempView("DIM_STATUS");
        dimDataProvider.createOrReplaceTempView("DIM_DATAPROVIDER");
        dimDataRepository.createOrReplaceTempView("DIM_DATAREPOSITORY");
        dimDate.createOrReplaceTempView("DIM_DATE");
        dimTime.createOrReplaceTempView("DIM_TIME");
        dimDataCell.createOrReplaceTempView("DIM_DATACELL");
        
        log.add("[loading_bigfair] Temporary dimension views successfully created.");
        System.out.println("[loading_bigfair] Temporary dimension views successfully created.");
        
        //Loading FACT table
        
        Dataset<Row> factTable = spark.sql
                ( "SELECT DISTINCT "
                
                    + "sk_dataprovider, "
                    + "sk_datarepository, "
                    + "sk_datacell, "
                    + "sk_status, "
                    + "sk_permissions, "
                    + "sk_license, "
                    + "sk_date, "
                    + "sk_time,"
                    + "size "
                
                + "FROM "
                
                    + "("
 
                    //BPSP

                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "BPSP.size "
                        
                    + "FROM BPSP "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'BPSP') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'BPSP') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = BPSP.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = BPSP.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = BPSP.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(BPSP.data_instance_unique_source_id_02, 0)"
                            + "AND DIM_DATACELL.data_object_title LIKE '%bpsp%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = BPSP.day "
                            + "AND DIM_DATE.month = BPSP.month "
                            + "AND DIM_DATE.year = BPSP.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = BPSP.hour "
                            + "AND DIM_TIME.minute = BPSP.minute "
                            + "AND DIM_TIME.second = BPSP.second) "
                        
                    + "UNION "

                    //USP
                        
                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "USP.size "
                        
                    + "FROM USP "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'USP') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'USP') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = USP.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = USP.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = USP.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(USP.data_instance_unique_source_id_02, 0) "
                            + "AND DIM_DATACELL.data_object_title LIKE '%HC%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = USP.day "
                            + "AND DIM_DATE.month = USP.month "
                            + "AND DIM_DATE.year = USP.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = USP.hour "
                            + "AND DIM_TIME.minute = USP.minute "
                            + "AND DIM_TIME.second = USP.second) "
                
                    + "UNION "

                    //SL
                        
                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "SL.size "
                        
                    + "FROM SL "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'SL') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'SL') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = SL.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = SL.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = SL.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(SL.data_instance_unique_source_id_02, 0) "
                            + "AND DIM_DATACELL.data_object_title LIKE '%HSL%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = SL.day "
                            + "AND DIM_DATE.month = SL.month "
                            + "AND DIM_DATE.year = SL.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = SL.hour "
                            + "AND DIM_TIME.minute = SL.minute "
                            + "AND DIM_TIME.second = SL.second) "
                        
                    + "UNION "

                    //AE
                        
                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "AE.size "
                        
                    + "FROM AE "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'AE') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'AE') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = AE.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = AE.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = AE.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(AE.data_instance_unique_source_id_02, 0) "
                            + "AND DIM_DATACELL.data_object_title LIKE '%EINSTEIN%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = AE.day "
                            + "AND DIM_DATE.month = AE.month "
                            + "AND DIM_DATE.year = AE.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = AE.hour "
                            + "AND DIM_TIME.minute = AE.minute "
                            + "AND DIM_TIME.second = AE.second) " 
                        
                    + "UNION "

                    //FG
                        
                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "FG.size "
                        
                    + "FROM FG "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'FG') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'FG') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = FG.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = FG.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = FG.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(FG.data_instance_unique_source_id_02, 0)"
                            + "AND DIM_DATACELL.data_object_title LIKE '%GrupoFleury%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = FG.day "
                            + "AND DIM_DATE.month = FG.month "
                            + "AND DIM_DATE.year = FG.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = FG.hour "
                            + "AND DIM_TIME.minute = FG.minute "
                            + "AND DIM_TIME.second = FG.second) "

                    + "UNION "

                    //CDC
                        
                    + "SELECT "

                        + "DIM_DATAPROVIDER.sk_dataprovider, "
                        + "DIM_DATAREPOSITORY.sk_datarepository, "
                        + "DIM_DATACELL.sk_datacell, "
                        + "DIM_STATUS.sk_status, "
                        + "DIM_PERMISSIONS.sk_permissions, "
                        + "DIM_LICENSE.sk_license, "
                        + "DIM_DATE.sk_date, "
                        + "DIM_TIME.sk_time,"
                        + "CDC.size "
                        
                    + "FROM CDC "
                        
                    + "INNER JOIN DIM_DATAPROVIDER "
                        + "ON (DIM_DATAPROVIDER.name = 'CDC') "
                        
                    + "INNER JOIN DIM_DATAREPOSITORY "
                        + "ON (DIM_DATAREPOSITORY.name = 'CDC') "
                        
                    + "INNER JOIN DIM_DATACELL "
                        + "ON (DIM_DATACELL.data_object_title = CDC.data_object_title "
                            + "AND DIM_DATACELL.data_attribute_title = CDC.data_attribute_title "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_01 = CDC.data_instance_unique_source_id_01 "
                            + "AND DIM_DATACELL.data_instance_unique_source_id_02 = NVL(CDC.data_instance_unique_source_id_02, 0)"
                            + "AND DIM_DATACELL.data_object_title LIKE '%Case_Surveillance%') "
                        
                    + "INNER JOIN DIM_STATUS "
                        + "ON (DIM_STATUS.sk_status = 1) "
                        
                    + "INNER JOIN DIM_PERMISSIONS "
                        + "ON (DIM_PERMISSIONS.sk_permissions = 1) "
                        
                    + "INNER JOIN DIM_LICENSE "
                        + "ON (DIM_LICENSE.sk_license = 1) "
                        
                    + "INNER JOIN DIM_DATE "
                        + "ON (DIM_DATE.day = CDC.day "
                            + "AND DIM_DATE.month = CDC.month "
                            + "AND DIM_DATE.year = CDC.year) "
                        
                    + "INNER JOIN DIM_TIME "
                        + "ON (DIM_TIME.hour = CDC.hour "
                            + "AND DIM_TIME.minute = CDC.minute "
                            + "AND DIM_TIME.second = CDC.second) "
                    
                    + ")"

                );
        
        factTable.show();
        
        factTable.write().format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat").mode(SaveMode.Overwrite).save("(removed_for_anonymization)/dw/fact_table");
                
        log.add("[loading_bigfair] Fact table successfully loaded.");
        System.out.println("[loading_bigfair] Fact table successfully loaded.");
        
        //Finishing JOB
                
        long jobTempoFinal = System.currentTimeMillis();
        
        log.add("[loading_bigfair] Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        System.out.println("[loading_bigfair] Job finished. Execution time: " + (jobTempoFinal - jobTempoInicial));
        
        return log;
        
    }
    
}
