package com.citius.healtheval.hedis.mrr.spark;

import org.apache.spark.sql.Dataset; import org.apache.spark.sql.Row; import org.apache.spark.sql.SaveMode; import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static com.citius.healtheval.hedis.mrr.spark.DBUtils.*;

public class HPALPopulation {

public static void main(String[] args) {

    SparkSession spark = SparkSession.builder()
            .appName("HPALPopulationJob")
            .master("local[*]")
            .getOrCreate();

    Properties dbProps = new Properties();
    dbProps.put("user", "yourUsername");
    dbProps.put("password", "yourPassword");
    dbProps.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    dbProps.put("url", "jdbc:sqlserver://yourServer;databaseName=yourDB");

    String correlationId = "CORRELATION_ID";
    int measurementYear = 2024;

    // Step 1: Fetch required datasets
    Dataset<Row> samplingRequest = fetchSamplingRequestMRR(spark, dbProps, correlationId);
    samplingRequest.createOrReplaceTempView("samplingRequest");

    Dataset<Row> baseData = fetchBaseDataIntermediate(spark, dbProps);
    baseData.createOrReplaceTempView("HEDIS_BaseData_intermediate");

    Dataset<Row> measureClassifications = fetchMeasureClassifications(spark, dbProps);
    measureClassifications.createOrReplaceTempView("clinicalMeasureClassification");

    Dataset<Row> sampleSizeInfo = fetchHedisMeasuresSampleSizeInfo(spark, dbProps);
    sampleSizeInfo.createOrReplaceTempView("hedisMeasuresSampleSizeInfo");

    Dataset<Row> randValues = fetchRandValues(spark, dbProps);
    randValues.createOrReplaceTempView("hedisMeasuresRandValue");

    Dataset<Row> visitDates = fetchVisitDates(spark, dbProps);
    visitDates.createOrReplaceTempView("hedisVisitDates");

    Dataset<Row> dischargeDates = fetchDischargeDates(spark, dbProps);
    dischargeDates.createOrReplaceTempView("hedisDischargeDates");

    // Step 2: Join base data with hybrid classifications
    Dataset<Row> eligibleHybridMembers = spark.sql("""
        SELECT bdi.*
        FROM HEDIS_BaseData_intermediate bdi
        JOIN clinicalMeasureClassification cmc
          ON bdi.clinicalmeasureid = cmc.clinicalmeasureid
        WHERE cmc.clinicalclassificationvalue = 'Hybrid'
    """);
    eligibleHybridMembers.createOrReplaceTempView("eligibleHybridMembers");

    // Step 3: Count eligible members per measure
    Dataset<Row> memberCounts = spark.sql("""
        SELECT measurecode, COUNT(DISTINCT patientid) AS eligibleMemberCount
        FROM eligibleHybridMembers
        GROUP BY measurecode
    """);
    memberCounts.createOrReplaceTempView("memberCounts");

    // Step 4: Join with MRSS and apply oversampling
    Dataset<Row> withMRSS = spark.sql("""
        SELECT mc.measurecode, mc.eligibleMemberCount, msi.samplesize AS MRSS,
               CEIL(msi.samplesize * (1 + ISNULL(msi.oversamplingrate, 0) / 100.0)) AS FSS
        FROM memberCounts mc
        JOIN hedisMeasuresSampleSizeInfo msi
          ON mc.measurecode = msi.measurecode
        WHERE msi.measurementyear = """ + measurementYear + """
    );
    withMRSS.createOrReplaceTempView("finalSampleSize");

    // Step 5: Select primary sample (placeholder: top-N based on FSS)
    Dataset<Row> primarySample = spark.sql("""
        SELECT DISTINCT em.patientid, em.measurecode, 'Primary' AS listtype
        FROM eligibleHybridMembers em
        JOIN finalSampleSize fss ON em.measurecode = fss.measurecode
        WHERE RAND() <= 1.0
        LIMIT 100 -- Placeholder, apply sampling logic here
    """);

    primarySample.createOrReplaceTempView("primarySample");

    // Step 6: Write to hedisPrimaryAuxiliaryList
    primarySample.write()
            .mode(SaveMode.Append)
            .jdbc(dbProps.getProperty("url"), "hedisPrimaryAuxiliaryList", dbProps);

    // Optional: Update sampling request status (not implemented)
    // Optional: Log error handling (not implemented)

    // Step 7: Output for console debugging
    System.out.println("=== Final Primary Sample ===");
    primarySample.show(100, false);
    primarySample.printSchema();

    spark.stop();
}

}
