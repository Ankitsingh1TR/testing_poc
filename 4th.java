package com.citius.healtheval.hedis.mrr.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DBUtils {

    private static Dataset<Row> fetchFromClinicalDB(SparkSession sparkSession, Properties dbProps, String query) {
        return sparkSession.read()
                .jdbc(dbProps.getProperty("url"), "(" + query + ") AS tmp", dbProps);
    }

    // For SP1
    public static Dataset<Row> fetchSamplingRequestMRR(SparkSession sparkSession, Properties dbProps, String correlationId) {
        String query = "SELECT jsonobject FROM hedisSamplingRequest_MRR WHERE correlationid = '" + correlationId + "' " +
                "AND requesttype = 'SAMPLING' AND campaignStatus = 'received' AND issamplingprocessed = 0";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    // For SP1/SP2/SP3
    public static Dataset<Row> fetchHedisMeasuresSampleSizeInfo(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM hedisMeasuresSampleSizeInfo";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    // For SP2
    public static Dataset<Row> fetchEligibleMembers(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM vw_healthPlanProductCategory";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    // For SP3
    public static Dataset<Row> fetchBaseDataIntermediate(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM HEDIS_BaseData_intermediate";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    public static Dataset<Row> fetchRandValues(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM hedisMeasuresRandValue";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    public static Dataset<Row> fetchMeasureClassifications(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM clinicalMeasureClassification WHERE clinicalclassificationvalue = 'Hybrid'";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    public static Dataset<Row> fetchVisitDates(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM hedisVisitDates";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }

    public static Dataset<Row> fetchDischargeDates(SparkSession sparkSession, Properties dbProps) {
        String query = "SELECT * FROM hedisDischargeDates";
        return fetchFromClinicalDB(sparkSession, dbProps, query);
    }
}
