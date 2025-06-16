import org.apache.spark.sql.*; import java.util.Properties;

public class DBUtils { private final SparkSession spark; private final Properties dbProps;

public DBUtils(SparkSession spark, Properties dbProps) {
    this.spark = spark;
    this.dbProps = dbProps;
}

public Dataset<Row> getHedisSamplingRequestMRR(String correlationId) {
    String query = "SELECT * FROM hedisSamplingRequest_MRR WHERE CorrelationId = '" + correlationId + "'";
    return fetchFromClinicalDB(query);
}

public Dataset<Row> getMeasureProductLineAndOversampling(long runConfigId, long submissionId) {
    String query = "SELECT * FROM ##hedisProductlineandmeasuresTypeMRRFinal WHERE RunConfigId = " + runConfigId +
            " AND SubmissionId = " + submissionId;
    return fetchFromClinicalDB(query);
}

public Dataset<Row> getHedisBaseData(long runConfigId, long orgId, String measureCode, long submissionId, int year) {
    String query = "SELECT * FROM HEDIS_BaseData WHERE RunConfigurationId = " + runConfigId +
            " AND OrganizationId = " + orgId +
            " AND MeasureCode = '" + measureCode +
            "' AND SubmissionId = " + submissionId +
            " AND YEAR(PeriodStartDate) = " + year;
    return fetchFromClinicalDB(query);
}

public int getCyarPyrRate(String measureCode, int year, String productLine) {
    return 77; // Placeholder for actual logic
}

public int getMRSSFromRate(int rate, String productLine, String measureCode, int year) {
    if (rate >= 95) return 106;
    if (rate >= 90) return 134;
    return 296;
}

public double getRandValue(String measureCode, int year) {
    if (measureCode.equals("GSD")) return 0.66;
    if (measureCode.equals("IMA")) return 0.50;
    return 0.55;
}

public void insertIntoHPAL(Dataset<Row> finalSample) {
    finalSample.write()
            .format("jdbc")
            .option("url", dbProps.getProperty("url"))
            .option("dbtable", "hedisPrimaryAuxilaryList")
            .option("driver", dbProps.getProperty("driver"))
            .option("user", dbProps.getProperty("user"))
            .option("password", dbProps.getProperty("password"))
            .mode(SaveMode.Append)
            .save();
}

public void updateSamplingStatus(String correlationId, String status) {
    System.out.println("Sampling status for CorrelationId: " + correlationId + " => " + status);
}

public void logExecutionError(String processName, Exception e) {
    e.printStackTrace();
}

private Dataset<Row> fetchFromClinicalDB(String query) {
    return spark.read()
            .format("jdbc")
            .option("driver", dbProps.getProperty("driver"))
            .option("url", dbProps.getProperty("url"))
            .option("dbtable", "(" + query + ") tmp")
            .option("user", dbProps.getProperty("user"))
            .option("password", dbProps.getProperty("password"))
            .load();
}

}

