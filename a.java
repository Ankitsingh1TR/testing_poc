import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.util.*;

public class DBUtils {
    private final SparkSession spark;

    public DBUtils(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<Row> getHedisSamplingRequestMRR(String correlationId) {
        return spark.read()
                .format("jdbc")
                .option("url", "jdbc:sqlserver://<host>:<port>;databaseName=<db>")
                .option("dbtable", "(SELECT * FROM hedisSamplingRequest_MRR WHERE CorrelationId = '" + correlationId + "') AS req")
                .option("user", "<user>")
                .option("password", "<password>")
                .load();
    }

    public Dataset<Row> getMeasureProductLineAndOversampling(long runConfigId, long submissionId) {
        return spark.read()
                .format("jdbc")
                .option("url", "jdbc:sqlserver://<host>:<port>;databaseName=<db>")
                .option("dbtable", "(SELECT * FROM ##hedisProductlineandmeasuresTypeMRRFinal WHERE RunConfigId = " + runConfigId + " AND SubmissionId = " + submissionId + ") AS cfg")
                .option("user", "<user>")
                .option("password", "<password>")
                .load();
    }

    public Dataset<Row> getHedisBaseData(long runConfigId, long orgId, String measureCode, long submissionId, int year) {
        return spark.read()
                .format("jdbc")
                .option("url", "jdbc:sqlserver://<host>:<port>;databaseName=<db>")
                .option("dbtable", "(SELECT * FROM HEDIS_BaseData WHERE RunConfigurationId = " + runConfigId +
                        " AND OrganizationId = " + orgId +
                        " AND MeasureCode = '" + measureCode +
                        "' AND SubmissionId = " + submissionId +
                        " AND YEAR(PeriodStartDate) = " + year + ") AS base")
                .option("user", "<user>")
                .option("password", "<password>")
                .load();
    }

    public int getCyarPyrRate(String measureCode, int year, String productLine) {
        // Simulate lookup logic or retrieve from Spark table or broadcast data
        // Placeholder return
        return 77; // Dummy value; replace with real query
    }

    public int getMRSSFromRate(int rate, String productLine, String measureCode, int year) {
        // Dummy logic for MRSS table lookup based on rate and productLine
        if (rate >= 95) return 106;
        if (rate >= 90) return 134;
        return 296; // Fallback value based on 77% from BRD
    }

    public double getRandValue(String measureCode, int year) {
        // Simulate lookup from rand value table
        Map<String, Double> randMap = new HashMap<>();
        randMap.put("GSD", 0.66);
        randMap.put("IMA", 0.5);
        randMap.put("COL", 0.75);
        return randMap.getOrDefault(measureCode, 0.5);
    }

    public void insertIntoHPAL(Dataset<Row> finalSample) {
        finalSample.write()
                .format("jdbc")
                .option("url", "jdbc:sqlserver://<host>:<port>;databaseName=<db>")
                .option("dbtable", "hedisPrimaryAuxilaryList")
                .option("user", "<user>")
                .option("password", "<password>")
                .mode(SaveMode.Append)
                .save();
    }

    public void updateSamplingStatus(String correlationId, String status) {
        // You can implement this using a JDBC connection or write/update logic
        System.out.println("Status Updated for Correlation ID " + correlationId + ": " + status);
    }

    public void logExecutionError(String processName, Exception e) {
        e.printStackTrace();
        // Log to error table or monitoring system if needed
    }
} 
