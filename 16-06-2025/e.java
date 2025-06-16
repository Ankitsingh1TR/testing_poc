import org.apache.spark.sql.*;
import java.util.Properties;

public class DBUtils {

    // Fetch the JSON from hedisSamplingRequest_MRR for the given correlationId
    public static String fetchDataFromhedisSamplingRequestMrr(SparkSession spark, Properties dbProps, String correlationId) {
        Dataset<Row> data = spark.read()
                .format("jdbc")
                .option("url", dbProps.getProperty("ClinicalDBUrl"))
                .option("dbtable", "hedisSamplingRequest_MRR")
                .option("user", dbProps.getProperty("ClinicalDBUser"))
                .option("password", dbProps.getProperty("ClinicalDBPass"))
                .option("driver", dbProps.getProperty("JDBCDriver"))
                .load()
                .filter("correlationId = '" + correlationId + "'");

        return data.select("json").first().getString(0);
    }

    // Fetch metadata mapping info from hedisMeasuresSampleSizeInfo
    public static Dataset<Row> fetchHedisMeasuresSampleSizeInfo(SparkSession spark, Properties dbProps) {
        return spark.read()
                .format("jdbc")
                .option("url", dbProps.getProperty("MeasureDBUrl"))
                .option("dbtable", "hedisMeasuresSampleSizeInfo")
                .option("user", dbProps.getProperty("MeasureDBUser"))
                .option("password", dbProps.getProperty("MeasureDBPass"))
                .option("driver", dbProps.getProperty("JDBCDriver"))
                .load();
    }

    // Fetch health plan product category details
    public static Dataset<Row> fetchHealthPlanProductCategoryView(SparkSession spark, Properties dbProps) {
        return spark.read()
                .format("jdbc")
                .option("url", dbProps.getProperty("ClinicalDBUrl"))
                .option("dbtable", "vw_healthPlanProductCategory")
                .option("user", dbProps.getProperty("ClinicalDBUser"))
                .option("password", dbProps.getProperty("ClinicalDBPass"))
                .option("driver", dbProps.getProperty("JDBCDriver"))
                .load();
    }
}