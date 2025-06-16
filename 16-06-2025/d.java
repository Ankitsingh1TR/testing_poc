import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import java.util.*;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.*;

public class HPALPopulation {

    private final SparkSession spark;
    private final Properties dbProps;

    public HPALPopulation(SparkSession spark, Properties dbProps) {
        this.spark = spark;
        this.dbProps = dbProps;
    }

    public void run(String correlationId) {
        try {
            // Step 1: Fetch JSON request from MRR
            String jsonObject = DBUtils.fetchDataFromhedisSamplingRequestMrr(spark, dbProps, correlationId);
            Dataset<String> jsonDataset = spark.createDataset(Collections.singletonList(jsonObject), Encoders.STRING());
            Dataset<Row> json = spark.read().json(jsonDataset);

            // Step 2: Flatten JSON
            Dataset<Row> request = json.withColumn("data", explode(col("data")))
                .withColumn("subMeasures", explode(col("data.subMeasures")))
                .select(
                    col("productCategory").alias("ProductLine"),
                    col("data.measureGroup").alias("MeasureCode"),
                    col("subMeasures.clinicalSubMeasureId").alias("ClinicalMeasureId"),
                    col("data.oversamplingRate").divide(100).alias("OverSamplingRate"),
                    col("measurementYear").alias("MeasurementYear"),
                    col("submissionId").alias("SubmissionId"),
                    col("subMeasures.cyar").alias("CYAR"),
                    col("subMeasures.pyr").alias("PYR"),
                    col("excludeEmp").cast("integer").alias("IsEmployee"),
                    lit(correlationId).alias("CorrelationId"),
                    col("organizationId").alias("OrganizationId"),
                    col("runConfigId").alias("RunConfigId")
                );

            // Step 3: Apply HBD2/GSD2 override logic
            request = request
                .withColumn("CYAR", when(col("ClinicalMeasureId").isin("HBD2", "GSD2").and(col("CYAR").isNotNull()), lit(100).minus(col("CYAR"))).otherwise(col("CYAR")))
                .withColumn("PYR", when(col("ClinicalMeasureId").isin("HBD2", "GSD2").and(col("PYR").isNotNull()), lit(100).minus(col("PYR"))).otherwise(col("PYR")))
                .filter(not(col("ClinicalMeasureId").isin("HBD1", "BPD", "GSD1")))
                .filter(not(col("ProductLine").equalTo("Exchange")));

            // Step 4: Join with metadata
            Dataset<Row> sampleSizeInfo = DBUtils.fetchHedisMeasuresSampleSizeInfo(spark, dbProps);
            Dataset<Row> productCategory = DBUtils.fetchHealthPlanProductCategoryView(spark, dbProps);

            String measurementYear = request.select("MeasurementYear").first().get(0).toString();

            Dataset<Row> productMeasureType = request.alias("req")
                .join(sampleSizeInfo.alias("meta"), col("req.MeasureCode").equalTo(col("meta.measurecode")))
                .join(productCategory.alias("cat"), col("req.SubmissionId").equalTo(col("cat.SubmissionId"))
                        .and(col("req.MeasurementYear").equalTo(col("cat.measuremerntYear"))))
                .filter(col("meta.measurementyear").equalTo(lit(measurementYear)))
                .withColumn("CYAR_1", when(col("meta.cyar").equalTo("CYAR").and(col("req.CYAR").isNotNull()), col("req.CYAR")).otherwise(lit(null).cast(DataTypes.StringType)))
                .withColumn("PYR_1", when(col("meta.pyr").equalTo("PYR").and(col("req.PYR").isNotNull()), col("req.PYR")).otherwise(lit(null).cast(DataTypes.StringType)))
                .groupBy("req.ProductLine", "req.MeasureCode", "req.OverSamplingRate", "req.MeasurementYear", "req.SubmissionId", "req.IsEmployee", "req.CorrelationId")
                .agg(min("CYAR_1").alias("CYAR"), min("PYR_1").alias("PYR"));

            Dataset<Row> combined = productMeasureType
                .groupBy("ProductLine", "MeasureCode", "OverSamplingRate", "MeasurementYear", "SubmissionId", "IsEmployee", "CorrelationId")
                .agg(min("CYAR").alias("min_CYAR"), min("PYR").alias("min_PYR"))
                .withColumn("CYAR_PYR", coalesce(col("min_CYAR"), col("min_PYR")))
                .select("ProductLine", "MeasureCode", "OverSamplingRate", "MeasurementYear", "SubmissionId", "CYAR_PYR", "IsEmployee", "CorrelationId")
                .orderBy("MeasureCode");

            combined.show(false); // Final output in table format

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HPAL Creation").setMaster("local[*]");
        conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED");
        conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED");
        conf.set("spark.executor.defaultJavaOptions", "-XX:+UseG1GC");
        conf.set("spark.driver.defaultJavaOptions", "-XX:+UseG1GC");

        SparkSession spark = SparkSession.builder().config(conf).appName("hedis_post_processing").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Properties dbProps = PropertyReader.LoadPropertiesFile("config/database.properties");
        dbProps.setProperty("ClinicalDBPass", CommonUtils.decryptPassword(dbProps.getProperty("ClinicalDBPass")));
        dbProps.setProperty("MeasureDBPass", CommonUtils.decryptPassword(dbProps.getProperty("MeasureDBPass")));

        new HPALPopulation(spark, dbProps).run("S2_1863");
    }
}