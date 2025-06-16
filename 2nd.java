
SparkSession spark = SparkSession.builder()
    .appName("HPALPopulation")
    .master("local[*]")  // Enables local execution using all cores
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")  // Optional, but explicit
    .getOrCreate();


System.out.println("🟢 Spark job completed. Visit http://localhost:4040 to see the UI.");
Thread.sleep(30000); // Keeps SparkContext alive for 30 seconds

_----_-------------

import org.apache.spark.sql.; import org.apache.spark.sql.expressions.Window; import static org.apache.spark.sql.functions.; import java.util.*;

public class HPALPopulation {

private final SparkSession spark;
private final DBUtils dbUtils;

public HPALPopulation(SparkSession spark, DBUtils dbUtils) {
    this.spark = spark;
    this.dbUtils = dbUtils;
}

public void run(String correlationId, long batchId, String jobName) {
    try {
        Dataset<Row> requestMetadata = dbUtils.getHedisSamplingRequestMRR(correlationId);
        Row request = requestMetadata.first();

        long runConfigId = request.getAs("RunConfigId");
        long orgId = request.getAs("OrgId");
        int measurementYear = request.getAs("MeasurementYear");
        long submissionId = request.getAs("SubmissionId");

        Dataset<Row> measureConfig = dbUtils.getMeasureProductLineAndOversampling(runConfigId, submissionId);
        for (Row config : measureConfig.collectAsList()) {
            String measureCode = config.getAs("MeasureCode");
            String productLine = config.getAs("ProductLine");
            double oversamplingRate = config.getAs("OverSamplingRate");

            Dataset<Row> baseData = dbUtils.getHedisBaseData(runConfigId, orgId, measureCode, submissionId, measurementYear);
            baseData = baseData.filter(col("ClinicalStatusCode").isin(1, 6, 15)).filter(col("IsDeleted").equalTo(0));

            long emCount = baseData.select("PatientID").distinct().count();
            System.out.println("EM count for " + measureCode + ": " + emCount);

            int priorOrCurrentRate = dbUtils.getCyarPyrRate(measureCode, measurementYear, productLine);
            int mrss = dbUtils.getMRSSFromRate(priorOrCurrentRate, productLine, measureCode, measurementYear);
            int fss = (int) Math.ceil(mrss * (1 + oversamplingRate));
            System.out.println("MRSS: " + mrss + ", FSS: " + fss);

            if (emCount <= mrss) {
                System.out.println("Skipping systematic sampling — taking all EM for " + measureCode);
                markAndSaveSample(baseData, measureCode, mrss, 0);
                continue;
            }

            WindowSpec sortingWindow = Window.orderBy(col("LastName"), col("FirstName"), col("DateOfBirth"));
            baseData = baseData.withColumn("RowNum", row_number().over(sortingWindow));

            int N = (int) Math.floor((double) emCount / fss);
            double rand = dbUtils.getRandValue(measureCode, measurementYear);
            int start = (int) Math.round(rand * N);
            if (start <= 0) start = 1;
            System.out.println("N: " + N + ", Start: " + start);

            List<Integer> sampleIndexes = new ArrayList<>();
            for (int i = 0; i < fss; i++) {
                int index = start + (int) Math.round(i * ((double) emCount / fss));
                if (index <= emCount) sampleIndexes.add(index);
            }

            Dataset<Row> primarySample = baseData.filter(col("RowNum").leq(mrss));
            Dataset<Row> oversample = baseData.filter(col("RowNum").gt(mrss)).limit(fss - mrss);

            markAndSaveSample(primarySample.unionByName(oversample), measureCode, mrss, fss - mrss);
        }

        dbUtils.updateSamplingStatus(correlationId, "PROCESSED");
        System.out.println("✔ Sampling complete for correlationId = " + correlationId);

        // Keep Spark UI alive after completion
        System.out.println("🟢 Visit http://localhost:4040 to see Spark UI.");
        Thread.sleep(30000);

    } catch (Exception e) {
        dbUtils.updateSamplingStatus(correlationId, "FAILED");
        dbUtils.logExecutionError("HPALPopulation", e);
    }
}

private void markAndSaveSample(Dataset<Row> sample, String measureCode, int mrss, int oversampleSize) {
    Dataset<Row> markedSample = sample.withColumn("SampleType",
            when(col("RowNum").leq(mrss), lit("Primary")).otherwise(lit("Auxiliary")));

    System.out.println("✔ Writing " + markedSample.count() + " rows for measure " + measureCode +
                       " (MRSS: " + mrss + ", OS: " + oversampleSize + ")");
    markedSample.show(false);

    dbUtils.insertIntoHPAL(markedSample);
    System.out.println("✔ Data written to hedisPrimaryAuxilaryList for " + measureCode);
}

}
spark.sparkContext().setJobGroup("SHOW_SAMPLE", "Show sampled members for " + measureCode);
markedSample.show(false);

spark.sparkContext().setJobGroup("COUNT_SAMPLE", "Count sampled members for " + measureCode);
long count = markedSample.count();

spark.sparkContext().setJobGroup("WRITE_SAMPLE", "Write sampled members to DB for " + measureCode);
dbUtils.insertIntoHPAL(markedSample);
_____________

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

private void markAndSaveSample(Dataset<Row> sample, String measureCode, int mrss, int oversampleSize) {
    Dataset<Row> markedSample = sample.withColumn("SampleType",
            when(col("RowNum").leq(mrss), lit("Primary")).otherwise(lit("Auxiliary")));

    // Convert to JSON and print
    List<Row> rows = markedSample.collectAsList();
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT); // Pretty print

    System.out.println("📦 Final sample rows for " + measureCode + ":");
    for (Row row : rows) {
        Map<String, Object> jsonMap = new LinkedHashMap<>();
        for (StructField field : row.schema().fields()) {
            jsonMap.put(field.name(), row.getAs(field.name()));
        }
        try {
            String json = mapper.writeValueAsString(jsonMap);
            System.out.println(json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    long count = rows.size();
    System.out.println("✔ Total rows to insert: " + count);

    dbUtils.insertIntoHPAL(markedSample);
    System.out.println("✔ Data written to hedisPrimaryAuxilaryList for " + measureCode);
}

