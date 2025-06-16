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

            int priorOrCurrentRate = dbUtils.getCyarPyrRate(measureCode, measurementYear, productLine);
            int mrss = dbUtils.getMRSSFromRate(priorOrCurrentRate, productLine, measureCode, measurementYear);
            int fss = (int) Math.ceil(mrss * (1 + oversamplingRate));

            if (emCount <= mrss) {
                markAndSaveSample(baseData, measureCode, mrss, 0);
                continue;
            }

            WindowSpec sortingWindow = Window.orderBy(col("LastName"), col("FirstName"), col("DateOfBirth"));
            baseData = baseData.withColumn("RowNum", row_number().over(sortingWindow));

            int N = (int) Math.floor((double) emCount / fss);
            double rand = dbUtils.getRandValue(measureCode, measurementYear);
            int start = (int) Math.round(rand * N);
            if (start <= 0) start = 1;

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

    } catch (Exception e) {
        dbUtils.updateSamplingStatus(correlationId, "FAILED");
        dbUtils.logExecutionError("HPALPopulation", e);
    }
}

private void markAndSaveSample(Dataset<Row> sample, String measureCode, int mrss, int oversampleSize) {
    Dataset<Row> markedSample = sample.withColumn("SampleType",
            when(col("RowNum").leq(mrss), lit("Primary")).otherwise(lit("Auxiliary")));

    dbUtils.insertIntoHPAL(markedSample);
}

}

