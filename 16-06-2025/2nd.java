String fssQuery = "SELECT m.measurecode, m.eligiblemember, m.MRSS, " +
                  "CEIL(m.MRSS * (1 + s.oversamplingrate / 100.0)) AS FSS " +
                  "FROM temp_mrss m " +
                  "JOIN Json_sample_req s ON m.measurecode = s.measurecode";
Dataset<Row> fssResult = sparkSession.sql(fssQuery);
fssResult.createOrReplaceTempView("temp_fss");
