Dataset<Row> jsonSampleReq = DBUtils.fetchHedisSamplingRequestMRR(sparkSession, dbProps, correlationId);
jsonSampleReq.createOrReplaceTempView("Json_sample_req");
