package org.broadinstitute.hellbender.utils.bigquery;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1beta2.*;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;


public class CommittedBQWriter implements AutoCloseable {
    static final Logger logger = LogManager.getLogger(CommittedBQWriter.class);

    protected BigQueryWriteClient bqWriteClient;
    protected WriteStream writeStream;
    protected JsonStreamWriter writer;
    protected TableName parentTable;
    protected WriteStream.Type streamType;
    protected int batchSize = 10000;
    protected JSONArray jsonArr = new JSONArray();

    private final ExponentialBackOff backoff = new ExponentialBackOff.Builder().
            setInitialIntervalMillis(500).
            setMaxElapsedTimeMillis(60000).
            setMaxIntervalMillis(30000).
            setMultiplier(1.5).
            setRandomizationFactor(0.5).
            build();

    protected CommittedBQWriter(String projectId, String datasetName, String tableName, WriteStream.Type type) {
        this.parentTable = TableName.of(projectId, datasetName, tableName);
        this.streamType = type;
    }

    protected void createStream() throws Descriptors.DescriptorValidationException, InterruptedException, IOException {
        if (bqWriteClient == null) {
            bqWriteClient = BigQueryWriteClient.create();
        }
        WriteStream writeStreamConfig = WriteStream.newBuilder().setType(streamType).build();
        CreateWriteStreamRequest createWriteStreamRequest =
                CreateWriteStreamRequest.newBuilder()
                        .setParent(parentTable.toString())
                        .setWriteStream(writeStreamConfig)
                        .build();
        writeStream = bqWriteClient.createWriteStream(createWriteStreamRequest);
        writer = JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build();
    }

    public AppendRowsResponse addJsonRow(JSONObject row) throws Descriptors.DescriptorValidationException, ExecutionException, InterruptedException, IOException {
        AppendRowsResponse response = null;
        if (writer == null) {
            createStream();
        }
        jsonArr.put(row);

        if (jsonArr.length() >= batchSize) {
            response = writeJsonArray();
        }
        return response;
    }

    protected AppendRowsResponse writeJsonArray() throws Descriptors.DescriptorValidationException, ExecutionException, InterruptedException, IOException {
        AppendRowsResponse response = null;
        try {
            ApiFuture<AppendRowsResponse> future = writer.append(jsonArr);
            response = future.get();
            jsonArr = new JSONArray();
        } catch (StatusRuntimeException ex) {
            Status status = ex.getStatus();
            if (status == Status.ABORTED || status == Status.INTERNAL || status == Status.CANCELLED || status == Status.UNAVAILABLE) {
                long backOffMillis = backoff.nextBackOffMillis();

                if (backOffMillis == ExponentialBackOff.STOP) {
                    logger.error("Caught exception writing to BigQuery but retries are exhausted, throwing.", ex);
                    throw ex;
                }

                logger.warn("Caught exception writing to BigQuery, retrying...", ex);
                Thread.sleep(backOffMillis);
                createStream();
                response = writeJsonArray();
            }
        }
        return response;
    }

    public void close() {
        if (writer != null) {
            writer.close();
        }
        if (bqWriteClient != null) {
            bqWriteClient.close();
        }
    }
}
