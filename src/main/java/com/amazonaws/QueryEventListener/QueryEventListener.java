/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.QueryEventListener;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.facebook.presto.spi.eventlistener.*;
import com.facebook.presto.spi.eventlistener.EventListener;

public class QueryEventListener
        implements EventListener
{
    Logger logger;
    FileHandler fh;
    final String loggerName = "QueryLog";
    PrestoCloudWatchWrapper cloudWatchWrapper = new PrestoCloudWatchWrapper();
    DateTimeFormatter formatter =
            DateTimeFormatter.ofLocalizedDateTime( FormatStyle.SHORT )
                    .withLocale( Locale.GERMAN )
                    .withZone( ZoneId.systemDefault() );

    public QueryEventListener()
    {

        createLogFile();

    }

    public QueryEventListener(Map<String, String> config)
    {

        createLogFile();

    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {

        StringBuilder msg = new StringBuilder();
        HashMap<String, String> prestoCWDimensions = new HashMap<>();

        try {
            prestoCWDimensions.put("queryId", queryCreatedEvent.getMetadata().getQueryId());
            prestoCWDimensions.put("createTime", formatter.format(queryCreatedEvent.getCreateTime()));
            prestoCWDimensions.put("queryState", queryCreatedEvent.getMetadata().getQueryState());
            prestoCWDimensions.put("prestoServer", queryCreatedEvent.getContext().getServerAddress());
            prestoCWDimensions.put("catalog", queryCreatedEvent.getContext().getCatalog().orElse("default"));
            prestoCWDimensions.put("schema", queryCreatedEvent.getContext().getSchema().orElse("default"));
            prestoCWDimensions.put("principal", queryCreatedEvent.getContext().getPrincipal().orElse("admin"));
            prestoCWDimensions.put("user", queryCreatedEvent.getContext().getUser());
            prestoCWDimensions.put("userAgent", queryCreatedEvent.getContext().getUserAgent().orElse("None"));

            cloudWatchWrapper.putPrestoMetricToCloudWatch(
                    prestoCWDimensions, "QUERY_STARTED",
                    1.0, "QUERY_STARTED"
            );
            logger.info("Query Initialized");
        }
        catch (Exception ex) {
            cloudWatchWrapper.putPrestoMetricToCloudWatch(
                    prestoCWDimensions, "QUERY_STARTED",
                    0.0, "QUERY_STARTED"
            );
            logger.info(ex.getMessage());
        }

    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {

        String errorCode = null;
        StringBuilder msg = new StringBuilder();
        HashMap<String, String> prestoCWDimensions = new HashMap<>();

        try {
            errorCode = queryCompletedEvent.getFailureInfo().get().getErrorCode().getName();
        }
        catch (NoSuchElementException noElEx) {
            errorCode = null;
        }

        try {
            String queryId = queryCompletedEvent.getMetadata().getQueryId();

            if (errorCode != null) {

                prestoCWDimensions.put("queryId", queryCompletedEvent.getMetadata().getQueryId());
                prestoCWDimensions.put("createTime",
                        formatter.format(queryCompletedEvent.getCreateTime()));
                prestoCWDimensions.put("user", queryCompletedEvent.getContext().getUser());
                prestoCWDimensions.put("queryCompleted",
                        String.valueOf(queryCompletedEvent.getStatistics().isComplete()));
                prestoCWDimensions.put("queryError", errorCode);
                prestoCWDimensions.put("remoteClientAddress",
                        queryCompletedEvent.getContext().getRemoteClientAddress().toString());

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "QUERY_COMPLETED",
                        0.0, "QUERY_COMPLETED"
                );

                logger.info("Query completed with errors. Logs sent to AWS Cloudwatch");

            }
            else {

                prestoCWDimensions.put("queryId", queryCompletedEvent.getMetadata().getQueryId());
                prestoCWDimensions.put("createTime",
                        formatter.format(queryCompletedEvent.getCreateTime()));
                prestoCWDimensions.put("user", queryCompletedEvent.getContext().getUser());
                prestoCWDimensions.put("queryCompleted",
                        String.valueOf(queryCompletedEvent.getStatistics().isComplete()));
                prestoCWDimensions.put("remoteClientAddress",
                        queryCompletedEvent.getContext().getRemoteClientAddress().toString());

                Double totalRows = Double.longBitsToDouble(queryCompletedEvent.getStatistics().getTotalRows());
                Double totalBytes = Double.longBitsToDouble(queryCompletedEvent.getStatistics().getTotalBytes());
                Double cpuTime = Double.longBitsToDouble(queryCompletedEvent.getStatistics().getCpuTime().getSeconds() / 60);
                Double wallTime = Double.longBitsToDouble(queryCompletedEvent.getStatistics().getWallTime().getSeconds() / 60);
                Double queuedTime = Double.longBitsToDouble(queryCompletedEvent.getStatistics().getQueuedTime().getSeconds() / 60);

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "QUERY_COMPLETED",
                        1.0, "QUERY_COMPLETED"
                );

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "TOTAL_ROWS",
                        totalRows, "QUERY_COMPLETED"
                );

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "TOTAL_BYTES",
                        totalBytes, "QUERY_COMPLETED"
                );

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "CPU_TIME",
                        cpuTime, "QUERY_COMPLETED"
                );

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "WALL_TIME",
                        wallTime, "QUERY_COMPLETED"
                );

                cloudWatchWrapper.putPrestoMetricToCloudWatch(
                        prestoCWDimensions, "QUEUED_TIME",
                        queuedTime, "QUERY_COMPLETED"
                );


                logger.info("Query completed without errors. Metrics sent to AWS Cloudwatch");
            }
        }
        catch (Exception ex) {
            logger.info(ex.getMessage());
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        StringBuilder msg = new StringBuilder();

        try {

            msg.append("---------------Split Completed----------------------------");
            msg.append("\n");
            msg.append("     ");
            msg.append("Query ID: ");
            msg.append(splitCompletedEvent.getQueryId().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Stage ID: ");
            msg.append(splitCompletedEvent.getStageId().toString());
            msg.append("\n");
            msg.append("     ");
            msg.append("Task ID: ");
            msg.append(splitCompletedEvent.getTaskId().toString());

            logger.info(msg.toString());

        }
        catch (Exception ex) {
            logger.info(ex.getMessage());
        }

    }

    public void createLogFile()
    {

        SimpleDateFormat dateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String timeStamp = dateTime.format(new Date());
        StringBuilder logPath = new StringBuilder();

        logPath.append("/var/log/presto/queries-");
        logPath.append(timeStamp);
        logPath.append(".%g.log");

        try {
            logger = Logger.getLogger(loggerName);
            fh = new FileHandler(logPath.toString(), 524288000, 5, true);
            logger.addHandler(fh);
            logger.setUseParentHandlers(false);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        }
        catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    public void createQueryCacheFile()
    {

    }

}
