package com.amazonaws.QueryEventListener;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PrestoCloudWatchWrapper {

    Logger logger = Logger.getLogger(PrestoCloudWatchWrapper.class.getName());


    private static AmazonCloudWatch cloudWatch = AmazonCloudWatchClientBuilder.defaultClient();
    private static String PRESTO_CLOUDWATCH_NAMESPACE = "PRESTO/%s";

    public void putPrestoMetricToCloudWatch
            (HashMap<String, String> dimensions, String metricName, Double value, String namespace)
    {
        Dimension dimension = new Dimension();

        List<Dimension> dimensionsList = dimensions.entrySet().stream()
                .map(entry -> dimension.withName(entry.getKey()).withValue(entry.getValue()))
                .collect(Collectors.toList());

        MetricDatum metricDatum = new MetricDatum()
                .withMetricName(metricName)
                .withUnit(StandardUnit.Count)
                .withValue(value)
                .withDimensions(dimensionsList);

        PutMetricDataRequest putMetricDataRequest = new PutMetricDataRequest()
                .withNamespace(String.format(PRESTO_CLOUDWATCH_NAMESPACE, namespace))
                .withMetricData(metricDatum);
        PutMetricDataResult response = cloudWatch.putMetricData(putMetricDataRequest);
        logger.info(String.format("%s", response.toString()));
    }

}
