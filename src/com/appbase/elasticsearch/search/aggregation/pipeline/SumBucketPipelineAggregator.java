package com.appbase.elasticsearch.search.aggregation.pipeline;

import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.Map;

/**
 * Created by mateus on 20/08/15.
 */
public class SumBucketPipelineAggregator extends org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregator {

    protected SumBucketPipelineAggregator(String name, String[] bucketsPaths, BucketHelpers.GapPolicy gapPolicy, ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, bucketsPaths, gapPolicy, formatter, metaData);
    }
    /**
     *
     * @param d
     */
    public void newDocument(Double d){
        this.collectBucketValue(null, d);
    }
    public InternalSimpleValue value(){
        return (InternalSimpleValue) this.buildAggregation(null, null);
    }
}
