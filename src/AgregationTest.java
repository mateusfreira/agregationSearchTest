import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.Map;

/**
 * Created by mateus on 10/08/15.
 */

public class AgregationTest {

    public void testAgregation(){
        Avga avg = new Avga("teste", new String[]{}, BucketHelpers.GapPolicy.INSERT_ZEROS, null, null);
        avg.newDocument(10.00);
        avg.newDocument(15.00);
        avg.newDocument(50.00);
        System.out.println(avg.value().getValue());
        avg.newDocument(50.00);
        System.out.println(avg.value().getValue());

    }
}

class Avga extends AvgBucketPipelineAggregator {

    /**
     *
     * @param name
     * @param bucketsPaths
     * @param gapPolicy
     * @param formatter
     * @param metaData
     */
    protected Avga(String name, String[] bucketsPaths, BucketHelpers.GapPolicy gapPolicy, ValueFormatter formatter, Map<String, Object> metaData) {
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