import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregator;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by mateus on 10/08/15.
 */

public class MaxTest {

    public void testAgregation(){

        Maxa max = null;
        max = new Maxa("teste", 0.00, null, null,  null);

        max = max.newDocument(10.00);
        max = max.newDocument(15.00);
        max = max.newDocument(50.00);
        System.out.println(max.value());
        max = max.newDocument(5l);
        System.out.println(max.value());

    }
}

class Maxa extends InternalMax {


    public Maxa(String name, double max, ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, max, formatter, pipelineAggregators, metaData);
    }
    public Maxa(InternalMax max){
        super("", max.getValue(), null, null, null);
    }

    /**
     *
     * @param d
     */
    public Maxa newDocument(double  d){
        List<InternalAggregation> list = new ArrayList<InternalAggregation>();
        list.add(new Maxa("", d, null, null, null));
        list.add(this);
        return new Maxa(this.doReduce(list, null));
    }
    /*
    public InternalSimpleValue value(){
        return (InternalSimpleValue) this.buildAggregation(0);
    }*/
}