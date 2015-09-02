package org.elasticsearch.search.aggregations.pipeline;

import com.appbase.elasticsearch.search.aggregation.pipeline.AvgBucketPipelineAggregator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.ArrayList;

/**
 * Created by mateus on 23/08/15.
 */
public class ClientUtil {


    public static void main(String[] args) {
        try {
            ScoreDoc[] sortedDocs = null;
            AtomicArray<QueryFetchSearchResult> queryResultsArr = new AtomicArray<>(10);
            QuerySearchResult result = new  QuerySearchResult();
            String name = "avg_calc";
            String[] bucketsPaths = new String[] { "avg_calc" };
            BucketHelpers.GapPolicy gapPolicy = BucketHelpers.GapPolicy.INSERT_ZEROS;
            ValueFormatter formatter = null;

            SiblingPipelineAggregator internal =(SiblingPipelineAggregator) new AvgBucketPipelineAggregator
                    .Factory(name, bucketsPaths, gapPolicy, formatter)
                    .create();

            ArrayList<SiblingPipelineAggregator> internalAggregations = new ArrayList<SiblingPipelineAggregator>();
            internalAggregations.add(internal);
            ArrayList<InternalAggregation> iAggregations = new ArrayList<InternalAggregation>();


            internalAggregations.add(internal);
            InternalAggregations iags = new InternalAggregations(iAggregations);
            result.aggregations(iags);
            result.pipelineAggregators(internalAggregations);
            result.topDocs(new TopDocs(1, null, 10.0f));
            queryResultsArr.set(0, new QueryFetchSearchResult(result, new FetchSearchResult()));
            AtomicArray<QueryFetchSearchResult> fetchResultsArr = new AtomicArray<>(10);
            InternalSearchResponse respose =
                    new SearchPhaseControllerUtil(Settings.builder().build(), null, null)
                            .merge(sortedDocs, queryResultsArr, fetchResultsArr);

            System.out.println(respose.hits().toString());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

class SearchPhaseControllerUtil extends SearchPhaseController{

    public SearchPhaseControllerUtil(Settings settings, BigArrays bigArrays, ScriptService scriptService) {
        super(settings, bigArrays, scriptService);
    }
}