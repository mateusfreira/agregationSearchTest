package org.elasticsearch.search;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.cache.query.QueryCacheModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineSearcher;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

import java.io.IOException;
import java.util.*;

import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Created by mateus on 29/08/15.
 */
public class SearchParse {

    private static long i = 0l;
    private static long incrementAndGet(){
        return ++i;
    }

    public static void main(String[] args){

        SearchRequest searchRequest;
        RestRequest request = new FixRestRequest();
        ThreadPool threadPool = new ThreadPool("active");
        searchRequest = RestSearchAction.parseSearchRequest(request, ParseFieldMatcher.EMPTY);
        Settings settings = Settings
                .builder()
                .put("path.home", "/tmp/")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();


        Node node = new Node(settings, false);
        Index index = new Index("index");


        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, node.injector().getInstance(IndicesAnalysisService.class)),
                new SimilarityModule(settings),
                new QueryCacheModule(settings)
        )
                .createChildInjector(node.injector());
        //searchRequest
        ShardSearchTransportRequest shardSearchTransportRequest;
        shardSearchTransportRequest = new ShardSearchTransportRequest(searchRequest, ShardRouting.newUnassigned("index", 1, null, false, null), 0,
        null, new Date().getTime());
        Engine.Searcher searcher =  new Engine.Searcher("index", new IndexSearcher(new MyIndexReader()));
        //SearcherFactory factory = new SearcherFactory();
        BigArrays bigArrays = node.injector().getInstance(BigArrays.class);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);
        //MapperService mapperService = injector.getInstance(MapperService.class);
        MapperService mapperService = new MapperService(index, settings, analysisService,
                null,
                node.injector().getInstance(ScriptService.class));

        IndexService indexService = injector.getInstance(IndexService.class);


        SearchContext context = new DefaultSearchContext(
                incrementAndGet(),
                shardSearchTransportRequest,
                new SearchShardTarget("1", "index", 1),
                searcher,
                indexService,
                null,
                null,
                null,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter(),
                ParseFieldMatcher.EMPTY, TimeValue.timeValueMillis(10000)
        );
        SearchContext.setCurrent(context);




        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(node.injector().getInstance(DfsPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(QueryPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(FetchPhase.class).parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());


        parseSource(context, request.content(), ImmutableMap.copyOf(elementParsers));
        AggregationPhase aggregationPhase= injector.getInstance(AggregationPhase.class);
        aggregationPhase.preProcess(context);
        aggregationPhase.execute(context);
        ScoreDoc[] sortedDocs = null;
        //AtomicArray query = new AtomicArray<>(context.queryResult());
        InternalSearchResponse respose =
                new SearchPhaseControllerUtil(Settings.builder().build(), null, null)
                        .merge(sortedDocs, xcontext.queryResult(, context.fetchResult());

    }
    private static void parseSource(SearchContext context, BytesReference source, ImmutableMap<String, SearchParseElement> elementParsers) throws SearchParseException {
        // nothing to parse...
        if (source == null || source.length() == 0) {
            return;
        }
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            XContentParser.Token token;
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ElasticsearchParseException("failed to parse search source. source must be an object, but found [{}] instead", token.name());
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context, "failed to parse search source. unknown search element [" + fieldName + "]", parser.getTokenLocation());
                    }
                    element.parse(parser, context);
                } else {
                    if (token == null) {
                        throw new ElasticsearchParseException("failed to parse search source. end of query source reached but query is not complete.");
                    } else {
                        throw new ElasticsearchParseException("failed to parse search source. expected field name but got [{}]", token);
                    }
                }
            }
        } catch (Throwable e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            XContentLocation location = parser != null ? parser.getTokenLocation() : null;
            throw new SearchParseException(context, "failed to parse search source [" + sSource + "]", location, e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
class FixRestRequest extends RestRequest{

    @Override
    public Method method() {
        return Method.GET;
    }

    @Override
    public String uri() {
        return "";
    }

    @Override
    public String rawPath() {
        return "/index/search";
    }

    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    public BytesReference content() {
        return new BytesArray("{  \"size\": 0,  \"aggs\": {    \"group_by_state\": {      \"terms\": {        \"field\": \"year\"      },      \"aggs\": {        \"average_balance\": {          \"avg\": {            \"field\": \"year\"          }        }      }    }  }}".getBytes());
    }

    @Override
    public String header(String name) {
        return null;
    }

    @Override
    public Iterable<Map.Entry<String, String>> headers() {
        return new HashSet<>();
    }

    @Override
    public boolean hasParam(String key) {
        return false;
    }

    @Override
    public String param(String key) {
        return null;
    }

    @Override
    public String param(String key, String defaultValue) {
        return null;
    }

    @Override
    public Map<String, String> params() {
        return new HashMap<>();
    }
}
class MyIndexReader extends CompositeReader{


    @Override
    protected List<? extends IndexReader> getSequentialSubReaders() {
        return new ArrayList<>();
    }

    @Override
    public Fields getTermVectors(int i) throws IOException {
        return null;
    }

    @Override
    public int numDocs() {
        return 0;
    }

    @Override
    public int maxDoc() {
        return 0;
    }

    @Override
    public void document(int i, StoredFieldVisitor storedFieldVisitor) throws IOException {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public int docFreq(Term term) throws IOException {
        return 0;
    }

    @Override
    public long totalTermFreq(Term term) throws IOException {
        return 0;
    }

    @Override
    public long getSumDocFreq(String s) throws IOException {
        return 0;
    }

    @Override
    public int getDocCount(String s) throws IOException {
        return 0;
    }

    @Override
    public long getSumTotalTermFreq(String s) throws IOException {
        return 0;
    }
}
class SearchPhaseControllerUtil extends SearchPhaseController {

    public SearchPhaseControllerUtil(Settings settings, BigArrays bigArrays, ScriptService scriptService) {
        super(settings, bigArrays, scriptService);
    }
}