package org.elasticsearch.search;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineSearcher;
import org.elasticsearch.index.search.stats.StatsGroupsParseElement;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import java.util.Date;

import org.elasticsearch.search.dfs.DfsPhase;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QueryPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
        //searchRequest
        ShardSearchTransportRequest shardSearchTransportRequest;
        shardSearchTransportRequest = new ShardSearchTransportRequest(searchRequest, ShardRouting.newUnassigned("index", 1, null, false, null), 0,
        null, new Date().getTime());

        SearchContext context = new DefaultSearchContext(
                incrementAndGet(),
                shardSearchTransportRequest,
                new SearchShardTarget("1", "index", 1),
                new Engine.Searcher("index", null),
                null,
                null,
                null,
                null,
                null,
                threadPool.estimatedTimeInMillisCounter(),
                ParseFieldMatcher.EMPTY, TimeValue.timeValueMillis(10000)
        );
        SearchContext.setCurrent(context);

        Node node = new Node(Settings.EMPTY, false);


        Map<String, SearchParseElement> elementParsers = new HashMap<>();
        elementParsers.putAll(node.injector().getInstance(DfsPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(QueryPhase.class).parseElements());
        elementParsers.putAll(node.injector().getInstance(FetchPhase.class).parseElements());
        elementParsers.put("stats", new StatsGroupsParseElement());


        parseSource(context, request.content(), ImmutableMap.copyOf(elementParsers));

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