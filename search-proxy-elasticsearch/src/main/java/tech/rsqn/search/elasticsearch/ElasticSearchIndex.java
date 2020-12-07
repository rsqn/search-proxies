package tech.rsqn.search.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.rsqn.search.proxy.Index;
import tech.rsqn.search.proxy.IndexAttribute;
import tech.rsqn.search.proxy.IndexEntry;
import tech.rsqn.search.proxy.IndexMetrics;
import tech.rsqn.search.proxy.SearchAttribute;
import tech.rsqn.search.proxy.SearchQuery;
import tech.rsqn.search.proxy.SearchResult;
import tech.rsqn.search.proxy.SearchResultItem;

public class ElasticSearchIndex implements Index {
    
    private static Logger log = LoggerFactory.getLogger(ElasticSearchIndex.class);
    
    public static final String ID_FIELD = "id";
    public static final String REFERENCE_FIELD = "reference";
    
    private RestHighLevelClient client;
    private String index;
    private List<String> wildCardFields;
    
    private BulkRequest bulkRequest;
    private int maxBatchSize = -1;
    
    public void setIndex(String index) {        
        this.index = index;
    }
    
    public void setConnectionParameters(String hostName, int port, String scheme) {
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(hostName, port, scheme));
        this.client = new RestHighLevelClient(clientBuilder);
    }
    
    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }
    
    public void setWildCardFields(List<String> wildCardFields) {
        this.wildCardFields = wildCardFields;
    }
    
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void submitSingleEntry(IndexEntry entry) {
        UpdateRequest updateRequest = buildUpdateRequest(entry);
        
        try {
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void submitBatchEntry(IndexEntry entry) {
        if(maxBatchSize > 0 && bulkRequest.numberOfActions() > maxBatchSize - 10) {
            synchronized(bulkRequest){
                if(bulkRequest.numberOfActions() == maxBatchSize) {
                    endBatch();
                    beginBatch();
                }
             }
        }

        bulkRequest.add(buildUpdateRequest(entry));
    }

    @Override
    public void beginBatch() {
        this.bulkRequest = new BulkRequest();
    }

    @Override
    public void endBatch() {
        try {
            if(bulkRequest != null && bulkRequest.numberOfActions() > 0) {
                BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if(response.hasFailures()) {
                    log.debug("There were failures in indexing bulk request: {}", response);
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public SearchResult search(String s, int max) {
        SearchRequest searchRequest = new SearchRequest(index); 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
        searchSourceBuilder.query(QueryBuilders.simpleQueryStringQuery(s)); 
        searchSourceBuilder.size(max);
        searchRequest.source(searchSourceBuilder);
        
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return elasticResponseToSearchResult(searchResponse);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public SearchResult search(SearchQuery query) {
        SearchRequest searchRequest = new SearchRequest(index); 
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
        searchSourceBuilder.query(searchQueryToElasticSearchQuery(query)).size(query.getLimit()*2).minScore(0.30f).from(query.getFrom()); 
        searchRequest.source(searchSourceBuilder); 

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);            
            SearchResult searchResult =  elasticResponseToSearchResult(searchResponse);
            searchResult.setLastIndex(searchResult.getLastIndex()+query.getFrom());
            
            return searchResult;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public IndexMetrics fetchMetrics() {
        // TODO Auto-generated method stub
        return new IndexMetrics();
    }

    @Override
    public void clearIndex() {
        // TODO Auto-generated method stub
        
    }
    
    private UpdateRequest buildUpdateRequest(IndexEntry entry) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.id(entry.getReference());
        updateRequest.doc(indexEntryToMap(entry));
        updateRequest.docAsUpsert(true);
    
        return updateRequest;
    }

    private Map<String, Object> indexEntryToMap(IndexEntry entry) {
        Map<String, Object> entryMap = new HashMap<>();

        entryMap.put(ID_FIELD, entry.getUid());
        entryMap.put(REFERENCE_FIELD, entry.getReference());
        
        for(Entry<String, IndexAttribute> entryAttribute : entry.getAttrs().entrySet()) {
            entryMap.put(entryAttribute.getKey(), entryAttribute.getValue().getAttrValue());
        }

        return entryMap;
    }
    
    private SearchResult elasticResponseToSearchResult(SearchResponse searchResponse) {
        SearchResult result = new SearchResult();
        
        List<SearchResultItem> matches = new ArrayList<>();
        for(SearchHit hit : searchResponse.getHits()) {
            SearchResultItem resultItem = new SearchResultItem();            
            resultItem.setScore(hit.getScore());
            
            IndexEntry indexEntry = new IndexEntry();
            
            indexEntry.setUid(hit.getSourceAsMap().get(ID_FIELD).toString());
            indexEntry.setReference(hit.getSourceAsMap().get(REFERENCE_FIELD).toString());
            
            for(Entry<String, Object> field : hit.getSourceAsMap().entrySet()) {
                indexEntry.addAttr(field.getKey(), field.getValue().toString());
            }
            resultItem.setIndexEntry(indexEntry);
            matches.add(resultItem);
        }
        
        result.setMatches(matches);
        result.setLastIndex(searchResponse.getHits().getHits().length);
        
        return result;
    }
    
    private QueryBuilder searchQueryToElasticSearchQuery(SearchQuery query) {        
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (SearchAttribute searchAttribute : query.getAttributes()) {
            if (SearchAttribute.WILDCARD_FIELD.equals(searchAttribute.getName())) {
                for (String wildCardField : wildCardFields) {
                    QueryBuilder q = searchAttributeToElasticSearchQuery(new SearchAttribute().with(wildCardField, searchAttribute.getPattern()));
                    queryBuilder.should(q);
                }
            } else {
                QueryBuilder q = searchAttributeToElasticSearchQuery(searchAttribute);
                queryBuilder.should(q);
            }
        }

        return queryBuilder;
    }

    private QueryBuilder searchAttributeToElasticSearchQuery(SearchAttribute searchAttribute) {
        QueryBuilder queryBuilder;
        
        if (SearchAttribute.Type.FUZZY == searchAttribute.getMatchType()) {
            queryBuilder = QueryBuilders.matchQuery(searchAttribute.getName(), searchAttribute.getPattern()).fuzziness(Fuzziness.AUTO);
        } else if (SearchAttribute.Type.EQ == searchAttribute.getMatchType()) {
            queryBuilder = QueryBuilders.termQuery(searchAttribute.getName(), (Object)searchAttribute.getPattern());
        } else if (SearchAttribute.Type.GTE == searchAttribute.getMatchType()) {
            queryBuilder = QueryBuilders.rangeQuery(searchAttribute.getName()).from((Object)searchAttribute.getPattern());
        } else if (SearchAttribute.Type.LTE == searchAttribute.getMatchType()) {
            queryBuilder = QueryBuilders.rangeQuery(searchAttribute.getName()).to((Object)searchAttribute.getPattern());
        } else if (SearchAttribute.Type.BETWEEN == searchAttribute.getMatchType()) {
            Long[] values = searchAttribute.getPattern();
            queryBuilder = QueryBuilders.rangeQuery(searchAttribute.getName())
                    .from(values[0])
                    .to(values[1]);
        }else {
            throw new RuntimeException("Unsupported search attribute type");
        }
        
        return queryBuilder;
    }
}
