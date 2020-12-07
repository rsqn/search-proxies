package tech.rsqn.search.elasticsearch;

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import tech.rsqn.search.proxy.IndexEntry;
import tech.rsqn.search.proxy.SearchQuery;
import tech.rsqn.search.proxy.SearchResult;


/*
 * Tests require ElasticSearch to be running, you can start one in docker like so:
 * 
 * docker pull docker.elastic.co/elasticsearch/elasticsearch:7.10.0
 * docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.10.0
 * 
 */
public class ElasticSearchIndexTest {
    ElasticSearchIndex index;

    @BeforeClass
    public void setUp() throws Exception {
        index = new ElasticSearchIndex();
        index.setIndex("test");
        index.setConnectionParameters("localhost", 9200, "http");
        index.setWildCardFields(Arrays.asList("name","desc","ident"));
        
        populateIndex();
    }

    private void populateIndex() {
        index.beginBatch();

        IndexEntry entry = new IndexEntry();
        entry.setReference("1");
        entry.addTextAttr("name", "bob the dogs");
        entry.addTextAttr("desc", "bob is a very big dog that eats food");
        entry.addAttr("ident", "1234");
        entry.addAttr("somenumber", 56L);

        index.submitBatchEntry(entry);

        entry = new IndexEntry();
        entry.setReference("2");
        entry.addTextAttr("name", "dog the bog");
        entry.addTextAttr("desc", "this is a place where dogs velocity is reduced via viscosity");
        entry.addAttr("ident", "6789");
        entry.addAttr("somenumber", 56L);
        index.submitBatchEntry(entry);

        entry = new IndexEntry();
        entry.setReference("3");
        entry.addTextAttr("name", "nut butter");
        entry.addTextAttr("desc", "a delicious substance, sometimes liked by dogs");
        entry.addAttr("ident", "1011");
        entry.addAttr("somenumber", "57");

        index.submitBatchEntry(entry);

        index.endBatch();
    }

    @Test(groups = "integration-test")
    public void shouldFindByIdentField() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .and("ident","6789");
        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);
        Assert.assertEquals(result.getMatches().get(0).getIndexEntry().getReference(), "2");
    }


    @Test(groups = "integration-test")
    public void shouldFindByNameAndIdent() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .and("name","nut butter")
                .and("ident","6789");

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 2);
        Assert.assertEquals(result.getMatches().get(0).getIndexEntry().getReference(), "3");

    }

    @Test(groups = "integration-test")
    public void shouldFindUsingPartialDescriptionField() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .and("desc","velocity via viscosity");

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);
        Assert.assertEquals(result.getMatches().get(0).getIndexEntry().getReference(), "2");
    }

    @Test(groups = "integration-test")
    public void shouldFindUsingSinglePartOfDescriptionField() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .and("desc","velocity");

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);
        Assert.assertEquals(result.getMatches().get(0).getIndexEntry().getReference(), "2");
    }


    @Test(groups = "integration-test")
    public void shouldFindUsingPartialNameField() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .and("name","nut b");

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);
        Assert.assertEquals(result.getMatches().get(0).getIndexEntry().getReference(), "3");

    }

    @Test(groups = "integration-test")
    public void shouldFindInAnyConfigured() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .with("*","dog");

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 3);

    }

    @Test(groups = "integration-test")
    public void shouldFindByLongValue() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .with("somenumber",57L);

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);

    }
    
    @Test(groups = "integration-test")
    public void shouldFindByIntegerValue() throws Exception {
        SearchQuery query = new SearchQuery()
                .limit(10)
                .with("somenumber",57);

        SearchResult result = index.search(query);
        Assert.assertEquals(result.getMatches().size(), 1);

    }

}

