package org.elasticsearch.module.graphite.test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.Iterables;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.inject.ProvisionException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.Collection;

import static com.google.common.base.Predicates.containsPattern;
import static org.elasticsearch.module.graphite.test.NodeTestHelper.createNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GraphitePluginIntegrationTest extends ESIntegTestCase {

    public static final int GRAPHITE_SERVER_PORT = 12345;

    private GraphiteMockServer graphiteMockServer;

    private String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
    private String index = UUID.randomUUID().toString().replaceAll("-", "");
    private String type = UUID.randomUUID().toString().replaceAll("-", "");
    private Node node;

    public GraphitePluginIntegrationTest() {
        super();
    }
    
    @Test
    public void test() {
        System.out.println("Test OK");
    }

    @Before
    public void startGraphiteMockServerAndNode() throws Exception {
        graphiteMockServer = new GraphiteMockServer(GRAPHITE_SERVER_PORT);
        graphiteMockServer.start();
    }

    @After
    public void stopGraphiteServer() throws Exception {
        if (graphiteMockServer != null && !graphiteMockServer.isInterrupted()) {
            graphiteMockServer.close();
        }
//        if (node != null && !node.isClosed()) {
//            node.close();
//        }
    }
    
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        
        Settings.Builder settingsBuilder = Settings.settingsBuilder();
        settingsBuilder.put(super.nodeSettings(nodeOrdinal));
        
        System.out.println(settingsBuilder.build().toDelimitedString(";".charAt(0)));
        
        settingsBuilder.put("path.conf", GraphitePluginIntegrationTest.class.getResource("/").getFile());
        //settingsBuilder.put("path.home", "./target/data");
        //settingsBuilder.put("path.data", "./target/data");      
        //settingsBuilder.put("gateway.type", "none");
        //settingsBuilder.put("cluster.name", clusterName);
        //settingsBuilder.put("index.number_of_shards", 1);
        //settingsBuilder.put("index.number_of_replicas", 1);

        settingsBuilder.put("metrics.graphite.host", "localhost");
        settingsBuilder.put("metrics.graphite.port", GRAPHITE_SERVER_PORT);
        settingsBuilder.put("metrics.graphite.every", "1s");
        if (!Strings.isEmpty("")) {
            settingsBuilder.put("metrics.graphite.prefix", "");
        }

        if (Strings.hasLength("")) {
            settingsBuilder.put("metrics.graphite.include", "");
        }

        if (Strings.hasLength("")) {
            settingsBuilder.put("metrics.graphite.exclude", "");
        }
        return settingsBuilder.build();
    }    

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(org.elasticsearch.plugin.graphite.GraphitePlugin.class);
    }    
    
//    @Test
//    public void testThatIndexingResultsInMonitoring() throws Exception {
//        assertThat("foobar", is(notNullValue()));
////        node = createNode(clusterName,  GRAPHITE_SERVER_PORT, "1s");
////        IndexResponse indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        Thread.sleep(2000);
////
////        ensureValidKeyNames();
////        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
////        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing." + type + ".indexCount 1");
////        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".indexes." + index + ".id.0.search._all.queryCount ");
////        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".node.jvm.threads.peakCount ");
////        assertGraphiteMetricIsContained("^elasticsearch." + clusterName + ".node.search._all.queryCount ");
//    }
//
//    @Test
//    public void testThatFieldExclusionWorks() throws Exception {
////        String excludeRegex = ".*\\.peakCount";
////        node = createNode(clusterName, GRAPHITE_SERVER_PORT, "1s", null, excludeRegex, null);
////
////        IndexResponse indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        Thread.sleep(2000);
////
////        ensureValidKeyNames();
////        // ensure no global exclusion
////        assertGraphiteMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
////        assertGraphiteMetricIsNotContained("elasticsearch." + clusterName + ".node.jvm.threads.peakCount ");
//    }
//
//    @Test
//    public void testThatFieldExclusionWorksWithPrefix() throws Exception {
////        String prefix = "my.awesome.prefix";
////        String excludeRegex = prefix + ".node.[http|jvm].*";
////        node = createNode(clusterName, GRAPHITE_SERVER_PORT, "1s", null, excludeRegex, prefix);
////
////        IndexResponse indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        Thread.sleep(2000);
////
////        ensureValidKeyNames();
////        // ensure no global exclusion
////        assertGraphiteMetricIsContained(prefix + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
////        assertGraphiteMetricIsNotContained(prefix + ".node.jvm.threads.peakCount ");
////        assertGraphiteMetricIsNotContained(prefix + ".node.http.totalOpen ");
//    }
//
//    @Test
//    public void testThatFieldInclusionWinsOverExclusion() throws Exception {
////        String excludeRegex = ".*" + clusterName + ".*";
////        String includeRegex = ".*\\.peakCount";
////        node = createNode(clusterName, GRAPHITE_SERVER_PORT, "1s", includeRegex, excludeRegex, null);
////
////        IndexResponse indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        SearchResponse searchResponse = searchElement(node);
////        assertThat(searchResponse.status(), is(notNullValue()));
////
////        Thread.sleep(2000);
////
////        ensureValidKeyNames();
////        assertGraphiteMetricIsNotContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
////        assertGraphiteMetricIsContained("elasticsearch." + clusterName + ".node.jvm.threads.peakCount ");
//    }
//
//    @Test(expected = ProvisionException.class)
//    public void testThatBrokenRegexLeadsToException() throws Exception {
////        String excludeRegex = "*.peakCount";
////        createNode(clusterName, GRAPHITE_SERVER_PORT, "1s", null, excludeRegex, null);
//    }
//
//
//    @Test
//    public void masterFailOverShouldWork() throws Exception {
////        node = createNode(clusterName, GRAPHITE_SERVER_PORT, "1s");
////        String clusterName = UUID.randomUUID().toString().replaceAll("-", "");
////        IndexResponse indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        Node origNode = node;
////        node = createNode(clusterName, GRAPHITE_SERVER_PORT, "1s");
////        graphiteMockServer.content.clear();
////        origNode.close();
////        indexResponse = indexElement(node, index, type, "value");
////        assertThat(indexResponse.getId(), is(notNullValue()));
////
////        // wait for master fail over and writing to graph reporter
////        Thread.sleep(2000);
////        assertGraphiteMetricIsContained("elasticsearch." + clusterName + ".indexes." + index + ".id.0.indexing._all.indexCount 1");
//    }
//
//    // the stupid hamcrest matchers have compile erros depending whether they run on java6 or java7, so I rolled my own version
//    // yes, I know this sucks... I want power asserts, as usual
//    private void assertGraphiteMetricIsContained(final String id) {
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern(id)), is(true));
//    }
//
//    private void assertGraphiteMetricIsNotContained(final String id) {
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern(id)), is(false));
//    }
//
//    // Make sure no elements with a chars [] are included
//    private void ensureValidKeyNames() {
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\.\\.")), is(false));
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\[")), is(false));
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\]")), is(false));
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\(")), is(false));
////        assertThat(Iterables.any(graphiteMockServer.content, containsPattern("\\)")), is(false));
//    }
//
//    private IndexResponse  indexElement(Node node, String index, String type, String fieldValue) {
//        return node.client().prepareIndex(index, type).
//                setSource("field", fieldValue)
//                .execute().actionGet();
//    }
//
//    private SearchResponse  searchElement(Node node) {
//        return node.client().prepareSearch().execute().actionGet();
//    }
}
