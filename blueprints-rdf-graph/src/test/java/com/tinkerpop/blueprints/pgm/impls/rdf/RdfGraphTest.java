package com.tinkerpop.blueprints.pgm.impls.rdf;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
//import com.tinkerpop.blueprints.pgm.impls.rdf.impls.MemoryStoreRdfGraph;
import com.tinkerpop.blueprints.pgm.impls.rdf.impls.NativeStoreRdfGraph;

import java.io.File;
import java.lang.reflect.Method;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RdfGraphTest extends GraphTest {

    public RdfGraphTest() {
        this.allowsDuplicateEdges = false;
        this.allowsSelfLoops = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = false;
        this.supportsEdgeIndex = false;
        this.ignoresSuppliedIds = true;
    }

    /*
    public void testRdfGraphFactory() {
        assertTrue(true);
        RdfGraphFactory.createTinkerGraph(new MemoryStoreRdfGraph());
    }

    public void testTypeConversion() {
        assertEquals(SailVertex.castLiteral(new LiteralImpl("marko", new URIImpl("http://www.w3.org/2001/XMLSchema#string"))).getClass(), String.class);
        assertEquals(SailVertex.castLiteral(new LiteralImpl("marko")).getClass(), String.class);
        assertEquals(SailVertex.castLiteral(new LiteralImpl("27", new URIImpl("http://www.w3.org/2001/XMLSchema#int"))).getClass(), Integer.class);
        assertEquals(SailVertex.castLiteral(new LiteralImpl("27", new URIImpl("http://www.w3.org/2001/XMLSchema#float"))).getClass(), Float.class);
        assertEquals(SailVertex.castLiteral(new LiteralImpl("27.0134", new URIImpl("http://www.w3.org/2001/XMLSchema#double"))).getClass(), Double.class);
        assertEquals(SailVertex.castLiteral(new LiteralImpl("hello", "en")), "hello");
    }

    public void testNamespaceConversion() throws Exception {
        SailGraph graph = new MemoryStoreSailGraph();
        graph.addNamespace("tg", "http://tinkerpop.com#");
        graph.addNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        assertEquals(SailGraph.prefixToNamespace("tg:name", graph.getSailConnection()), "http://tinkerpop.com#name");
        assertEquals(SailGraph.prefixToNamespace("rdf:label", graph.getSailConnection()), "http://www.w3.org/1999/02/22-rdf-syntax-ns#label");
        assertEquals(SailGraph.namespaceToPrefix("http://www.w3.org/1999/02/22-rdf-syntax-ns#label", graph.getSailConnection()), "rdf:label");
        assertEquals(SailGraph.namespaceToPrefix("http://tinkerpop.com#name", graph.getSailConnection()), "tg:name");
        graph.shutdown();

    }

    public void testURIs() {
        assertFalse(SailHelper.isURI("_:1234"));
        assertFalse(SailHelper.isURI("_:abcdefghijklmnopqrstuvwxyz"));
        assertTrue(SailHelper.isURI("http://marko"));
        assertTrue(SailHelper.isURI("http://www.w3.org/2001/XMLSchema#string"));
    }

    public void testBNodes() {
        assertTrue(SailHelper.isBNode("_:1234"));
        assertTrue(SailHelper.isBNode("_:abcdefghijklmnopqrstuvwxyz"));
        assertFalse(SailHelper.isBNode("_:"));
        assertFalse(SailHelper.isBNode("http://marko"));
        assertFalse(SailHelper.isBNode("http://www.w3.org/2001/XMLSchema#string"));
    }

    public void testLiterals() {
        assertTrue(SailHelper.isLiteral("\"java\"^^<http://www.w3.org/2001/XMLSchema#string>"));
        assertFalse(SailHelper.isLiteral("http://www.w3.org/2001/XMLSchema#string"));
        assertFalse(SailHelper.isLiteral("^^<http://www.w3.org/2001/XMLSchema#string>"));
        assertTrue(SailHelper.isLiteral("\"\"^^<http://www.w3.org/2001/XMLSchema#string>"));
        assertTrue(SailHelper.isLiteral("\"\""));
        assertTrue(SailHelper.isLiteral("\"marko\""));
        assertFalse(SailHelper.isLiteral("\"marko\"marko"));
        assertFalse(SailHelper.isLiteral("\""));
        // TODO: make this true assertFalse(SesameGraph.isLiteral("\"marko\"marko\""));


        Matcher matcher = SailHelper.literalPattern.matcher("\"java\"^^<http://www.w3.org/2001/XMLSchema#string>");
        matcher.matches();
        assertNull(matcher.group(6));
        assertEquals(matcher.group(1), "java");
        assertEquals(matcher.group(4), "http://www.w3.org/2001/XMLSchema#string");

        matcher = SailHelper.literalPattern.matcher("\"java\"@en");
        matcher.matches();
        assertNull(matcher.group(4));
        assertEquals(matcher.group(1), "java");
        assertEquals(matcher.group(6), "en");
    }

    public void testLiteralProperties() {
        SailGraph graph = new MemoryStoreSailGraph();
        Vertex v = graph.getVertex("\"java\"^^<http://www.w3.org/2001/XMLSchema#string>");
        assertEquals(v.getProperty(SailTokens.VALUE), "java");
        assertEquals(v.getProperty(SailTokens.DATATYPE), "http://www.w3.org/2001/XMLSchema#string");
        assertNull(v.getProperty(SailTokens.LANGUAGE));
        assertEquals(v.getProperty(SailTokens.KIND), "literal");

        v = graph.getVertex("\"10\"^^<http://www.w3.org/2001/XMLSchema#int>");
        assertEquals(v.getProperty(SailTokens.VALUE), 10);
        assertEquals(v.getProperty(SailTokens.DATATYPE), "http://www.w3.org/2001/XMLSchema#int");
        assertNull(v.getProperty(SailTokens.LANGUAGE));
        assertEquals(v.getProperty(SailTokens.KIND), "literal");

        v = graph.getVertex("\"goodbye\"@en");
        assertEquals(v.getProperty(SailTokens.VALUE), "goodbye");
        assertEquals(v.getProperty(SailTokens.LANGUAGE), "en");
        assertNull(v.getProperty(SailTokens.DATATYPE));
        assertEquals(v.getProperty(SailTokens.KIND), "literal");

    }

    public void testValueKinds() {
        SailGraph graph = new MemoryStoreSailGraph();
        Vertex v = graph.getVertex("\"java\"^^<http://www.w3.org/2001/XMLSchema#string>");
        assertEquals(v.getProperty(SailTokens.KIND), "literal");

        v = graph.getVertex("http://markorodriguez.com");
        assertEquals(v.getProperty(SailTokens.KIND), "uri");

        v = graph.getVertex("_:123");
        assertEquals(v.getProperty(SailTokens.KIND), "bnode");
    }

    public void testSparql() {
        SailGraph graph = new MemoryStoreSailGraph();
        SailGraphFactory.createTinkerGraph(graph);

        String query = "SELECT ?x ?y WHERE { ?x tg:knows ?y }";
        this.stopWatch();

        List<Map<String, Vertex>> results = graph.executeSparql(query);
        assertEquals(results.size(), 2);
        for (Map<String, Vertex> map : results) {
            assertEquals(map.get("x"), graph.getVertex("tg:1"));
            assertTrue(map.get("y").equals(graph.getVertex("tg:2")) || map.get("y").equals(graph.getVertex("tg:4")));
        }
        graph.shutdown();
    }
    */

    //// TEST SUITES

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    public void testGraphSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    /*
    public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionalGraphTestSuite", this.stopWatch());
    }
    */

    public Graph getGraphInstance() {
        //return new RdfGraph(new MemoryStore());
    	return new NativeStoreRdfGraph("/tmp/rdfgraph");
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testRdf");
        if (doTest == null || doTest.equals("true")) {
        	deleteDirectory(new File("/tmp/rdfgraph"));
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                	deleteDirectory(new File("/tmp/rdfgraph"));
                }
            }
        }
    }
}
