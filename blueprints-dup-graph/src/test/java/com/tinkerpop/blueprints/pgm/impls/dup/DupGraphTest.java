package com.tinkerpop.blueprints.pgm.impls.dup;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;
import com.tinkerpop.blueprints.pgm.util.graphml.GraphMLReaderTestSuite;

import java.io.File;
import java.lang.reflect.Method;

/**
 * Test suite for BDB graph implementation.
 *
 * @author Daniel Margo (http://eecs.harvard.edu/~dmargo)
 */
public class DupGraphTest extends GraphTest {

    public DupGraphTest() {
        /* INSERT CORRECT VALUES HERE */
        this.allowsDuplicateEdges = false;
        this.allowsSelfLoops = true;
        this.isPersistent = true;
        this.isRDFModel = false;
        this.supportsVertexIteration = true;
        this.supportsEdgeIteration = true;
        this.supportsVertexIndex = false;
        this.supportsEdgeIndex = false;
        this.ignoresSuppliedIds = true;
        this.supportsTransactions = false;
    }

    /*public void testDupBenchmarkTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new DupBenchmarkTestSuite(this));
        printTestPerformance("DupBenchmarkTestSuite", this.stopWatch());
    }*/
    
    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }
    
    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }
    
    public void testGraphMLReaderTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphMLReaderTestSuite(this));
        printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
    }

    /*public void testTransactionalGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new TransactionalGraphTestSuite(this));
        printTestPerformance("TransactionGraphTestSuite", this.stopWatch());
    }

    public void testIndexableGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexableGraphTestSuite(this));
        printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
    }

    public void testIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new IndexTestSuite(this));
        printTestPerformance("IndexTestSuite", this.stopWatch());
    }

    public void testAutomaticIndexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new AutomaticIndexTestSuite(this));
        printTestPerformance("AutomaticIndexTestSuite", this.stopWatch());
    }*/

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testDupGraph");
        if (doTest == null || doTest.equals("true")) {
            // Need to get 'dupGraphDirectory' or an appropriate default;
            // see other tests for examples.
            String directory = getWorkingDirectory();
            deleteDirectory(new File(directory));
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                    // The precise cleanup you'll need to do here is
                    // implementation-dependent; see other tests.
                    deleteDirectory(new File(directory));
                }
            }
        }
    }
    
    public Graph getGraphInstance() {
    	// Here as well, you'll ultimately want to System.getProperty
        return new DupGraph(getWorkingDirectory());
    }
    
    private String getWorkingDirectory() {
        String directory = System.getProperty("dupGraphDirectory");
        if (directory == null) {
            if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
                directory = "C:/temp/blueprints_test";
            else
                directory = "/tmp/blueprints_test";
        }
        return directory;
    }
    
}
