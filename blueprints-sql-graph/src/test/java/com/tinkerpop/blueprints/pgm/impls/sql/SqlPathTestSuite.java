package com.tinkerpop.blueprints.pgm.impls.sql;

import java.util.ArrayList;
import java.util.List;

import com.tinkerpop.blueprints.pgm.TestSuite;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.GraphTest;

/**
 * @author Daniel Margo (http://eecs.harvard.edu/~dmargo)
 */
public class SqlPathTestSuite extends TestSuite {
	
    public SqlPathTestSuite() {
    }

    public SqlPathTestSuite(final GraphTest graphTest) {
        super(graphTest);
    }

    public void testSqlGraph() throws Exception {
        SqlGraph graph = (SqlGraph) graphTest.getGraphInstance();
        for (int i = 0; i < 20; ++i) {
        	graph.addVertex(null);
        }
        List<Vertex> vertices = new ArrayList<Vertex>();
        for (Vertex v : graph.getVertices()) {
        	vertices.add(v);
        }
        for (int i = 0; i < 19; ++i) {
        	graph.addEdge(null, vertices.get(i), vertices.get(i + 1), null);
        }
        System.out.print("\t");
        for (Vertex v : graph.getShortestPath(vertices.get(0), vertices.get(19))) {
        	System.out.print("" + v + " ");
        }
        System.out.println();
        graph.shutdown();
    }
}
