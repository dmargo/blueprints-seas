package com.tinkerpop.blueprints.pgm.impls.neo4j;

import com.tinkerpop.blueprints.BaseTest;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Index;
import com.tinkerpop.blueprints.pgm.IndexableGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4jbatch.Neo4jBatchGraph;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jBatchGraphTest extends BaseTest {

    public void testAddingVerticesEdges() {
        final String directory = this.getWorkingDirectory();
        final Neo4jBatchGraph batch = new Neo4jBatchGraph(directory);
        final List<Long> ids = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            ids.add((Long) batch.addVertex(null).getId());
        }
        for (int i = 1; i < ids.size(); i++) {
            long idA = ids.get(i - 1);
            long idB = ids.get(i);
            batch.addEdge(null, batch.getVertex(idA), batch.getVertex(idB), idA + "-" + idB);
        }
        batch.shutdown();

        final Graph graph = new Neo4jGraph(directory);
        graph.removeVertex(graph.getVertex(0)); // remove reference node
        assertEquals(count(graph.getVertices()), 10);

        assertEquals(count(graph.getEdges()), 9);
        for (final Edge edge : graph.getEdges()) {
            long idA = (Long) edge.getOutVertex().getId();
            long idB = (Long) edge.getInVertex().getId();
            assertEquals(idA + 1, idB);
            assertEquals(edge.getLabel(), idA + "-" + idB);
        }

        graph.shutdown();
    }

    public void testAddingVerticesEdgesWithIndices() {
        final String directory = this.getWorkingDirectory();
        final Neo4jBatchGraph batch = new Neo4jBatchGraph(directory);
        batch.createAutomaticIndex(Index.VERTICES, Vertex.class, new HashSet<String>(Arrays.asList("name", "age")));
        Index<Edge> edgeIndex = batch.createManualIndex(Index.EDGES, Edge.class);

        assertEquals(count(batch.getIndices()), 2);

        final List<Long> ids = new ArrayList<Long>();
        for (int i = 0; i < 10; i++) {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put("name", i + "");
            map.put("age", i * 10);
            map.put("nothing", 0);
            ids.add((Long) batch.addVertex(map).getId());
        }
        for (int i = 1; i < ids.size(); i++) {
            final Map<String, Object> map = new HashMap<String, Object>();
            map.put("weight", 0.5f);
            long idA = ids.get(i - 1);
            long idB = ids.get(i);
            final Edge edge = batch.addEdge(map, batch.getVertex(idA), batch.getVertex(idB), idA + "-" + idB);
            edgeIndex.put("unique", idA + "-" + idB, edge);
            edgeIndex.put("full", "blah", edge);
        }
        batch.flushIndices();
        batch.shutdown();

        final IndexableGraph graph = new Neo4jGraph(directory);
        Index<Vertex> vertexIndex = graph.getIndex(Index.VERTICES, Vertex.class);
        edgeIndex = graph.getIndex(Index.EDGES, Edge.class);
        graph.removeVertex(graph.getVertex(0)); // remove reference node
        assertEquals(count(graph.getVertices()), 10);

        for (final Vertex vertex : graph.getVertices()) {
            int age = (Integer) vertex.getProperty("age");
            assertEquals(vertex.getProperty("name"), (age / 10) + "");

            assertEquals(count(vertexIndex.get("nothing", 0)), 0);
            assertEquals(count(vertexIndex.get("age", age)), 1);
            assertEquals(vertexIndex.get("age", age).iterator().next(), vertex);
            assertEquals(count(vertexIndex.get("name", (age / 10) + "")), 1);
            assertEquals(vertexIndex.get("name", (age / 10) + "").iterator().next(), vertex);
        }
        assertEquals(count(graph.getEdges()), 9);
        assertEquals(count(edgeIndex.get("full", "blah")), 9);
        Set<Edge> edges = new HashSet<Edge>();
        for (Edge edge : edgeIndex.get("full", "blah")) {
            edges.add(edge);
        }
        assertEquals(edges.size(), 9);
        for (final Edge edge : graph.getEdges()) {
            long idA = (Long) edge.getOutVertex().getId();
            long idB = (Long) edge.getInVertex().getId();
            assertEquals(idA + 1, idB);
            assertEquals(edge.getLabel(), idA + "-" + idB);
            assertEquals(edge.getProperty("weight"), 0.5f);

            assertEquals(count(edgeIndex.get("weight", 0.5f)), 0);
            assertEquals(count(edgeIndex.get("unique", idA + "-" + idB)), 1);
            assertEquals(edgeIndex.get("unique", idA + "-" + idB).iterator().next(), edge);
            assertTrue(edges.contains(edge));
        }


        graph.shutdown();
    }

    private String getWorkingDirectory() {
        String directory = System.getProperty("neo4jBatchGraphDirectory");
        if (directory == null) {
            if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
                directory = "C:/temp/blueprints_test";
            else
                directory = "/tmp/blueprints_test";
        }
        deleteDirectory(new File(directory));
        return directory;
    }
}