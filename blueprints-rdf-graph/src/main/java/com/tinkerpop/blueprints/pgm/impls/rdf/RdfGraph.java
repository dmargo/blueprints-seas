package com.tinkerpop.blueprints.pgm.impls.rdf;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.rdf.util.RdfEdgeSequence;
import com.tinkerpop.blueprints.pgm.impls.rdf.util.RdfVertexSequence;
import com.tinkerpop.blueprints.pgm.impls.sail.SailTokens;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import java.util.*;

/**
 * A Blueprints implementation of an RDF database.
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class RdfGraph implements TransactionalGraph {
	
	/*
    public static final Map<String, RDFFormat> formats = new HashMap<String, RDFFormat>();

    static {
        formats.put("n-triples", RDFFormat.NTRIPLES);
        formats.put("n3", RDFFormat.N3);
        formats.put("rdf-xml", RDFFormat.RDFXML);
        formats.put("trig", RDFFormat.TRIG);
        formats.put("trix", RDFFormat.TRIX);
        formats.put("turtle", RDFFormat.TURTLE);
    }

    public static RDFFormat getFormat(final String format) {
        RDFFormat ret = formats.get(format);
        if (null == ret)
            throw new RuntimeException(
        		format +" is an unsupported RDF file format. " +
        		"Use rdf-xml, n-triples, turtle, n3, trix, or trig.");
        else
            return ret;
    }
    */
    
	public static final String RDFGRAPH_NS = "http://www.eecs.harvard.edu/~dmargo/01/rdfgraph-ns#";
	public static final String RDFGRAPH_PREFIX = "graph";

    protected Sail rawGraph;
    protected SailConnection sailConnection;
    public ValueFactory valueFactory;
    
    protected boolean inTransaction = false;
    protected int txBuffer = 1;
	protected int txCounter = 0;
    //private static final String LOG4J_PROPERTIES = "log4j.properties";
    
    public RdfGraph() {}

    /**
     * Construct a new RdfGraph with an uninitialized Sail.
     *
     * @param rawGraph a not-yet-initialized Sail instance
     */
    public RdfGraph(final Sail rawGraph) {
        this.startSail(rawGraph);
    }

    protected void startSail(final Sail sail) {
        /*try {
            PropertyConfigurator.configure(RdfGraph.class.getResource(LOG4J_PROPERTIES));
        } catch (Exception e) {}*/
        try {
            this.rawGraph = sail;
            this.rawGraph.initialize();
            this.sailConnection = this.rawGraph.getConnection();
            this.valueFactory = this.rawGraph.getValueFactory();

            /*
            this.addNamespace(SailTokens.FOAF_PREFIX, SailTokens.FOAF_NS);
            this.addNamespace(SailTokens.OWL_PREFIX, SailTokens.OWL_NS);
            this.addNamespace(SailTokens.RDF_PREFIX, SailTokens.RDF_NS);
            this.addNamespace(SailTokens.RDFS_PREFIX, SailTokens.RDFS_NS);
            this.addNamespace(SailTokens.XSD_PREFIX, SailTokens.XSD_NS);
            */
            this.addNamespace(RDFGRAPH_PREFIX, RDFGRAPH_NS);
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Vertex addVertex(Object id) {
    	return new RdfVertex(this);
    }

    public Vertex getVertex(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("RdfGraph.getVertex(id) cannot be null.");
    	
    	try {
    		return new RdfVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }
    
    public Iterable<Vertex> getVertices() {
    	return new RdfVertexSequence(this);
    }
    
    public void removeVertex(final Vertex vertex) {
    	if (vertex == null || vertex.getId() == null)
    		return;
    	((RdfVertex) vertex).remove();
    }

    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {
        return new RdfEdge(
    		this,
    		(RdfVertex) outVertex,
    		(RdfVertex) inVertex,
    		label);
    }
    
    public Edge getEdge(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("RdfGraph.getEdge(id) cannot be null.");
    	
    	try {
    		return new RdfEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
        return new RdfEdgeSequence(this, null, true);
    }
    
    public void removeEdge(final Edge edge) {
    	if (edge == null || edge.getId() == null)
    		return;
    	((RdfEdge) edge).remove();
    }

    public void clear() {
        try {
            this.sailConnection.clear();
            this.autoStopTransaction(Conclusion.SUCCESS);
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void shutdown() {
        try {
        	this.autoStopTransaction(Conclusion.FAILURE);
        	this.sailConnection.close();
            this.rawGraph.shutDown();
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public String toString() {
        String type = this.rawGraph.getClass().getSimpleName().toLowerCase();
        return "rdfgraph[" + type + "]";
    }    
    
    public Sail getRawGraph() {
        return this.rawGraph;
    }
    
    /**
     * Get the Sail connection currently being used by the graph.
     *
     * @return the Sail connection
     */
    public SailConnection getSailConnection() {
        return this.sailConnection;
    }

    /**
     * Add a prefix-to-namespace mapping to the Sail connection of this graph.
     *
     * @param prefix    the prefix (e.g. tg)
     * @param namespace the namespace (e.g. http://tinkerpop.com#)
     */
    public void addNamespace(final String prefix, final String namespace) {
        try {
            this.sailConnection.setNamespace(prefix, namespace);
            this.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Get all the prefix-to-namespace mappings of the graph.
     *
     * @return a map of the prefix-to-namespace mappings
     */
    public Map<String, String> getNamespaces() {
        Map<String, String> namespaces = new HashMap<String, String>();
        try {
            CloseableIteration<? extends Namespace, SailException> results =
        		this.sailConnection.getNamespaces();
            
            while (results.hasNext()) {
                Namespace namespace = results.next();
                namespaces.put(namespace.getPrefix(), namespace.getName());
            }
            
            results.close();
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return namespaces;
    }
    
    private String getPrefixes() {
        String prefixString = "";
        
        for (Map.Entry<String, String> entry : this.getNamespaces().entrySet()) {
            prefixString =
        		prefixString +
        		SailTokens.PREFIX_SPACE +
        		entry.getKey() + SailTokens.COLON_LESSTHAN +
        		entry.getValue() +
        		SailTokens.GREATERTHAN_NEWLINE;
        }
        
        return prefixString;
    }
    
    /**
     * Remove a prefix-to-namespace mapping from the Sail connection of this graph.
     *
     * @param prefix the prefix of the prefix-to-namespace mapping to remove
     */
    public void removeNamespace(final String prefix) {
        try {
            this.sailConnection.removeNamespace(prefix);
            this.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Given a URI, compress it to its prefixed URI.
     *
     * @param uri the expanded URI (e.g. http://tinkerpop.com#knows)
     * @return the compressed URI (e.g. tg:knows)
     */
    public String prefixNamespace(final String uri) {
        return RdfGraph.namespaceToPrefix(uri, this.sailConnection);
    }
    
    protected static String namespaceToPrefix(String uri, final SailConnection sailConnection) {
        try {
            CloseableIteration<? extends Namespace, SailException> namespaces =
        		sailConnection.getNamespaces();
            
            while (namespaces.hasNext()) {
                Namespace namespace = namespaces.next();
                
                if (uri.contains(namespace.getName()))
                    uri = uri.replace(
                		namespace.getName(),
                		namespace.getPrefix() + SailTokens.NAMESPACE_SEPARATOR);
            }
            
            namespaces.close();
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return uri;
    }
    
    /**
     * Given a URI, expand it to its full URI.
     *
     * @param uri the compressed URI (e.g. tg:knows)
     * @return the expanded URI (e.g. http://tinkerpop.com#knows)
     */
    public String expandPrefix(final String uri) {
        return RdfGraph.prefixToNamespace(uri, this.sailConnection);
    }
 
    protected static String prefixToNamespace(String uri, final SailConnection sailConnection) {
        try {
            if (uri.contains(SailTokens.NAMESPACE_SEPARATOR)) {
                String namespace = sailConnection.getNamespace(
            		uri.substring(0, uri.indexOf(SailTokens.NAMESPACE_SEPARATOR)));
                
                if (null != namespace)
                    uri = namespace +
                    	uri.substring(uri.indexOf(SailTokens.NAMESPACE_SEPARATOR) + 1);
            }
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return uri;
    }

    public void startTransaction() {
        if (this.inTransaction)
            throw new RuntimeException(TransactionalGraph.NESTED_MESSAGE);
        this.inTransaction = true;
        this.txCounter = 0;
    }

    public void stopTransaction(final Conclusion conclusion) {
        try {
            if (conclusion == Conclusion.SUCCESS) {
                this.sailConnection.commit();
            } else {
                this.sailConnection.rollback();
            }
        	this.inTransaction = false;
        	this.txCounter = 0;
        } catch (SailException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected void autoStopTransaction(final Conclusion conclusion) {
        if (this.txBuffer > 0) {
            try {
                this.txCounter += 1;
                if (conclusion == Conclusion.FAILURE) {
                    this.sailConnection.rollback();
                    this.txCounter = 0;
                    this.inTransaction = false;
                } else if (this.txCounter % this.txBuffer == 0) {
                    this.sailConnection.commit();
                    this.txCounter = 0;
                    this.inTransaction = false;
                }
            } catch (SailException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public void setMaxBufferSize(final int bufferSize) {
        this.stopTransaction(Conclusion.SUCCESS);
        this.inTransaction = false;
        this.txBuffer = bufferSize;
    }

    public int getMaxBufferSize() {
        return this.txBuffer;
    }

    public int getCurrentBufferSize() {
        return this.txCounter;
    }

    /**
     * Evaluate a SPARQL query against the SailGraph (http://www.w3.org/TR/rdf-sparql-query/).
     * The result is a mapping between the ?-bindings and the bound URI, blank node, or literal
     * represented as a Vertex.
     *
     * @param sparqlQuery the SPARQL query to evaluate
     * @return the mapping between a ?-binding and the URI, blank node, or literal as a Vertex
     * @throws RuntimeException if an error occurs in the SPARQL query engine
     */
    public List<Map<String, Vertex>> executeSparql(String sparqlQuery) throws RuntimeException {
        try {
            sparqlQuery = getPrefixes() + sparqlQuery;
            final SPARQLParser parser = new SPARQLParser();
            final ParsedQuery query = parser.parseQuery(sparqlQuery, null);
            boolean includeInferred = false;
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> results =
        		this.sailConnection.evaluate(
    				query.getTupleExpr(),
    				query.getDataset(),
    				new MapBindingSet(),
    				includeInferred);
            final List<Map<String, Vertex>> returnList = new ArrayList<Map<String, Vertex>>();
            
            try {
                while (results.hasNext()) {
                    BindingSet bs = results.next();
                    Map<String, Vertex> returnMap = new HashMap<String, Vertex>();
                    for (Binding b : bs) {
                        returnMap.put(b.getName(), this.getVertex(b.getValue().toString()));
                    }
                    returnList.add(returnMap);
                }
            } finally {
                results.close();
            }
            return returnList;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
