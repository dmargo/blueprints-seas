package com.tinkerpop.blueprints.pgm.impls.rdf;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailException;

import info.aduna.iteration.CloseableIteration;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class RdfEdge extends RdfElement implements Edge {
	
	public static final String defaultLabel = "RDFEDGE_DEFAULT_LABEL";
	public static final URI edgeContext = new URIImpl(RdfGraph.RDFGRAPH_NS + "edge");

    protected Statement rawEdge = null;
    protected URI edgeURI = null;
    protected RdfGraph graph = null;
    
    public RdfEdge(RdfGraph graph, RdfVertex outVertex, RdfVertex inVertex, String label) {
    	// First, confirm vertices exist.
    	if (outVertex.rawVertex == null)
    		throw new RuntimeException("RdfEdge: outVertex is closed.");
    	else if (inVertex.rawVertex == null)
    		throw new RuntimeException("RdfEdge: inVertex is closed.");

    	boolean exists = false;
    	
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(
					outVertex.rawVertex, RdfVertex.isURI, RdfVertex.vertexURI, false);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException(
				"RdfEdge: outVertex " + outVertex.getId() + " does not exist.");
    
       	exists = false;
       	
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(
					inVertex.rawVertex, RdfVertex.isURI, RdfVertex.vertexURI, false);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException(
				"RdfEdge: inVertex " + inVertex.getId() + " does not exist.");
    	
    	// Now, construct the edge and its URI.    	
    	if (label == null)
    		label = RdfEdge.defaultLabel;
    	URI labelURI = new URIImpl(RdfGraph.RDFGRAPH_NS + label);
    	
    	RdfHelper.addStatement(
			outVertex.rawVertex, labelURI, inVertex.rawVertex, edgeContext, graph.sailConnection);
    	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	this.rawEdge = new ContextStatementImpl(
			outVertex.rawVertex, labelURI, inVertex.rawVertex, edgeContext);
    	
    	String outString = outVertex.rawVertex.toString();
    	String inString = inVertex.rawVertex.toString();
    	this.edgeURI = new URIImpl(
			RdfGraph.RDFGRAPH_NS +
    		outString.substring(RdfGraph.RDFGRAPH_NS.length(), outString.length()) + ":" +
    		label + ":" +
    		inString.substring(RdfGraph.RDFGRAPH_NS.length(), inString.length()));

        this.graph = graph;
    }

    public RdfEdge(RdfGraph graph, Object id) {
    	if (id.getClass() != String.class)
    		throw new RuntimeException("RdfEdge: " + id + " is not a valid Edge ID.");
    	
    	String[] tokens = (
			(String) id).substring(RdfGraph.RDFGRAPH_NS.length(), ((String) id).length()).split(":");
       	if (tokens.length != 3)
    		throw new RuntimeException("RdfEdge: " + id + " is not a valid Edge ID.");
    	
    	URI outURI = new URIImpl(RdfGraph.RDFGRAPH_NS + tokens[0]);
    	URI labelURI = new URIImpl(RdfGraph.RDFGRAPH_NS + tokens[1]);
    	URI inURI = new URIImpl(RdfGraph.RDFGRAPH_NS + tokens[2]);
    	
    	// Confirm edge exists.
    	boolean exists = false;
    	
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(outURI, labelURI, inURI, false, edgeContext);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException ("RdfEdge " + id + " does not exist.");
    
    	this.rawEdge = new ContextStatementImpl(outURI, labelURI, inURI, edgeContext);
    	this.edgeURI = new URIImpl((String) id);
    	
    	this.graph = graph;
    }

    public RdfEdge(RdfGraph graph, Statement rawEdge) {
    	this.rawEdge = rawEdge;
    	
    	String outString = rawEdge.getSubject().stringValue();
    	String label = rawEdge.getPredicate().stringValue();
    	String inString = rawEdge.getObject().stringValue();
    	this.edgeURI = new URIImpl(
			RdfGraph.RDFGRAPH_NS +
    		outString.substring(RdfGraph.RDFGRAPH_NS.length(), outString.length()) + ":" +
    		label.substring(RdfGraph.RDFGRAPH_NS.length(), label.length()) + ":" +
    		inString.substring(RdfGraph.RDFGRAPH_NS.length(), inString.length()));
    	
    	this.graph = graph;
    }
    
    protected void remove() {
    	RdfHelper.removeStatement(edgeURI, null, null, propertyContext, graph.sailConnection);
    	RdfHelper.removeStatement(this.rawEdge, graph.sailConnection);
    	//this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	
    	this.rawEdge = null;
    	this.edgeURI = null;
    	this.graph = null;
    }
    
    public Object getId() {
    	return edgeURI != null ? edgeURI.stringValue() : null;
    }
    
    public Vertex getOutVertex() {
        return new RdfVertex(this.graph, this.rawEdge.getSubject().stringValue());
    }
    
    public Vertex getInVertex() {
        return new RdfVertex(this.graph, this.rawEdge.getObject().stringValue());
    }

    public String getLabel() {
    	String label = this.rawEdge.getPredicate().stringValue();
    	return label.substring(RdfGraph.RDFGRAPH_NS.length(), label.length());
    }

    public Object getProperty(final String key) {
    	URI keyURI = new URIImpl(RdfGraph.RDFGRAPH_NS + key);   	
    	
    	Literal result = null;
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(this.edgeURI, keyURI, null, false, propertyContext);
    		
    		if (iter.hasNext()) {
	    		Statement statement = iter.next();
	    		result = (Literal) statement.getObject();
    		}
    		
    		iter.close();    		
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	return result != null ? castLiteral(result) : null;
    }
    
    public Set<String> getPropertyKeys() {
    	Set<String> result = new HashSet<String>();
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(this.edgeURI, null, null, false, propertyContext);
    		
    		while (iter.hasNext()) {
    			String fullKey = iter.next().getPredicate().stringValue();
    			result.add(fullKey.substring(RdfGraph.RDFGRAPH_NS.length(), fullKey.length()));
    		}
    		
    		iter.close();    		
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	return result;
    }

    public void setProperty(final String key, final Object value) {
    	if (key == null || key.equals("id")|| key.equals("label"))
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
    	this.removeProperty(key);
    	
    	URI keyURI = new URIImpl(RdfGraph.RDFGRAPH_NS + key); 	
    	Literal valueLiteral = RdfElement.castObject(value);
    	
    	RdfHelper.addStatement(this.edgeURI, keyURI, valueLiteral, propertyContext, graph.sailConnection);
    	//this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }

    public Object removeProperty(final String key) {
    	Object result = this.getProperty(key);
    	
    	if (result != null) {
        	URI keyURI = new URIImpl(RdfGraph.RDFGRAPH_NS + key);    	
        	
    		RdfHelper.removeStatement(this.edgeURI, keyURI, null, propertyContext, graph.sailConnection);
        	//this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	}
    	
    	return result;
    }

    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final RdfEdge other = (RdfEdge) obj;
        return this.getId().equals(other.getId());
    }

    public int hashCode() {
        return this.getId().hashCode();
    }
    
    public String toString() {
    	return StringFactory.edgeString(this);
    }
}
