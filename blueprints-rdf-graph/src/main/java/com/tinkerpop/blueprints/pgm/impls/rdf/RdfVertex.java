package com.tinkerpop.blueprints.pgm.impls.rdf;


import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.rdf.util.RdfEdgeSequence;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.SailException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RdfVertex extends RdfElement implements Vertex {

	public static final URI vertexPred = new URIImpl(RdfGraph.RDFGRAPH_NS + "vertex");
	public static final Value blankObj = new BNodeImpl("");
			
    protected Resource rawVertex = null;
    protected RdfGraph graph = null;
    
    public RdfVertex(final RdfGraph graph) {
    	this.rawVertex = graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, UUID.randomUUID().toString());
    	
    	RdfHelper.addStatement(
			this.rawVertex,
			RdfVertex.vertexPred,
			RdfVertex.blankObj,
			null,
			graph.sailConnection);
    	graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	
    	this.graph = graph;
    }

    public RdfVertex(final RdfGraph graph, final Object id) {
    	if (id.getClass() != String.class)
    		throw new RuntimeException("RdfVertex: " + id + "is not a valid Vertex ID.");
    	
    	this.rawVertex = new URIImpl((String) id);
    	boolean exists = false;
    	
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(
					this.rawVertex,
					RdfVertex.vertexPred,
					RdfVertex.blankObj,
					false);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException ("RdfVertex " + id + " does not exist.");
    	
        this.graph = graph;
    }
    
    public RdfVertex(final RdfGraph graph, final Resource rawVertex) {
    	this.rawVertex = rawVertex;
    	this.graph = graph;
    }

    protected void remove() {
    	// First, remove edges.
        for (Edge e : this.getInEdges())
        	((RdfEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((RdfEdge) e).remove();
    	
    	// Next, remove properties and vertex.
    	RdfHelper.removeStatement(this.rawVertex, RdfElement.propertyPred, null, null, graph.sailConnection);
       	RdfHelper.removeStatement(this.rawVertex, RdfVertex.vertexPred, RdfVertex.blankObj, null, graph.sailConnection);
    	
    	this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	
    	this.rawVertex = null;
    	this.graph = null;
    }
    
    public Object getId() {
        return rawVertex != null ? this.rawVertex.stringValue() : null;
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
    	return new RdfEdgeSequence(this.graph, this.rawVertex, true, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	return new RdfEdgeSequence(this.graph, this.rawVertex, false, labels);
    }
    
    public Object getProperty(final String key) {
    	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);
    	
    	Literal result = null;
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(this.rawVertex, RdfElement.propertyPred, null, false, keyCtxt);
    		
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
    			graph.sailConnection.getStatements(this.rawVertex, RdfElement.propertyPred, null, false);
    		
    		while (iter.hasNext()) {
    			String keyString = iter.next().getContext().stringValue();
    			result.add(keyString.substring(RdfGraph.RDFGRAPH_NS.length(), keyString.length()));
    		}
    		
    		iter.close();    		
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	return result;
    }
    
    public void setProperty(final String key, final Object value) {
    	if (key == null || key.equals("id"))
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
    	this.removeProperty(key);
    	
    	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);	
    	Literal valueLiteral = RdfElement.castObject(value);
    	    	
    	Statement statement = new ContextStatementImpl(this.rawVertex, RdfElement.propertyPred, valueLiteral, keyCtxt);
    	RdfHelper.addStatement(statement, graph.sailConnection);
    	this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }
    
    public Object removeProperty(final String key) {
    	Object result = this.getProperty(key);
    	if (result != null) {
        	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);
        	
    		RdfHelper.removeStatement(rawVertex, RdfElement.propertyPred, null, keyCtxt, graph.sailConnection);
        	this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
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

        final RdfVertex other = (RdfVertex) obj;
        return (this.rawVertex.stringValue().equals(other.rawVertex.stringValue()));
    }
    
    public int hashCode() {
        return this.rawVertex.stringValue().hashCode();
    }
    
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
