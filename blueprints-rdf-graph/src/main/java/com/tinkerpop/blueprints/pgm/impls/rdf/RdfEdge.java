package com.tinkerpop.blueprints.pgm.impls.rdf;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
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
	
	public static final URI edgePred = new URIImpl(RdfGraph.RDFGRAPH_NS + "edge");

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
					outVertex.rawVertex, RdfVertex.vertexPred, RdfVertex.blankObj, false);
    		
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
					inVertex.rawVertex, RdfVertex.vertexPred, RdfVertex.blankObj, false);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException(
				"RdfEdge: inVertex " + inVertex.getId() + " does not exist.");
    	
    	// Now, construct the edge and its URI.
    	Resource labelCtxt = label == null ? null : graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, label);
    	
    	RdfHelper.addStatement(
			outVertex.rawVertex, RdfEdge.edgePred, inVertex.rawVertex, labelCtxt, graph.sailConnection);
    	graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	this.rawEdge = new ContextStatementImpl(
			outVertex.rawVertex, RdfEdge.edgePred, inVertex.rawVertex, labelCtxt);
    	
    	String outString = outVertex.rawVertex.toString();
    	String inString = inVertex.rawVertex.toString();
    	this.edgeURI = graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS,
    		outString.substring(RdfGraph.RDFGRAPH_NS.length(), outString.length()) + ":" +
    		(label == null ? "" : label + ":") +
    		inString.substring(RdfGraph.RDFGRAPH_NS.length(), inString.length()));

        this.graph = graph;
    }

    public RdfEdge(RdfGraph graph, Object id) {
    	if (id.getClass() != String.class)
    		throw new RuntimeException("RdfEdge: " + id + " is not a valid Edge ID.");
    	
    	String[] tokens = (
			(String) id).substring(RdfGraph.RDFGRAPH_NS.length(), ((String) id).length()).split(":");
       	if (tokens.length != 2 && tokens.length != 3)
    		throw new RuntimeException("RdfEdge: " + id + " is not a valid Edge ID.");
    	
    	Resource outRes = graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, tokens[0]);
    	Value inVal = graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, tokens.length == 2 ? tokens[1] : tokens[2]);
    	Resource labelCtxt = tokens.length == 2 ? null : graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, tokens[1]);
    	
    	// Confirm edge exists.
    	boolean exists = false;
    	
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(outRes, RdfEdge.edgePred, inVal, false, labelCtxt);
    		
    		exists = iter.hasNext();
    		
    		iter.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	if (!exists)
    		throw new RuntimeException ("RdfEdge " + id + " does not exist.");
    
    	this.rawEdge = new ContextStatementImpl(outRes, RdfEdge.edgePred, inVal, labelCtxt);
    	this.edgeURI = graph.valueFactory.createURI((String) id);
    	
    	this.graph = graph;
    }

    public RdfEdge(RdfGraph graph, Statement rawEdge) {
    	this.rawEdge = rawEdge;
    	
    	String outString = rawEdge.getSubject().stringValue();
    	String inString = rawEdge.getObject().stringValue();
    	Resource labelCtxt = rawEdge.getContext();
    	String labelString = labelCtxt == null ? null : labelCtxt.stringValue();
    	
    	this.edgeURI = new URIImpl(
			RdfGraph.RDFGRAPH_NS +
    		outString.substring(RdfGraph.RDFGRAPH_NS.length(), outString.length()) + ":" +
			(labelString == null ? "" : labelString.substring(RdfGraph.RDFGRAPH_NS.length(), labelString.length()) + ":") +
    		inString.substring(RdfGraph.RDFGRAPH_NS.length(), inString.length()));
    	
    	this.graph = graph;
    }
    
    protected void remove() {
    	RdfHelper.removeStatement(this.edgeURI, RdfElement.propertyPred, null, null, this.graph.sailConnection);
    	RdfHelper.removeStatement(this.rawEdge, this.graph.sailConnection);
    	this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    	
    	this.rawEdge = null;
    	this.edgeURI = null;
    	this.graph = null;
    }
    
    public Object getId() {
    	return this.edgeURI != null ? this.edgeURI.stringValue() : null;
    }
    
    public Vertex getOutVertex() {
        return new RdfVertex(this.graph, this.rawEdge.getSubject().stringValue());
    }
    
    public Vertex getInVertex() {
        return new RdfVertex(this.graph, this.rawEdge.getObject().stringValue());
    }

    public String getLabel() {
    	Resource labelCtxt = this.rawEdge.getContext();
    	if (labelCtxt == null)
    		return "";
    	else {
        	String label = labelCtxt.stringValue();
        	return label.substring(RdfGraph.RDFGRAPH_NS.length(), label.length());
    	}
    }

    public Object getProperty(final String key) {
    	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);
    	
    	Literal result = null;
    	try {
    		CloseableIteration<? extends Statement,SailException> iter =
    			graph.sailConnection.getStatements(this.edgeURI, RdfElement.propertyPred, null, false, keyCtxt);
    		
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
    			graph.sailConnection.getStatements(this.edgeURI, RdfElement.propertyPred, null, false);
    		
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
    	if (key == null || key.equals("id")|| key.equals("label"))
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
    	this.removeProperty(key);
    	
    	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);	
    	Literal valueLiteral = RdfElement.castObject(value);
    	
    	RdfHelper.addStatement(this.edgeURI, RdfElement.propertyPred, valueLiteral, keyCtxt, graph.sailConnection);
    	this.graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    }

    public Object removeProperty(final String key) {
    	Object result = this.getProperty(key);
    	
    	if (result != null) {
        	Resource keyCtxt = this.graph.valueFactory.createURI(RdfGraph.RDFGRAPH_NS, key);  	
        	
    		RdfHelper.removeStatement(this.edgeURI, RdfElement.propertyPred, null, keyCtxt, graph.sailConnection);
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
