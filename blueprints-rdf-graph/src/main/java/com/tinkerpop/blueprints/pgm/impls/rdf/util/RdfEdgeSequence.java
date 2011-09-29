package com.tinkerpop.blueprints.pgm.impls.rdf.util;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.rdf.RdfEdge;
import com.tinkerpop.blueprints.pgm.impls.rdf.RdfGraph;
import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.sail.SailException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RdfEdgeSequence implements Iterable<Edge>, Iterator<Edge> {

    private final RdfGraph graph;
    private final CloseableIteration<? extends Statement, SailException> statements;
    
    public RdfEdgeSequence(
		final RdfGraph graph,
		final Resource rawVertex,
		final URI labelURI,
		final boolean getOut)
    {    	
    	try {
    		if (getOut)
    			this.statements = graph.getSailConnection().getStatements(
					rawVertex, labelURI, null, false, RdfEdge.edgeContext);
    		else
    			this.statements = graph.getSailConnection().getStatements(
					null, labelURI, rawVertex, false, RdfEdge.edgeContext);
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}

    	this.graph = graph;
    }

    public Edge next() {
    	Statement result;
    	try {
    		result = this.statements.next();
    		if (!this.statements.hasNext())
    			this.statements.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	return new RdfEdge(this.graph, result);
    }
    
    public boolean hasNext() {
    	try {
    		if (this.statements.hasNext())
    			return true;
    		else {
    			this.statements.close();
    			return false;
    		}
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}    
	}

    public void close() {
    	try {
    		this.statements.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}    
	}
    
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public Iterator<Edge> iterator() {
        return this;
    }
}
