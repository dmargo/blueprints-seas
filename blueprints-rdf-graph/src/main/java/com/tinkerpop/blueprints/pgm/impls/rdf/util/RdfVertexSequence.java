package com.tinkerpop.blueprints.pgm.impls.rdf.util;

import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.rdf.RdfGraph;
import com.tinkerpop.blueprints.pgm.impls.rdf.RdfVertex;
import info.aduna.iteration.CloseableIteration;

import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RdfVertexSequence implements Iterable<Vertex>, Iterator<Vertex> {

    private final RdfGraph graph;
    private final CloseableIteration<? extends Statement, SailException> statements;
    
    public RdfVertexSequence(final RdfGraph graph) {
    	try {
			this.statements = graph.getSailConnection().getStatements(
				null, RdfVertex.vertexPred, RdfVertex.blankObj, false);
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}

    	this.graph = graph;
    }

    public Vertex next() {
    	Statement result;
    	
    	try {
    		result = this.statements.next();
    		if (!this.statements.hasNext())
    			this.statements.close();
    	} catch (SailException e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    	
    	return new RdfVertex(this.graph, result.getSubject());
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

    public Iterator<Vertex> iterator() {
        return this;
    }
}
