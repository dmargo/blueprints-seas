package com.tinkerpop.blueprints.pgm.impls.hollow.util;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.hollow.HollowGraph;
import com.tinkerpop.blueprints.pgm.impls.hollow.HollowEdge;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class HollowEdgeSequence implements Iterable<Edge>, Iterator<Edge> {

    private HollowGraph graph = null;
    private boolean hasNext = false;
    
    public HollowEdgeSequence(final HollowGraph graph) {
        this.graph = graph;
    }
    
    public HollowEdgeSequence(final HollowGraph graph, final long vid, final boolean getOut) {
        this.graph = graph;
    }
    
    public HollowEdgeSequence(final HollowGraph graph, final long vid, final boolean getOut, final String[] labels) {
        this.graph = graph;
    }
	
	public Edge next() {
		throw new NoSuchElementException();
	}

	public boolean hasNext() {
    	return this.hasNext;
    }
	
	public void close() {}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}