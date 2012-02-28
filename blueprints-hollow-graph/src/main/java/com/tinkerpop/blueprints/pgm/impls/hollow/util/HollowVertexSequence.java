package com.tinkerpop.blueprints.pgm.impls.hollow.util;

import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.hollow.HollowGraph;
import com.tinkerpop.blueprints.pgm.impls.hollow.HollowVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class HollowVertexSequence implements Iterable<Vertex>, Iterator<Vertex> {

    private HollowGraph graph = null;
    private int i = 0;
    
    public HollowVertexSequence(final HollowGraph graph) {
        this.graph = graph;
    }
	
	public Vertex next() {
		if (i++ < this.graph.vertexCount) {
			return new HollowVertex(this.graph, 0);
		} else {
			throw new NoSuchElementException();
		}
	}

	public boolean hasNext() {
		return i < this.graph.vertexCount;
    }
	
	public void close() {
		i = this.graph.vertexCount;
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Vertex> iterator() {
        return this;
    }
}