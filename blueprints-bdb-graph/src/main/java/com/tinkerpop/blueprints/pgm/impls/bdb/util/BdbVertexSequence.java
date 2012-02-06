package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class BdbVertexSequence implements Iterator<Vertex>, Iterable<Vertex> {

    private BdbGraph graph;
    private Cursor cursor;    
    private long storedId;
    private boolean useStoredId = false;
    
    public BdbVertexSequence(final BdbGraph graph) {
        OperationStatus status;
    	
        try {
            this.cursor = graph.vertexDb.openCursor(null, null);
            status = this.cursor.getFirst(graph.key, graph.data, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
        
        if (status == OperationStatus.SUCCESS) {
        	this.storedId = LongBinding.entryToLong(this.graph.key);
        	this.useStoredId = true;
        } else
        	this.close();
    }
	
	public Vertex next() {
		if (this.cursor == null)
			throw new NoSuchElementException();
	    if (this.useStoredId) {
	    	this.useStoredId = false;
	    	return new BdbVertex(this.graph, this.storedId);
	    }
    
	    OperationStatus status;
        
	    try {
	        status = this.cursor.getNext(this.graph.key, this.graph.data, null);  	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS)
        	return new BdbVertex(this.graph, LongBinding.entryToLong(this.graph.key));
        else {
            this.close();
            throw new NoSuchElementException();
        }
	}

	public boolean hasNext() {
		if (cursor == null)
			return false;
		if (this.useStoredId)
			return true;
		
	    OperationStatus status;
        
	    try {
	        status = this.cursor.getNext(this.graph.key, this.graph.data, null); 	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS) {
        	this.storedId = LongBinding.entryToLong(this.graph.key);
        	this.useStoredId = true;
        	return true;
        } else {
            this.close();
            return false;
        }
	}
	
	public void close() {
	    try{
	    	this.cursor.close();
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	this.graph = null;
        	this.cursor = null;
        	this.useStoredId = false;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Vertex> iterator() {
        return this;
    }
}