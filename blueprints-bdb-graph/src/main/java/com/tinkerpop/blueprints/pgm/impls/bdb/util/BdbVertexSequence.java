package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.sleepycat.db.SecondaryCursor;
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
    private SecondaryCursor cursor;    
    private DatabaseEntry key = new DatabaseEntry();
    private boolean useStoredKey = false;
    
    public BdbVertexSequence(final BdbGraph graph) {
        this.graph = graph;
        this.key = new DatabaseEntry();
        this.useStoredKey = false;
    	
        try {
            this.cursor = graph.vertexDb.openSecondaryCursor(null, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
	
	public Vertex next() {
		if (cursor == null)
			throw new NoSuchElementException();
	    if (useStoredKey) {
	    	useStoredKey = false;
	    	return new BdbVertex(graph, key);
	    }
    
	    OperationStatus status;
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
	    try {
	        status = cursor.getNextNoDup(key, pKey, data, null);   	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS)
        	return new BdbVertex(graph, key);
        else {
            close();
            throw new NoSuchElementException();
        }
	}

	public boolean hasNext() {
		if (cursor == null)
			return false;
		if (useStoredKey)
			return true;
		
	    OperationStatus status;
        DatabaseEntry pKey = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        
	    try {
	        status = cursor.getNextNoDup(key, pKey, data, null);   	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS) {
        	useStoredKey = true;
        	return true;
        } else {
            close();
            return false;
        }
	}
	
	public void close() {
	    try{
	    	cursor.close();
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	graph = null;
        	cursor = null;
        	key = null;
        	useStoredKey = false;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Vertex> iterator() {
        return this;
    }
}