package com.tinkerpop.blueprints.pgm.impls.dup.util;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.dup.DupGraph;
import com.tinkerpop.blueprints.pgm.impls.dup.DupVertex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupVertexSequence implements Iterator<Vertex>, Iterable<Vertex> {

    private DupGraph graph;
    private Cursor cursor;    
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry data = new DatabaseEntry();
    private boolean useStoredKey = false;
    
    public DupVertexSequence(final DupGraph graph) {
    	OperationStatus status;
    	
        try {
            this.cursor = graph.vertexDb.openCursor(null, null);
            status = this.cursor.getFirst(this.key, this.data, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
        
        if (status == OperationStatus.SUCCESS)
        	this.useStoredKey = true;
        else
        	this.close();
    }
	
	public Vertex next() {
		if (this.cursor == null)
			throw new NoSuchElementException();
	    if (this.useStoredKey) {
	    	this.useStoredKey = false;
	    	return new DupVertex(this.graph, this.key);
	    }
    
	    OperationStatus status;  
	    try {
	        status = this.cursor.getNext(this.key, this.data, null);   	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS)
        	return new DupVertex(this.graph, this.key);
        else {
            this.close();
            throw new NoSuchElementException();
        }
	}

	public boolean hasNext() {
		if (this.cursor == null)
			return false;
		if (this.useStoredKey)
			return true;
		
	    OperationStatus status;
	    
	    try {
	        status = this.cursor.getNext(this.key, this.data, null);   	        
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS) {
        	this.useStoredKey = true;
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
        	this.useStoredKey = false;
        	this.key = null;
        	this.cursor = null;
        	this.graph = null;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Vertex> iterator() {
        return this;
    }
}