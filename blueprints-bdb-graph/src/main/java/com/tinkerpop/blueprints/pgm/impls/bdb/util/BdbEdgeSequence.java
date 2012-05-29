package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.OperationStatus; 
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class BdbEdgeSequence implements Iterator<Edge>, Iterable<Edge> {

    private BdbGraph graph;
    private SecondaryCursor cursor;    
    private BdbPrimaryKey storedKey;
    private boolean useStoredKey = false;
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry pKey = new DatabaseEntry();
    private DatabaseEntry data = new DatabaseEntry();
    
    public BdbEdgeSequence(final BdbGraph graph) {          
        OperationStatus status;
    	
        try {
            this.cursor = graph.outDb.openSecondaryCursor(null, null);
            key.setPartial(0, 0, true);
            status = this.cursor.getFirst(key, pKey, data, null);
            key.setPartial(false);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
        
        if (status == OperationStatus.SUCCESS) {
        	this.storedKey = BdbGraph.primaryKeyBinding.entryToObject(this.pKey);
        	this.useStoredKey = true;
        } else
        	this.close();
    }
	
	public Edge next() {
		if (this.cursor == null)
			throw new NoSuchElementException();
	    if (this.useStoredKey) {
	    	this.useStoredKey = false;
	    	return new BdbEdge(this.graph, this.storedKey);
	    }
	    
	    OperationStatus status;
	    
	    try {
	    	this.key.setPartial(0, 0, true);
            status = this.cursor.getNext(this.key, this.pKey, this.data, null);
            this.key.setPartial(false);
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
               
        if (status == OperationStatus.SUCCESS)
        	return new BdbEdge(graph, BdbGraph.primaryKeyBinding.entryToObject(this.pKey));
        else {
            close();
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
	    	this.key.setPartial(0, 0, true);
            status = cursor.getNext(this.key, this.pKey, this.data, null);
            this.key.setPartial(false);
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
               
        if (status == OperationStatus.SUCCESS) {
        	this.storedKey = BdbGraph.primaryKeyBinding.entryToObject(this.pKey);
        	this.useStoredKey = true;
        	return true;
        } else {
            this.close();
            return false;
        }	
	}
	
	public void close() {
	    try {
	    	cursor.close();
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	this.graph = null;
        	this.cursor = null;
        	this.storedKey = null;
        	this.useStoredKey = false;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}