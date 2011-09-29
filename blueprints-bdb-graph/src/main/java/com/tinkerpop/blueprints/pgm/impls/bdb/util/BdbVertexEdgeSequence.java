package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.sleepycat.db.SecondaryDatabase; 
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class BdbVertexEdgeSequence implements Iterator<Edge>, Iterable<Edge> {

    private BdbGraph graph;
    private SecondaryCursor cursor;    
    private DatabaseEntry pKey;
    private boolean useStoredKey;
    
    public BdbVertexEdgeSequence(final BdbGraph graph, final SecondaryDatabase secondaryDb, final DatabaseEntry key) {
    	this.graph = graph;
    	
    	OperationStatus status;
    	this.pKey = new DatabaseEntry();
    	DatabaseEntry data = new DatabaseEntry();
    	
    	try {
            this.cursor = secondaryDb.openSecondaryCursor(null, null);
            status = this.cursor.getSearchKey(key, this.pKey, data, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        if (status == OperationStatus.SUCCESS)
        	this.useStoredKey = true;
        else {
        	this.useStoredKey = false;
        	close();
        }
    } 
	
	public Edge next() {    
		if (cursor == null)
			throw new NoSuchElementException();
	    if (useStoredKey) {
	    	useStoredKey = false;
	    	return new BdbEdge(graph, pKey);
	    }
	    
	    OperationStatus status;
	    DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry data = new DatabaseEntry();
	    
	    try {
            status = cursor.getNextDup(key, pKey, data, null);   	 
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
               
        if (status == OperationStatus.SUCCESS)
        	return new BdbEdge(graph, pKey);
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
	    DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry data = new DatabaseEntry();
	    
	    try {
            status = cursor.getNextDup(key, pKey, data, null);   	 
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
	    try {
	    	cursor.close();
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	graph = null;
        	cursor = null;
        	pKey = null;
        	useStoredKey = false;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}