package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
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
public class BdbEdgeVertexSequence implements Iterator<Edge>, Iterable<Edge> {

    private BdbGraph graph;
    private SecondaryCursor cursor;    
    private BdbPrimaryKey storedKey;
    private boolean useStoredKey = false;
    private DatabaseEntry key = new DatabaseEntry();
    private DatabaseEntry pKey = new DatabaseEntry();
    private DatabaseEntry data = new DatabaseEntry();
    
    public BdbEdgeVertexSequence(final BdbGraph graph, final SecondaryDatabase secondaryDb, final long id) {
    	OperationStatus status;
    	LongBinding.longToEntry(id, key);
    	
    	try {
            this.cursor = secondaryDb.openSecondaryCursor(null, null);
            status = this.cursor.getSearchKey(key, pKey, data, null);
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
            status = this.cursor.getNextDup(this.key, this.pKey, this.data, null);
            this.key.setPartial(false);
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
               
        if (status == OperationStatus.SUCCESS)
        	return new BdbEdge(graph, BdbGraph.primaryKeyBinding.entryToObject(this.pKey));
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
	    	this.key.setPartial(0, 0, true);
            status = this.cursor.getNextDup(this.key, this.pKey, this.data, null);   	
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