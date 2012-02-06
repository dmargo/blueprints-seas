package com.tinkerpop.blueprints.pgm.impls.bdb.util;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.OperationStatus; 
import com.sleepycat.db.SecondaryDatabase; 
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbEdge;
import com.tinkerpop.blueprints.pgm.impls.bdb.BdbGraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine)
 */
public class BdbEdgeVertexLabelSequence implements Iterator<Edge>, Iterable<Edge> {

    private BdbGraph graph;
    private SecondaryCursor cursor;  
    private BdbPrimaryKey storedKey;
    private boolean useStoredKey = false;
    private Set<String> labels;
   
    public BdbEdgeVertexLabelSequence(final BdbGraph graph, final SecondaryDatabase secondaryDb, final long id, final String... labels) {      
    	OperationStatus status;
    	LongBinding.longToEntry(id, graph.key);
    	
    	try {
            this.cursor = secondaryDb.openSecondaryCursor(null, null);
            status = this.cursor.getSearchKey(graph.key, graph.pKey, graph.data, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    	
    	this.graph = graph;
    	
    	this.labels = new HashSet<String>();
    	for (String label : labels)
    		this.labels.add(label);
        
        if (status == OperationStatus.SUCCESS) {
        	this.storedKey = BdbGraph.primaryKeyBinding.entryToObject(this.graph.pKey);
        	this.useStoredKey = this.labels.contains(this.storedKey.label);
    	} else {
        	this.close();
        }
    } 
    
    public Edge next() {
    	if (this.cursor == null)
    		throw new NoSuchElementException();
	    if (this.useStoredKey) {
	    	this.useStoredKey = false;
	    	return new BdbEdge(this.graph, this.storedKey);
	    }
	    
	    OperationStatus status;
	    
	    while (true) {
		    try {
		    	this.graph.key.setPartial(0, 0, true);
	            status = this.cursor.getNextDup(this.graph.key, this.graph.pKey, this.graph.data, null);
	            this.graph.key.setPartial(false);
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	               
	        if (status == OperationStatus.SUCCESS) {
	        	this.storedKey = BdbGraph.primaryKeyBinding.entryToObject(this.graph.pKey);
	        	if (this.labels.contains(this.storedKey.label))
	        		return new BdbEdge(this.graph, this.storedKey);
	        } else {
	            this.close();
	            throw new NoSuchElementException();
	        }
	    }
    }

    public boolean hasNext() {
    	if (this.cursor == null)
    		return false;
		if (this.useStoredKey)
			return true;
    	
	    OperationStatus status;
	    
	    while (true) {
		    try {
		    	this.graph.key.setPartial(0, 0, true);
	            status = cursor.getNextDup(this.graph.key, this.graph.pKey, this.graph.data, null);
	            this.graph.key.setPartial(false);
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	               
	        if (status == OperationStatus.SUCCESS) {
	        	this.storedKey = BdbGraph.primaryKeyBinding.entryToObject(this.graph.pKey);
	        	if (this.labels.contains(this.storedKey.label)) {
		        	this.useStoredKey = true;
		        	return true;
	        	}
	        } else {
	            this.close();
	            return false;
	        }
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
        	this.labels = null;
        }
    }

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}