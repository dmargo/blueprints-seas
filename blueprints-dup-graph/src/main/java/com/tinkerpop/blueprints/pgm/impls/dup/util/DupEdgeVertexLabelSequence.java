package com.tinkerpop.blueprints.pgm.impls.dup.util;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry; 
import com.sleepycat.db.OperationStatus; 
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.impls.dup.DupEdge;
import com.tinkerpop.blueprints.pgm.impls.dup.DupGraph;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdgeVertexLabelSequence implements Iterator<Edge>, Iterable<Edge> {

    private DupGraph graph;
    private Cursor cursor;
    private DatabaseEntry id;
    private DatabaseEntry data = new DatabaseEntry();
    private SortedSet<String> labels = new TreeSet<String>();
    private boolean useStored = false;
    private boolean getOut;
    
    public DupEdgeVertexLabelSequence(final DupGraph graph, final DatabaseEntry id, final boolean getOut, final String... labels) {
    	for (String label : labels)
    		this.labels.add(label);
    	StringBinding.stringToEntry(this.labels.first(), this.data);
    	
    	OperationStatus status;
    	
        try {
        	if (getOut)
        		this.cursor = graph.outDb.openCursor(null, null);
        	else
        		this.cursor = graph.inDb.openCursor(null, null);
        	
            status = this.cursor.getSearchBothRange(id, this.data, null);
            if (status == OperationStatus.SUCCESS)
            	status = this.cursor.getCurrent(id, this.data, null);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        
        this.graph = graph;
        this.id = id;
        this.getOut = getOut;
        
        if (status == OperationStatus.SUCCESS) {
        	DupEdgeData edata = DupEdge.edgeDataBinding.entryToObject(this.data);
        	if (this.labels.contains(edata.label))
        		this.useStored = true;
        	else
        		this.close();
        } else
        	this.close();
    }
	
	public Edge next() {
		if (this.cursor == null)
			throw new NoSuchElementException();
	    if (this.useStored) {
	    	this.useStored = false;
	    	DupEdgeData edata = DupEdge.edgeDataBinding.entryToObject(data);
	    	if (this.getOut)
	    		return new DupEdge(this.graph, RecordNumberBinding.entryToRecordNumber(this.id), edata.label, edata.id);
	    	else
	    		return new DupEdge(this.graph, edata.id, edata.label, RecordNumberBinding.entryToRecordNumber(this.id));
	    }
	    
	    OperationStatus status;
	    
	    while (true) {
		    try {
	            status = this.cursor.getNextDup(this.id, this.data, null);   	 
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
	               
	        if (status == OperationStatus.SUCCESS) {
		    	DupEdgeData edata = DupEdge.edgeDataBinding.entryToObject(data);
		    	if (this.labels.contains(edata.label))
			    	if (this.getOut)
			    		return new DupEdge(this.graph, RecordNumberBinding.entryToRecordNumber(this.id), edata.label, edata.id);
			    	else
			    		return new DupEdge(this.graph, edata.id, edata.label, RecordNumberBinding.entryToRecordNumber(this.id));
	    	} else {
		        this.close();
		        throw new NoSuchElementException();
	    	}
	    }
	}

	public boolean hasNext() {
		if (cursor == null)
			return false;
		if (this.useStored)
			return true;
		
	    OperationStatus status;
	    
	    while (true) {
		    try {
	            status = this.cursor.getNextDup(this.id, this.data, null);   	 
		    } catch (RuntimeException e) {
	            throw e;
	        } catch (Exception e) {
	            throw new RuntimeException(e.getMessage(), e);
	        }
		    
	        if (status == OperationStatus.SUCCESS) {
	        	DupEdgeData edata = DupEdge.edgeDataBinding.entryToObject(this.data);
	        	if (this.labels.contains(edata.label)) {
	        		this.useStored = true;
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
	    	this.cursor.close();
	    } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	this.useStored = false;
        	this.data = null;
        	this.id = null;
        	this.cursor = null;
        	this.graph = null;
        }
	}

    public void remove() { 
        throw new UnsupportedOperationException(); 
    } 

    public Iterator<Edge> iterator() {
        return this;
    }
}