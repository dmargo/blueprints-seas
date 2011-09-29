package com.tinkerpop.blueprints.pgm.impls.dup;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.OperationStatus;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeVertexLabelSequence;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeVertexSequence;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupPropertyData;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupVertex extends DupElement implements Vertex {
		
    private DupGraph graph;
    protected DatabaseEntry id = new DatabaseEntry();

    protected DupVertex(final DupGraph graph) {    	
		OperationStatus status;
		graph.data.setSize(0);
		
		try {
			status = graph.vertexDb.append(null, this.id, graph.data);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		if (status != OperationStatus.SUCCESS)
			throw new RuntimeException("DupVertex: Failed to create vertex ID.");
			
		this.graph = graph;
    }

    protected DupVertex(final DupGraph graph, final Object id) {
    	if(id.getClass() != Long.class)
    		throw new RuntimeException("DupVertex: " + id + " is not a valid vertex ID.");
    	
        OperationStatus status;
    	RecordNumberBinding.recordNumberToEntry((Long) id, this.id);
        
        try {
        	status = graph.vertexDb.exists(null, this.id);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		if (status != OperationStatus.SUCCESS)
			throw new RuntimeException("DupVertex: Vertex " + id + " does not exist.");

        this.graph = graph;
    }

    public DupVertex(final DupGraph graph, final DatabaseEntry id) {
    	this.graph = graph;
    	this.id = new DatabaseEntry();
    	this.id.setData(id.getData().clone());
    }  

    protected void remove() {
    	// Remove linked edge records.
        for (Edge e : this.getInEdges())
        	((DupEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((DupEdge) e).remove();

    	// Remove properties and vertex record.
        try {
        	this.graph.vertexPropertyDb.delete(null, this.id);
        	this.graph.vertexDb.delete(null, this.id);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

        this.id = null;
        this.graph = null;
    }
    
    public Object getId() {
    	return id != null ? RecordNumberBinding.entryToRecordNumber(this.id) : null;
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
    	if (labels.length == 0)
    		return new DupEdgeVertexSequence(this.graph, this.id, true);
    	else
    		return new DupEdgeVertexLabelSequence(this.graph, this.id, true, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	if (labels.length == 0)
    		return new DupEdgeVertexSequence(this.graph, this.id, false);
    	else
    		return new DupEdgeVertexLabelSequence(this.graph, this.id, false, labels);
    }

    public Object getProperty(final String pkey) {
    	Cursor cursor;
    	OperationStatus status;
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	
        try {
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	status = cursor.getSearchBothRange(this.id, this.graph.data, null);
        	if (status == OperationStatus.SUCCESS) {
        		
        		this.graph.key.setPartial(0, 0, true);
        		status = cursor.getCurrent(this.graph.key, this.graph.data, null);
        		this.graph.key.setPartial(false);
        	}
        	
        	cursor.close();
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
        
        if (status != OperationStatus.SUCCESS)
        	return null;
        
        DupPropertyData result = DupElement.propertyDataBinding.entryToObject(this.graph.data);
        return pkey.equals(result.pkey) ? result.value : null;
    }

    public Set<String> getPropertyKeys() {
    	Cursor cursor;
    	OperationStatus status;
    	DupPropertyData result;
		Set<String> ret = new HashSet<String>();
		
		try {
			cursor = this.graph.vertexPropertyDb.openCursor(null, null);
			
			status = cursor.getSearchKey(this.id, this.graph.data, null);
			while (status == OperationStatus.SUCCESS) {
				
				result = DupElement.propertyDataBinding.entryToObject(this.graph.data);
				ret.add(result.pkey);
				status = cursor.getNextDup(this.id, this.graph.data, null);
			}
			
			cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return ret;
    }
    
    public void setProperty(final String pkey, final Object value) {
    	if (pkey.equals("id"))
    		throw new RuntimeException("DupVertex: " + pkey + " is an invalid property key.");  		
    	
    	Cursor cursor;
    	OperationStatus status;
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	DupPropertyData pdata;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	status = cursor.getSearchBothRange(this.id, this.graph.data, null);
        	if (status == OperationStatus.SUCCESS) {
        		
        		this.graph.key.setPartial(0, 0, true);
        		status = cursor.getCurrent(this.graph.key, this.graph.data, null);
        		this.graph.key.setPartial(false);
        		if (status == OperationStatus.SUCCESS) {
        			
        			pdata = DupElement.propertyDataBinding.entryToObject(this.graph.data);
        			if (pkey.equals(pdata.pkey))
        				cursor.delete();
        		} else
        			pdata = new DupPropertyData();
        	} else
        		pdata = new DupPropertyData();
        	
        	// Put the new pkey and value.
        	pdata.pkey = pkey;
        	pdata.value = value;
        	DupElement.propertyDataBinding.objectToEntry(pdata, this.graph.data);
        	status = cursor.put(this.id, this.graph.data);
        	
        	cursor.close();
        	
        	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw e;
		} catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw new RuntimeException(e.getMessage(), e);
		}
        
        if (status != OperationStatus.SUCCESS)
        	throw new RuntimeException("DupVertex: Could not set property '" + pkey + "'.");
    }

    public Object removeProperty(final String pkey) {
    	Cursor cursor;
    	OperationStatus status;
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	DupPropertyData result = null;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	if (cursor.getSearchBothRange(this.id, this.graph.data, null) == OperationStatus.SUCCESS) {
        		
        		this.graph.key.setPartial(0, 0, false);
        		status = cursor.getCurrent(new DatabaseEntry(), this.graph.data, null);
        		this.graph.key.setPartial(true);
        		if (status == OperationStatus.SUCCESS) {
        			
        			result = DupElement.propertyDataBinding.entryToObject(this.graph.data);
        			if (pkey.equals(result.pkey))
        				cursor.delete();
        		}
        	}
        	
        	cursor.close();
        	
        	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw e;
		} catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
			throw new RuntimeException(e.getMessage(), e);
		}
        
        return result != null ? result.value : null;
    }
    
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final DupVertex other = (DupVertex) obj;
        return this.id.equals(other.id);
    }

    public int hashCode() {
        return this.id.hashCode();
    }
    
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
