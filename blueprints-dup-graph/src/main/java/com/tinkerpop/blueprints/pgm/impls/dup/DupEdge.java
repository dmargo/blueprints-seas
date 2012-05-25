package com.tinkerpop.blueprints.pgm.impls.dup;

import com.sleepycat.bind.RecordNumberBinding;
import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.OperationStatus;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeData;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeDataBinding;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeKey;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupEdgeKeyBinding;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupPropertyData;
import com.tinkerpop.blueprints.pgm.impls.dup.util.DupRecordNumberComparator;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class DupEdge extends DupElement implements Edge {
	
    final public static DupEdgeDataBinding edgeDataBinding = new DupEdgeDataBinding();
    final protected static DupEdgeKeyBinding edgeKeyBinding = new DupEdgeKeyBinding();
	//final private static String DEFAULT_LABEL = "";
	
	private DupGraph graph;
	protected long out;
	protected String label;
	protected long in;
	
    protected DupEdge(
		final DupGraph graph,
		final DupVertex outVertex,
		final DupVertex inVertex,
		final String label) throws DatabaseException
    {
    	// First, verify in and out vertex existence.
    	OperationStatus status;
    	status = graph.vertexDb.exists(null, outVertex.id);
    	if (status == OperationStatus.SUCCESS)
    		status = graph.vertexDb.exists(null, inVertex.id);

    	this.out = RecordNumberBinding.entryToRecordNumber(outVertex.id);
    	this.in = RecordNumberBinding.entryToRecordNumber(inVertex.id);
        
        if (status != OperationStatus.SUCCESS)
        	throw new RuntimeException("DupEdge: Vertex " + this.out + " or " + this.in + " does not exist.");
        
        if (out < 0 || in < 0)
        	throw new InternalError("Record numbers are not supposed to be negative");
        
        // Then, add out and in edge records.
        DupEdgeData edata = new DupEdgeData(label, this.in);
        DupEdge.edgeDataBinding.objectToEntry(edata, graph.data);
        
    	if (graph.outDb.putNoDupData(null, outVertex.id, graph.data) == OperationStatus.SUCCESS) {
    		edata.id = this.out;
    		DupEdge.edgeDataBinding.objectToEntry(edata, graph.data);
    		graph.inDb.putNoDupData(null, inVertex.id, graph.data);
    		//XXX dmargo: This needs to be a transaction to be safe.
    	}
        
        this.graph = graph;
        this.label = label;
    }
	
    protected DupEdge(
		final DupGraph graph,
		final long outVertex,
		final long inVertex,
		final String label) throws DatabaseException
    {
    	this.out = outVertex;
    	this.in = inVertex;        
        this.graph = graph;
        this.label = label;
    }

    protected DupEdge(final DupGraph graph, final Object id) throws DatabaseException {
    	if(!(id instanceof DupEdgeKey))
    		throw new IllegalArgumentException("DupEdge: " + id + " is not a valid Edge ID.");
    	DupEdgeKey ekey = (DupEdgeKey) id;
    	if (ekey.out < 0)
    		throw new IllegalArgumentException("Invalid ID: vertex ID's cannot be negative");
    	
    	// Look for a valid edge record.    	
    	RecordNumberBinding.recordNumberToEntry(ekey.out, graph.key);

		DupEdgeData edata = new DupEdgeData(ekey.label, ekey.in);
		DupEdge.edgeDataBinding.objectToEntry(edata, graph.data);

        if (graph.outDb.getSearchBoth(null, graph.key, graph.data, null) != OperationStatus.SUCCESS)
        	throw new RuntimeException("DupEdge: Edge " + id + " does not exist.");
        
        this.graph = graph;
        this.out = ekey.out;
        this.label = ekey.label;
        this.in = ekey.in;
    }

    public DupEdge(
		final DupGraph graph,
		final long out,
		final String label,
		final long in)
    {
    	this.graph = graph;
    	this.out = out;
    	this.label = label;
    	this.in = in;
    }
    
    public static DupEdge getRandomEdge(final DupGraph graph) throws DatabaseException {
    	
    	// XXX This will loop forever if there are no edges
    	
    	// TODO Use a dup comparison function if it has a chance to speed things up
    	// TODO It would be safer to do this on a read-only copy of the database, so that
    	//      we do not risk our special comparison function on the master copy and
    	//      thus potentially corrupt it if something goes horribly wrong
    	
    	// Note: Use inDb instead of outDb, since most of our graphs are Barabasi graphs,
    	// in which the in-degree of a node is constant. Warning: This is a hack.
        
    	OperationStatus status;
    	do {
    		
    		// Get a random out-vertex using a distribution that is a rough approximation of what we
    		// had if weighted each out-vertex by its out-degree -- which is equivalent to picking
    		// random edges using uniform distribution. The following code would choose a random leaf
    		// page according to the aforementioned approximation to the given distribution, but it
    		// will pick an out-vertex uniformly at random from the leaf page, so we still need to
    		// account for this
    		
		   	Cursor cursor = graph.inDb.openCursor(null, null);
		   	RecordNumberBinding.recordNumberToEntry(DupRecordNumberComparator.RANDOM, graph.key);
		   	status = cursor.getSearchKeyRange(graph.key, graph.data, null);
		   	if (status == OperationStatus.NOTFOUND) {
		   		cursor.close();
		   		continue;
		   	}
		
		   	
		   	// Now traverse hopefully at least one page of edges and pick one at random using uniform
		   	// distribution. This is to account for the fact that the previous piece of code returns
		   	// an out-vertex picked uniformly at random from its leaf page instead of being weighted
		   	// by its out-degree
		   	
		   	// TODO Is this enough? The max should be large enough so that we traverse at least one page
		   	int steps = (int) (Math.random() * Math.max(cursor.count(), 200));
		   	while (steps --> 0) {
		   		status = cursor.getNext(graph.key, graph.data, null);
		   		if (status == OperationStatus.NOTFOUND) {
		   			status = cursor.getFirst(graph.key, graph.data, null);
		   		}
		   	}
		    cursor.close();
	   	}
	    while (status == OperationStatus.NOTFOUND);
    	
    	
    	// Now return the encountered edge 
    	
    	long other = RecordNumberBinding.entryToRecordNumber(graph.key);
    	DupEdgeData d = edgeDataBinding.entryToObject(graph.data);
    	return new DupEdge(graph, other, d.id, d.label);
    }

    protected void remove() throws DatabaseException{
    	// Remove property records.
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
      	DupEdge.edgeKeyBinding.objectToEntry(ekey, this.graph.key);
    	
        this.graph.edgePropertyDb.delete(null, this.graph.key);
        
        // Remove edge records.
        RecordNumberBinding.recordNumberToEntry(this.out, this.graph.key);
        
        DupEdgeData edata = new DupEdgeData(this.label, this.in);
        DupEdge.edgeDataBinding.objectToEntry(edata, this.graph.data);
        

    	Cursor cursor = this.graph.outDb.openCursor(null, null);
    	if (cursor.getSearchBoth(this.graph.key, this.graph.data, null) == OperationStatus.SUCCESS)
    		cursor.delete();
    	cursor.close();
    	
    	RecordNumberBinding.recordNumberToEntry(this.in, this.graph.key);
    	
    	edata.id = this.out;
    	DupEdge.edgeDataBinding.objectToEntry(edata, this.graph.data);
    	
    	cursor = this.graph.inDb.openCursor(null,  null);
    	if (cursor.getSearchBoth(this.graph.key, this.graph.data, null) == OperationStatus.SUCCESS)
    		cursor.delete();
    	cursor.close();

        this.in = 0;
		this.label = null;
        this.out = 0;
        this.graph = null;
    }

    public Object getId() {
    	if (graph == null || out == 0 || label == null || in == 0)
    		return null;
    	
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
    	return ekey;
    }

    public Vertex getOutVertex() {
    	try {
    		return new DupVertex(this.graph, new Long(out));
    	} catch (RuntimeException e) {
    		throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    }

    public Vertex getInVertex() {
    	try {
	        return new DupVertex(this.graph, new Long(in));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    }
    
    public String getLabel() {
    	return label;
    }

    public Object getProperty(final String pkey) {
    	Cursor cursor;
    	OperationStatus status;
    	
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
    	DupEdge.edgeKeyBinding.objectToEntry(ekey, this.graph.key);
    	
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	
        try {
        	cursor = graph.vertexPropertyDb.openCursor(null,  null);
        	
        	status = cursor.getSearchBothRange(this.graph.key, this.graph.data, null);
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
    	
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
    	DupEdge.edgeKeyBinding.objectToEntry(ekey, this.graph.key);
    	
    	DupPropertyData result;
		Set<String> ret = new HashSet<String>();
		
		try {
			cursor = this.graph.vertexPropertyDb.openCursor(null, null);
			
			status = cursor.getSearchKey(this.graph.key, this.graph.data, null);
			this.graph.key.setPartial(0, 0, true);
			while (status == OperationStatus.SUCCESS) {
				
				result = DupElement.propertyDataBinding.entryToObject(this.graph.data);
				ret.add(result.pkey);
				status = cursor.getNextDup(this.graph.key, this.graph.data, null);
			}
			this.graph.key.setPartial(false);
			
			cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return ret;
    }
    
    public void setProperty(final String pkey, final Object value) {
    	if (pkey.equals("id") || pkey.equals("label"))
    		throw new RuntimeException("DupEdge: " + pkey + " is an invalid property key.");  	
    	
    	Cursor cursor;
    	OperationStatus status;
    	
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
    	DupEdge.edgeKeyBinding.objectToEntry(ekey, this.graph.key);
    	
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	DupPropertyData pdata;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = this.graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	status = cursor.getSearchBothRange(this.graph.key, this.graph.data, null);
        	if (status == OperationStatus.SUCCESS) {
        		
        		//XXX dmargo: This could be done partially, but I'm not sure the benefits
        		//outweigh instantiating a new DatabaseEntry.
        		status = cursor.getCurrent(this.graph.key, this.graph.data, null);
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
        	status = cursor.put(this.graph.key, this.graph.data);
        	
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
    	
    	DupEdgeKey ekey = new DupEdgeKey(this.out, this.label, this.in);
    	DupEdge.edgeKeyBinding.objectToEntry(ekey, this.graph.key);
    	
    	StringBinding.stringToEntry(pkey, this.graph.data);
    	DupPropertyData result = null;
    	
        try {
            //graph.autoStartTransaction();
        	
        	cursor = graph.vertexPropertyDb.openCursor(null,  null);
        	
        	// If pkey exists, delete it.
        	if (cursor.getSearchBothRange(this.graph.key, this.graph.data, null) == OperationStatus.SUCCESS) {
        		
        		this.graph.key.setPartial(0, 0, true);
        		status = cursor.getCurrent(this.graph.key, this.graph.data, null);
        		this.graph.key.setPartial(false);
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

        final DupEdge other = (DupEdge) obj;
        return (this.out == other.out && this.in == other.in && this.label.equals(other.label));
    }

    public int hashCode() {    	
    	return (new Long(out)).hashCode() ^ label.hashCode() ^ (new Long(in)).hashCode();
    }
    
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
