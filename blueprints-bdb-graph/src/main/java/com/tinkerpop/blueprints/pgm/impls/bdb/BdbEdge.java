package com.tinkerpop.blueprints.pgm.impls.bdb;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbEdgeKey;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbEdgeKeyBinding;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbEdgePropertyKeyCreator;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbPrimaryKey;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbEdge extends BdbElement implements Edge {
	final public static BdbEdgeKeyBinding edgeKeyBinding = new BdbEdgeKeyBinding();
	final public static BdbEdgePropertyKeyCreator edgePropertyKeyCreator = new BdbEdgePropertyKeyCreator();
	
	private BdbGraph graph;
	protected long outId;
	protected long inId;
	protected String label;
	
    protected BdbEdge(final BdbGraph graph, final BdbVertex outVertex, final BdbVertex inVertex, final String label) throws DatabaseException {
    	// First, verify in and out vertex existence.
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(inVertex.id);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);

		if (graph.graphDb.exists(null, graph.key) == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbEdge(inVertex) " + inVertex.id + " does not exist.");
		
		primaryKey.id1 = outVertex.id;
		BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
		
		if (graph.graphDb.exists(null, graph.key) == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbEdge(outVertex) " + outVertex.id + " does not exist.");
    	
		// Then, insert a new edge record.
    	primaryKey.type = BdbPrimaryKey.EDGE;
    	primaryKey.id2 = inVertex.id;
    	primaryKey.label = label;
    	
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
    	graph.data.setSize(0);

		if (graph.graphDb.put(null, graph.key, graph.data) != OperationStatus.SUCCESS)
			throw new RuntimeException("BdbEdge() failed to put into database.");
		
		this.graph = graph;
		this.outId = outVertex.id;
		this.inId = inVertex.id;
		this.label = label;
    }

    protected BdbEdge(final BdbGraph graph, final Object id) throws DatabaseException {
    	if(!(id instanceof BdbEdgeKey))
    		throw new IllegalArgumentException("BdbEdge(id) " + id + " is not an instanceof BdbEdgeKey.");
    	
    	// Look for a valid edge record.
    	final BdbEdgeKey edgeKey = (BdbEdgeKey) id;
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(edgeKey.outId, edgeKey.inId, edgeKey.label);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
    	
		// If NOTFOUND, then id is invalid.
		if (graph.graphDb.exists(null, graph.key) == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Edge (" + primaryKey.id1 + ", " + primaryKey.id2 +  ") does not exist.");

        this.graph = graph;
        this.outId = edgeKey.outId;
        this.inId = edgeKey.inId;
        this.label = edgeKey.label;
    }

    public BdbEdge(final BdbGraph graph, final BdbPrimaryKey primaryKey) {	
        this.graph = graph;
        this.outId = primaryKey.id1;
        this.inId = primaryKey.id2;
        this.label = primaryKey.label;
    }
    
    public static BdbEdge getRandomEdge(final BdbGraph graph) throws DatabaseException {
        
        // Get a random element
    	
    	OperationStatus status;
    	BdbPrimaryKey k;
    	
    	do {
	    	do {
		    	Cursor cursor = graph.graphDb.openCursor(null, null);
		    	BdbGraph.primaryKeyBinding.objectToEntry(BdbPrimaryKey.RANDOM, graph.key);
		    	status = cursor.getSearchKeyRange(graph.key, graph.data, null);
		        cursor.close();
	    	}
		    while (status == OperationStatus.NOTFOUND);
	    	k = BdbGraph.primaryKeyBinding.entryToObject(graph.key);
    	}
    	while (k.type != BdbPrimaryKey.EDGE);
    	
        return new BdbEdge(graph, k);
    }

    protected void remove() throws DatabaseException {
    	// Remove property records.
    	BdbEdgeKey edgeKey = new BdbEdgeKey(this.outId, this.inId, this.label);
    	BdbEdge.edgeKeyBinding.objectToEntry(edgeKey, this.graph.key);

    	graph.edgePropertyDb.delete(null, this.graph.key);

		// Remove edge record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.outId, this.inId, this.label);
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
        
        graph.graphDb.delete(null, this.graph.key);

		this.label = null;
        this.inId = -1;
        this.outId = -1;
        this.graph = null;
    }

    public Object getId() {
    	if (this.graph == null)
    		return null;
    	
    	return new BdbEdgeKey(this.outId, this.inId, this.label);
    }

    public Vertex getOutVertex() {
    	try {
    		return new BdbVertex(this.graph, new Long(outId));
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
    		throw new RuntimeException(e.getMessage(), e);
    	}
    }

    public Vertex getInVertex() {
    	try {
    		return new BdbVertex(this.graph, new Long(inId));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    }
    
    public String getLabel() {
    	return label;
    }

    public Object getProperty(final String propertyKey) {
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.outId, this.inId, this.label, propertyKey);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
        
        OperationStatus status;
        
        try {
        	status = graph.graphDb.get(null, this.graph.key, this.graph.data, null);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    	
		if (status == OperationStatus.NOTFOUND)
			return null;
			
    	return graph.serialBinding.entryToObject(this.graph.data);
    }

    public Set<String> getPropertyKeys() {
		SecondaryCursor cursor = null;
		OperationStatus status;
		BdbPrimaryKey primaryKey;
		Set<String> ret = new HashSet<String>();
    	
    	BdbEdgeKey edgeKey = new BdbEdgeKey(this.outId, this.inId, this.label);
    	BdbEdge.edgeKeyBinding.objectToEntry(edgeKey, this.graph.key);
		
		try {
			cursor = graph.edgePropertyDb.openSecondaryCursor(null, null);
			
			this.graph.data.setPartial(0, 0, true);
			status = cursor.getSearchKey(this.graph.key, this.graph.pKey, this.graph.data, null);
			this.graph.key.setPartial(0, 0, true);
			while (status == OperationStatus.SUCCESS) {
				primaryKey = BdbGraph.primaryKeyBinding.entryToObject(this.graph.pKey);
				ret.add(primaryKey.propertyKey);
				status = cursor.getNextDup(this.graph.key, this.graph.pKey, this.graph.data, null);
			}
			this.graph.key.setPartial(false);
			this.graph.data.setPartial(false);
			
			cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return ret;
    }
    
    public void setProperty(final String propertyKey, final Object value) {    	
    	if (propertyKey == null || propertyKey.equals("id") || propertyKey.equals("label"))
    		throw new IllegalArgumentException("BdbEdge.setProperty(propertyKey) is invalid.");
    	
        BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.outId, this.inId, this.label);
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
    	
        try {
            //graph.autoStartTransaction();

        	// First, verify edge existence.
    		if (graph.graphDb.exists(null, this.graph.key) == OperationStatus.NOTFOUND)
    			throw new RuntimeException("BdbEdge.setProperty edge (" + primaryKey.id1 + ", " + primaryKey.id2 + ") does not exist.");

    		// Then, insert a new property record.
    		primaryKey.type = BdbPrimaryKey.EDGE_PROPERTY;
        	primaryKey.propertyKey = propertyKey;
           	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
           	
           	graph.serialBinding.objectToEntry(value, this.graph.data);

    		if (graph.graphDb.put(null, this.graph.key, this.graph.data) != OperationStatus.SUCCESS)
    			throw new RuntimeException("BdbEdge.setProperty failed to put into database.");
    		        	
        	//graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Object removeProperty(final String propertyKey) {
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.outId, this.inId, this.label, propertyKey);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
        
    	Object value = null;
    	
        try {
            //graph.autoStartTransaction();
        	
        	if (graph.graphDb.get(null, this.graph.key, this.graph.data, null) == OperationStatus.SUCCESS) {
        		graph.graphDb.delete(null, this.graph.key);
        		value = graph.serialBinding.entryToObject(this.graph.data);
        	}    	
        	
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
        
        return value;
    }
    
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        final BdbEdge other = (BdbEdge) obj;
        return (this.outId == other.outId && this.inId == other.inId && this.label.equals(other.label));
    }

    public int hashCode() {
    	return (new Long(outId)).hashCode() ^ (new Long(inId)).hashCode() ^ label.hashCode();
    }
    
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
