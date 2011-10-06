package com.tinkerpop.blueprints.pgm.impls.bdb;

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
	
    protected BdbEdge(final BdbGraph graph, final BdbVertex outVertex, final BdbVertex inVertex, final String label) {
    	// First, verify in and out vertex existence.
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.VERTEX;
    	primaryKey.id1 = inVertex.id;
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
    	
        OperationStatus status;

        try {
        	status = graph.graphDb.exists(null, graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		if (status == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Vertex " + inVertex.id + " does not exist.");
		
		primaryKey.id1 = outVertex.id;
		BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
		
        try {
        	status = graph.graphDb.exists(null, graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		if (status == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Vertex " + outVertex.id + " does not exist.");
    	
		// Then, insert a new edge record.
    	primaryKey.type = BdbPrimaryKey.EDGE;
    	primaryKey.id2 = inVertex.id;
    	primaryKey.label = label;
    	
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
    	graph.data.setSize(0);
    	
		try {
			status = graph.graphDb.put(null, graph.key, graph.data);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		assert(status == OperationStatus.SUCCESS);
		
		this.graph = graph;
		this.outId = outVertex.id;
		this.inId = inVertex.id;
		this.label = label;
    }

    protected BdbEdge(final BdbGraph graph, final Object id) {
    	if(id.getClass() != BdbEdgeKey.class)
    		throw new RuntimeException("BdbGraph: " + id + " is not a valid Edge ID.");
    	
    	// Look for a valid edge record.
    	final BdbEdgeKey edgeKey = (BdbEdgeKey) id;
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.EDGE;
    	primaryKey.id1 = edgeKey.outId;
    	primaryKey.id2 = edgeKey.inId;
    	primaryKey.label = edgeKey.label;
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, graph.key);
        
        OperationStatus status;
        
        try {
        	status = graph.graphDb.exists(null, graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		// If NOTFOUND, then id is invalid.
		if (status == OperationStatus.NOTFOUND)
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

    protected void remove() {
    	// Remove property records.
    	BdbEdgeKey edgeKey = new BdbEdgeKey();
    	edgeKey.outId = outId;
    	edgeKey.inId = inId;
    	edgeKey.label = label;
    	BdbEdge.edgeKeyBinding.objectToEntry(edgeKey, this.graph.key);

        try {
        	graph.edgePropertyDb.delete(null, this.graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		// Remove edge record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        primaryKey.type = BdbPrimaryKey.EDGE;
        primaryKey.id1 = outId;
        primaryKey.id2 = inId;
        primaryKey.label = label;
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
        
        try {
        	graph.graphDb.delete(null, this.graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		this.label = null;
        this.inId = -1;
        this.outId = -1;
        this.graph = null;
    }

    public Object getId() {
    	if (graph == null || outId == -1 || inId == -1 || label == null)
    		return null;
    	
    	BdbEdgeKey edgeKey = new BdbEdgeKey();
    	edgeKey.outId = outId;
    	edgeKey.inId = inId;
    	edgeKey.label = label;
    	return edgeKey;
    }

    public Vertex getOutVertex() {
    	return new BdbVertex(graph, new Long(outId));
    }

    public Vertex getInVertex() {
        return new BdbVertex(graph, new Long(inId));
    }
    
    public String getLabel() {
    	return label;
    	/*
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.EDGE;
    	primaryKey.id1 = outId;
    	primaryKey.id2 = inId;
    	
    	DatabaseEntry key = new DatabaseEntry();
    	graph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
        OperationStatus status;
    	DatabaseEntry data = new DatabaseEntry();
        
        try {
        	status = graph.graphDb.get(null, key, data, null);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    	
		if (status == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Edge (" + primaryKey.id1 + ", " + primaryKey.id2 +  ") does not exist.");
		
    	return StringBinding.entryToString(data);
    	*/
    }
    
    /* Not required, and not possible without EIDs.
    public void setLabel(final String label) {
    	// First, verify edge existence.
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.EDGE;
    	primaryKey.id1 = outId;
    	primaryKey.id2 = inId;
    	
    	DatabaseEntry key = new DatabaseEntry();
    	graph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
        OperationStatus status;
        
        try {
        	status = graph.graphDb.exists(null, key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
    	
		if (status == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Edge (" + primaryKey.id1 + ", " + primaryKey.id2 +  ") does not exist.");
		
		// Then, put the label.
		DatabaseEntry data = new DatabaseEntry();
		StringBinding.stringToEntry(label, data);
		
        try {
        	status = graph.graphDb.put(null, key, data);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		assert(status == OperationStatus.SUCCESS);
    }
    */

    public Object getProperty(final String propertyKey) {
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.EDGE_PROPERTY;
    	primaryKey.id1 = outId;
    	primaryKey.id2 = inId;
    	primaryKey.label = label;
    	primaryKey.propertyKey = propertyKey;
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
    	
    	BdbEdgeKey edgeKey = new BdbEdgeKey();
    	edgeKey.outId = outId;
    	edgeKey.inId = inId;
    	edgeKey.label = label;
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
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
        try {
            //graph.autoStartTransaction();

        	// First, verify edge existence.
            BdbPrimaryKey primaryKey = new BdbPrimaryKey();
            primaryKey.type = BdbPrimaryKey.EDGE;
            primaryKey.id1 = outId;
            primaryKey.id2 = inId;
            primaryKey.label = label;
            BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
  		
    		if (graph.graphDb.exists(null, this.graph.key) == OperationStatus.NOTFOUND)
    			throw new RuntimeException("BdbGraph: Edge (" + primaryKey.id1 + ", " + primaryKey.id2 + ") does not exist.");

    		// Then, insert a new property record.
    		primaryKey.type = BdbPrimaryKey.EDGE_PROPERTY;
        	primaryKey.propertyKey = propertyKey;
           	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
           	
           	graph.serialBinding.objectToEntry(value, this.graph.data);

           	OperationStatus status = graph.graphDb.put(null, this.graph.key, this.graph.data);
    		assert(status == OperationStatus.SUCCESS);
    		        	
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
        try {
            //graph.autoStartTransaction();

        	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        	primaryKey.type = BdbPrimaryKey.EDGE_PROPERTY;
        	primaryKey.id1 = outId;
        	primaryKey.id2 = inId;
        	primaryKey.label = label;
        	primaryKey.propertyKey = propertyKey;
        	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
            
        	Object value = null;
        	
        	if (graph.graphDb.get(null, this.graph.key, this.graph.data, null) == OperationStatus.SUCCESS) {
        		graph.graphDb.delete(null, this.graph.key);
        		value = graph.serialBinding.entryToObject(this.graph.data);
        	}    	
        	
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
    		
            return value;
        } catch (RuntimeException e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //graph.autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
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
