package com.tinkerpop.blueprints.pgm.impls.bdb;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbPrimaryKey;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbVertexEdgeLabelSequence;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbVertexEdgeSequence;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbVertex extends BdbElement implements Vertex {
    private BdbGraph graph;
    protected long id;

    protected BdbVertex(final BdbGraph graph) {    	
		Cursor cursor = null;
		OperationStatus status;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		// Get the last ID# in the graph.
		try {
	        cursor = graph.graphDb.openCursor(null, null);
	        status = cursor.getLast(key, data, null);
	        cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		BdbPrimaryKey primaryKey;
		
		if (status == OperationStatus.NOTFOUND) {
			primaryKey = new BdbPrimaryKey();
			this.id = 0;
		} else {
			primaryKey = graph.primaryKeyBinding.entryToObject(key);
			this.id = primaryKey.id1 + 1;
			if (this.id <= 0)
				throw new RuntimeException("BdbGraph: Database is out of ID#s.");
		}
		
		// Insert a new vertex record.
		primaryKey.type = BdbPrimaryKey.VERTEX;
		primaryKey.id1 = this.id;
		primaryKey.id2 = 0;
		primaryKey.propertyKey = null;

		graph.primaryKeyBinding.objectToEntry(primaryKey, key);
		data = new DatabaseEntry();
		
		try {
			status = graph.graphDb.put(null, key, data);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		assert(status == OperationStatus.SUCCESS);
		
		this.graph = graph;
    }

    protected BdbVertex(final BdbGraph graph, final Object id) {
    	if(id.getClass() != Long.class)
    		throw new RuntimeException("BdbGraph: " + id + " is not a valid Vertex ID.");
    	
    	// Look for a valid vertex record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        primaryKey.type = BdbPrimaryKey.VERTEX;
        primaryKey.id1 = ((Long) id).longValue();

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
		
		// If NOTFOUND, then id is invalid.
		if (status == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbGraph: Vertex " + primaryKey.id1 + " does not exist.");

        this.graph = graph;
        this.id = primaryKey.id1;
    }

    public BdbVertex(final BdbGraph graph, final DatabaseEntry key) {
    	this.graph = graph;
        this.id = LongBinding.entryToLong(key);
    }  

    protected void remove() {
    	// Remove linked edge records.
        for (Edge e : this.getInEdges())
        	((BdbEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((BdbEdge) e).remove();

    	// Remove properties.
        DatabaseEntry key = new DatabaseEntry();
        LongBinding.longToEntry(id, key);
        
        try {
        	graph.vertexPropertyDb.delete(null, key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		// Remove vertex record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        primaryKey.type = BdbPrimaryKey.VERTEX;
        primaryKey.id1 = id;
        graph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
        try {
        	graph.graphDb.delete(null, key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

        this.id = -1;
        this.graph = null;
    }
    
    public Object getId() {
    	if (id == -1)
    		return null;
    	return new Long(id);
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
    	DatabaseEntry key = new DatabaseEntry();
    	LongBinding.longToEntry(id, key);
    	if (labels.length == 0)
    		return new BdbVertexEdgeSequence(graph, graph.outDb, key);
    	else
    		return new BdbVertexEdgeLabelSequence(graph, graph.outDb, key, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	DatabaseEntry key = new DatabaseEntry();
    	LongBinding.longToEntry(id, key);
    	if (labels.length == 0)
    		return new BdbVertexEdgeSequence(graph, graph.inDb, key);
    	else
    		return new BdbVertexEdgeLabelSequence(graph, graph.inDb, key, labels);    
    }

    public Object getProperty(final String propertyKey) {
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
    	primaryKey.id1 = id;
    	primaryKey.propertyKey = propertyKey;
    	
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
			return null;
		
    	return graph.serialBinding.entryToObject(data);
    }

    public Set<String> getPropertyKeys() {
    	DatabaseEntry key = new DatabaseEntry();
    	LongBinding.longToEntry(id, key);
    	
		SecondaryCursor cursor = null;
		OperationStatus status;
		DatabaseEntry pKey = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		
		BdbPrimaryKey primaryKey;
		Set<String> ret = new HashSet<String>();
		
		try {
			cursor = graph.vertexPropertyDb.openSecondaryCursor(null, null);
			
			status = cursor.getSearchKey(key, pKey, data, null);
			while (status == OperationStatus.SUCCESS) {
				primaryKey = graph.primaryKeyBinding.entryToObject(pKey);
				ret.add(primaryKey.propertyKey);
				status = cursor.getNextDup(key, pKey, data, null);
			}
			
			cursor.close();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		return ret;
    }
    
    public void setProperty(final String propertyKey, final Object value) {
    	if (propertyKey == null || propertyKey.equals("id"))
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
        try {
            //graph.autoStartTransaction();

        	// First, verify vertex existence.
            BdbPrimaryKey primaryKey = new BdbPrimaryKey();
            primaryKey.type = BdbPrimaryKey.VERTEX;
            primaryKey.id1 = id;

            DatabaseEntry key = new DatabaseEntry();
            graph.primaryKeyBinding.objectToEntry(primaryKey, key);
  		
    		if (graph.graphDb.exists(null, key) == OperationStatus.NOTFOUND)
    			throw new RuntimeException("BdbGraph: Vertex " + primaryKey.id1 + " does not exist.");

    		// Then, insert a new property record.
    		primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
        	primaryKey.propertyKey = propertyKey;
           	graph.primaryKeyBinding.objectToEntry(primaryKey, key);
           	
           	DatabaseEntry data = new DatabaseEntry();
           	graph.serialBinding.objectToEntry(value, data);

           	OperationStatus status = graph.graphDb.put(null, key, data);
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
        	primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
        	primaryKey.id1 = id;
        	primaryKey.propertyKey = propertyKey;
        	
        	DatabaseEntry key = new DatabaseEntry();
        	graph.primaryKeyBinding.objectToEntry(primaryKey, key);
        	DatabaseEntry data = new DatabaseEntry();
            
        	Object value = null;
        	
        	if (graph.graphDb.get(null, key, data, null) == OperationStatus.SUCCESS) {
        		graph.graphDb.delete(null, key);
        		value = graph.serialBinding.entryToObject(data);
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

        final BdbVertex other = (BdbVertex) obj;
        return (this.id == other.id);
    }

    public int hashCode() {
        return (new Long(id)).hashCode();
    }
    
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
