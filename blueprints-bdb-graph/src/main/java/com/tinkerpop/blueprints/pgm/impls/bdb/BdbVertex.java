package com.tinkerpop.blueprints.pgm.impls.bdb;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.SecondaryCursor;
import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.StringFactory;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbInKeyCreator;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbOutKeyCreator;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbPrimaryKey;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbEdgeVertexLabelSequence;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbEdgeVertexSequence;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbVertexKeyCreator;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.BdbVertexPropertyKeyCreator;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class BdbVertex extends BdbElement implements Vertex {
    final public static BdbVertexKeyCreator vertexKeyCreator = new BdbVertexKeyCreator();
    final public static BdbOutKeyCreator outKeyCreator = new BdbOutKeyCreator();
    final public static BdbInKeyCreator inKeyCreator = new BdbInKeyCreator();
    final public static BdbVertexPropertyKeyCreator vertexPropertyKeyCreator = new BdbVertexPropertyKeyCreator();
	
    private BdbGraph graph;
    protected long id;

    protected BdbVertex(final BdbGraph graph) {    	
		Cursor cursor = null;
		OperationStatus status;
		
		// Get the last ID# in the graph.
		try {
	        cursor = graph.graphDb.openCursor(null, null);
	        status = cursor.getLast(graph.key, graph.data, null);
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
			primaryKey = BdbGraph.primaryKeyBinding.entryToObject(graph.key);
			this.id = primaryKey.id1 + 1;
			if (this.id <= 0)
				throw new RuntimeException("BdbGraph: Database is out of ID#s.");
		}
		
		// Insert a new vertex record.
		primaryKey.type = BdbPrimaryKey.VERTEX;
		primaryKey.id1 = this.id;
		primaryKey.id2 = 0;
		primaryKey.propertyKey = null;

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
    }

    protected BdbVertex(final BdbGraph graph, final Object id) {
    	if(id.getClass() != Long.class)
    		throw new RuntimeException("BdbGraph: " + id + " is not a valid Vertex ID.");
    	
    	// Look for a valid vertex record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        primaryKey.type = BdbPrimaryKey.VERTEX;
        primaryKey.id1 = ((Long) id).longValue();
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
			throw new RuntimeException("BdbGraph: Vertex " + primaryKey.id1 + " does not exist.");

        this.graph = graph;
        this.id = primaryKey.id1;
    }

    public BdbVertex(final BdbGraph graph, final long id) {
    	this.graph = graph;
        this.id = id;
    }  

    protected void remove() {
    	// Remove linked edge records.
        for (Edge e : this.getInEdges())
        	((BdbEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((BdbEdge) e).remove();

    	// Remove properties.
        LongBinding.longToEntry(id, this.graph.key);
        
        try {
        	graph.vertexPropertyDb.delete(null, this.graph.key);
        } catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		
		// Remove vertex record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey();
        primaryKey.type = BdbPrimaryKey.VERTEX;
        primaryKey.id1 = id;
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
        
        try {
        	graph.graphDb.delete(null, this.graph.key);
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
    	if (labels.length == 0)
    		return new BdbEdgeVertexSequence(graph, graph.outDb, this.id);
    	else
    		return new BdbEdgeVertexLabelSequence(graph, graph.outDb, this.id, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	if (labels.length == 0)
    		return new BdbEdgeVertexSequence(graph, graph.inDb, this.id);
    	else
    		return new BdbEdgeVertexLabelSequence(graph, graph.inDb, this.id, labels);    
    }

    public Object getProperty(final String propertyKey) {
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey();
    	primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
    	primaryKey.id1 = id;
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
		
    	LongBinding.longToEntry(id, this.graph.key);
    	
		
		try {
			cursor = graph.vertexPropertyDb.openSecondaryCursor(null, null);
			
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
    	if (propertyKey == null || propertyKey.equals("id"))
    		throw new RuntimeException("BdbGraph: Invalid propertyKey.");
    	
        try {
            //graph.autoStartTransaction();

        	// First, verify vertex existence.
            BdbPrimaryKey primaryKey = new BdbPrimaryKey();
            primaryKey.type = BdbPrimaryKey.VERTEX;
            primaryKey.id1 = id;
            BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, this.graph.key);
  		
    		if (graph.graphDb.exists(null, this.graph.key) == OperationStatus.NOTFOUND)
    			throw new RuntimeException("BdbGraph: Vertex " + primaryKey.id1 + " does not exist.");

    		// Then, insert a new property record.
    		primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
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
        	primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
        	primaryKey.id1 = id;
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
