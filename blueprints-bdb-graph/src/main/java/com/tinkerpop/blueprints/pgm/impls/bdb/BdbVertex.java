package com.tinkerpop.blueprints.pgm.impls.bdb;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
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
import java.util.NoSuchElementException;
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

    protected BdbVertex(final BdbGraph graph) throws DatabaseException {    		
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry data = graph.data.get();

    	// Get the last ID# in the graph.
		Cursor cursor = graph.graphDb.openCursor(null, null);
        OperationStatus status = cursor.getLast(key, data, null);
        cursor.close();
		
        BdbPrimaryKey primaryKey;
        if (status == OperationStatus.NOTFOUND) {
        	primaryKey = new BdbPrimaryKey();
        	this.id = 0;
        } else {
        	primaryKey = BdbGraph.primaryKeyBinding.entryToObject(key);
        	this.id = primaryKey.id1 + 1;
        	if (this.id <= 0)
    			throw new RuntimeException("BdbVertex() is out of ID#s.");
        }
        
        primaryKey.type = BdbPrimaryKey.VERTEX;
        primaryKey.id1 = this.id;
        primaryKey.id2 = 0;
        primaryKey.label = null;
        primaryKey.propertyKey = null;
		
		BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
		data.setSize(0);
		
		// Insert a new vertex record.
		if (graph.graphDb.put(null, key, data) != OperationStatus.SUCCESS)
			throw new RuntimeException("BdbVertex failed to put into database.");
		
		this.graph = graph;
		this.id = primaryKey.id1;
    }

    protected BdbVertex(final BdbGraph graph, final Object id) throws DatabaseException {
    	if(!(id instanceof Long))
    		throw new IllegalArgumentException("BdbVertex(id) " + id + " is not an instanceof Long.");
    	DatabaseEntry key = graph.key.get();
   	
        BdbPrimaryKey primaryKey = new BdbPrimaryKey(((Long) id).longValue());
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
		if (graph.graphDb.exists(null, key) == OperationStatus.NOTFOUND)
			throw new RuntimeException("BdbVertex(id) " + primaryKey.id1 + " does not exist.");

        this.graph = graph;
        this.id = primaryKey.id1;
    }

    public BdbVertex(final BdbGraph graph, final long id) {
    	this.graph = graph;
        this.id = id;
    }
    
    public static BdbVertex getRandomVertex(final BdbGraph graph) throws DatabaseException {
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry pKey = graph.pKey.get();
    	DatabaseEntry data = graph.data.get();
    	
    	// Note: This implementation assumes that the number of vertex deletions is negligible
    	// as compared to the total number of nodes
    	
		// Get the last ID# in the graph.
		SecondaryCursor cursor = graph.vertexDb.openSecondaryCursor(null, null);
        OperationStatus status = cursor.getLast(key, data, null);
        cursor.close();
        if (status == OperationStatus.NOTFOUND) throw new NoSuchElementException();
        Long lastId = LongBinding.entryToLong(key);
        
        // Get a random element
        cursor = graph.vertexDb.openSecondaryCursor(null, null);
        LongBinding.longToEntry((long)(lastId.longValue() * Math.random()), key);
        status = cursor.getSearchKeyRange(key, pKey, data, null);
        cursor.close();
        if (status == OperationStatus.NOTFOUND) throw new InternalError();
        
        return new BdbVertex(graph, (long) LongBinding.entryToLong(key));
    }

    protected void remove() throws DatabaseException {
    	DatabaseEntry key = graph.key.get();

    	// Remove linked edge records.
        for (Edge e : this.getInEdges())
        	((BdbEdge) e).remove();
    	for (Edge e : this.getOutEdges())
            ((BdbEdge) e).remove();

    	// Remove properties.
        LongBinding.longToEntry(this.id, key);
    	graph.vertexPropertyDb.delete(null, key);
		
		// Remove vertex record.
        BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.id);
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
        graph.graphDb.delete(null, key);

        this.id = -1;
        this.graph = null;
    }
    
    public Object getId() {
    	if (this.id == -1)
    		return null;
    	return new Long(this.id);
    }

    public Iterable<Edge> getOutEdges(final String... labels) {
    	if (labels.length == 0)
    		return new BdbEdgeVertexSequence(this.graph, this.graph.outDb, this.id);
    	else
    		return new BdbEdgeVertexLabelSequence(this.graph, this.graph.outDb, this.id, labels);
    }

    public Iterable<Edge> getInEdges(final String... labels) {
    	if (labels.length == 0)
    		return new BdbEdgeVertexSequence(this.graph, this.graph.inDb, this.id);
    	else
    		return new BdbEdgeVertexLabelSequence(this.graph, this.graph.inDb, this.id, labels);    
    }

    public Object getProperty(final String propertyKey) {
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry data = graph.data.get();
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.id, propertyKey);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
        OperationStatus status;
        
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
		SecondaryCursor cursor = null;
		OperationStatus status;
		BdbPrimaryKey primaryKey;
		Set<String> ret = new HashSet<String>();
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry pKey = graph.pKey.get();
    	DatabaseEntry data = graph.data.get();
		
    	LongBinding.longToEntry(id, key);
    	
		try {
			cursor = graph.vertexPropertyDb.openSecondaryCursor(null, null);
			
			data.setPartial(0, 0, true);
			status = cursor.getSearchKey(key, pKey, data, null);
			key.setPartial(0, 0, true);
			while (status == OperationStatus.SUCCESS) {
				primaryKey = BdbGraph.primaryKeyBinding.entryToObject(pKey);
				ret.add(primaryKey.propertyKey);
				status = cursor.getNextDup(key, pKey, data, null);
			}
			key.setPartial(false);
			data.setPartial(false);
			
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
    		throw new IllegalArgumentException("BdbVertex.setProperty(propertyKey) is invalid.");
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry data = graph.data.get();
    	
        BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.id);
        BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
    	
        try {
            //graph.autoStartTransaction();

        	// First, verify vertex existence.
    		if (this.graph.graphDb.exists(null, key) == OperationStatus.NOTFOUND)
    			throw new RuntimeException("BdbVertex.setProperty vertex " + primaryKey.id1 + " does not exist.");

    		// Then, insert a new property record.
    		primaryKey.type = BdbPrimaryKey.VERTEX_PROPERTY;
        	primaryKey.propertyKey = propertyKey;
           	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
           	
           	graph.serialBinding.objectToEntry(value, data);

    		if (graph.graphDb.put(null, key, data) != OperationStatus.SUCCESS)
    			throw new RuntimeException("BdbVertex.setProperty failed to put into database.");
    		        	
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
    	DatabaseEntry key = graph.key.get();
    	DatabaseEntry data = graph.data.get();
    	BdbPrimaryKey primaryKey = new BdbPrimaryKey(this.id, propertyKey);
    	BdbGraph.primaryKeyBinding.objectToEntry(primaryKey, key);
        
    	Object value = null;
    	
        try {
            //graph.autoStartTransaction();

        	if (graph.graphDb.get(null, key, data, null) == OperationStatus.SUCCESS) {
        		graph.graphDb.delete(null, key);
        		value = graph.serialBinding.entryToObject(data);
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
