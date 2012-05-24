package com.tinkerpop.blueprints.pgm.impls.dup;

import com.sleepycat.db.BtreeStats;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseStats;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.QueueStats;
import com.sleepycat.db.StatsConfig;
import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.dup.util.*;

import java.io.File;

/**
 * A Blueprints implementation of Berkeley database using duplicates (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine/)
 */
public class DupGraph implements Graph {
	
	final protected static DupRecordNumberComparator dupRecordNumberComparator = new DupRecordNumberComparator();
	
    private Environment dbEnv;
    
    //private Database classDb;
    //private StoredClassCatalog classCatalog;
    //protected SerialBinding<Object> serialBinding;
  
    public Database vertexDb;
    public Database outDb;
    public Database inDb;
    protected Database vertexPropertyDb;
    protected Database edgePropertyDb;
    
    // XXX This causes major concurrency problems
    
    final public DatabaseEntry key = new DatabaseEntry();
    final public DatabaseEntry data = new DatabaseEntry();

    /**
     * Creates a new instance of a BdbGraph at directory.
     *
     * @param directory The database environment's persistent directory name.
     */
    public DupGraph(final String directory) {
        try {
        	File envHome = new File(directory);
        	envHome.mkdirs();
        	
        	EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(true);
            envConf.setCacheMax(256);
            envConf.setInitializeCache(true);
            //envConf.setInitializeLocking(true);
            //envConf.setTransactional(true);
            
            this.dbEnv = new Environment(envHome, envConf);        
     
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setType(DatabaseType.BTREE);
            dbConfig.setBtreeComparator(dupRecordNumberComparator);
            
            //this.classDb = this.dbEnv.openDatabase(null, "class.db", null, dbConfig);
            //this.classCatalog = new StoredClassCatalog(this.classDb);
            //this.serialBinding = new SerialBinding<Object>(this.classCatalog, Object.class);
            
            dbConfig.setSortedDuplicates(true);
            this.outDb = this.dbEnv.openDatabase(null, "out.db", null, dbConfig);
            
            dbConfig.setBtreeComparator(null);
            this.inDb = this.dbEnv.openDatabase(null, "in.db", null, dbConfig);
            this.vertexPropertyDb = this.dbEnv.openDatabase(null, "vertexProperty.db", null, dbConfig);
            this.edgePropertyDb = this.dbEnv.openDatabase(null, "edgeProperty.db", null, dbConfig);
            
            dbConfig.setSortedDuplicates(false);
            dbConfig.setType(DatabaseType.QUEUE);
            dbConfig.setRecordLength(0);
            this.vertexDb = this.dbEnv.openDatabase(null, "vertex.db", null, dbConfig);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }        
    }

    // BLUEPRINTS GRAPH INTERFACE

    public Vertex addVertex(final Object id) {        
        try {
            //autoStartTransaction();
            final Vertex vertex = new DupVertex(this);
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return vertex;
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }     
    }

    public Vertex getVertex(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("DupGraph.getVertex(id) cannot be null.");
    	
    	try {
    		return new DupVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Vertex> getVertices() {
    	return new DupVertexSequence(this);
    }

    public long countVertices() {    	
    	// Note: The fast version of StatConfig does not give us the results
    	// we need, so this is kind of slow
    	DatabaseStats s;
		try {
			s = vertexDb.getStats(null, StatsConfig.DEFAULT);
			return ((QueueStats) s).getNumData();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
    }

    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((DupVertex) vertex).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public Vertex getRandomVertex() {
    	try {
    		return DupVertex.getRandomVertex(this);
    	}
    	catch (DatabaseException e) {
    		throw new RuntimeException(e);
    	}
    }

    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {    	
        try {
            //autoStartTransaction();
            final Edge edge = new DupEdge(
        		this,
        		(DupVertex) outVertex,
        		(DupVertex) inVertex,
        		label);
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
            return edge;
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Edge getEdge(final Object id) {
    	if (id == null)
    		throw new IllegalArgumentException("DupGraph.getEdge(id) cannot be null.");
    	
    	try {
    		return new DupEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
    	return new DupEdgeSequence(this);
    }

    public long countEdges() {    	
    	// Note: The fast version of StatConfig does not give us the results
    	// we need, so this is kind of slow
    	DatabaseStats s;
		try {
			s = outDb.getStats(null, StatsConfig.DEFAULT);
	    	return ((BtreeStats) s).getNumData();
		} catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
    }

    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((DupEdge) edge).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    public Edge getRandomEdge() {
    	try {
    		return DupEdge.getRandomEdge(this);
    	}
    	catch (DatabaseException e) {
    		throw new RuntimeException(e);
    	}
    }

    public void clear() {
        try {
        	vertexDb.truncate(null, false);
            outDb.truncate(null, false);
            inDb.truncate(null, false);
        	vertexPropertyDb.truncate(null,  false);
        	edgePropertyDb.truncate(null, false);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public void shutdown() {
        try {
            edgePropertyDb.close();
            edgePropertyDb = null;

            vertexPropertyDb.close();
            vertexPropertyDb = null;
            
            inDb.close();
            inDb = null;
            
            outDb.close();
            outDb = null;
            
            vertexDb.close();
            vertexDb = null;
            
            //classCatalog.close();
            //classCatalog = null;
            //classDb.close(); classCatalog.close() does this implicitly
            //classDb = null;
            
            dbEnv.close();
            dbEnv = null;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public String toString() {
    	try {
    		return "dupgraph[" + dbEnv.getHome() + "]";
    	} catch (Exception e) {
    		return "dupgraph[?]";
    	}
    }

    /* TRANSACTIONAL GRAPH INTERFACE

    public void setTransactionMode(final Mode mode) {
    	if (txn != null) {
    		try {
    			txn.commit();
    		} catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
            	txn = null;
            }
    	}
    	txnMode = mode;
    }

    public Mode getTransactionMode() {
        return txnMode;
    }

    public void startTransaction() {
        if (txnMode == Mode.AUTOMATIC)
            throw new RuntimeException(TransactionalGraph.TURN_OFF_MESSAGE);
        if (txn == null) {
        	try {
            	txn = dbEnv.beginTransaction(null, null);
        	} catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else
            throw new RuntimeException(TransactionalGraph.NESTED_MESSAGE);
    }

    protected void autoStartTransaction() {
        if (txnMode == Mode.AUTOMATIC) {
            if (txn == null) {
            	try {
                	txn = dbEnv.beginTransaction(null, null);
            	} catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            } else
                throw new RuntimeException(TransactionalGraph.NESTED_MESSAGE);
        }
    }

    public void stopTransaction(final Conclusion conclusion) {
        if (txnMode == Mode.AUTOMATIC)
            throw new RuntimeException(TransactionalGraph.TURN_OFF_MESSAGE);
        if (txn == null)
        	throw new RuntimeException("BdbGraph: There is no active transaction to stop.");
        try {
        	if (conclusion == TransactionalGraph.Conclusion.SUCCESS)
        		txn.commit();
        	else
        		txn.abort();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
        	txn = null;
        }
    }

    protected void autoStopTransaction(final Conclusion conclusion) {
        if (txnMode == Mode.AUTOMATIC) {
            if (txn == null)
            	throw new RuntimeException("BdbGraph: There is no active transaction to stop.");
            try {
            	if (conclusion == TransactionalGraph.Conclusion.SUCCESS)
            		txn.commit();
            	else
            		txn.abort();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            } finally {
            	txn = null;
            }
        }
    }
    
    */

    /* INDEXABLE GRAPH INTERFACE

    public <T extends Element> Index<T> createManualIndex(final String indexName, final Class<T> indexClass) {
        if (this.manualIndices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        final BdbIndex index = new BdbIndex(this, indexName, indexClass, Index.Type.MANUAL); // Your constructor may vary.
        this.manualIndices.put(index.getIndexName(), index);

        return index;
    }

    public <T extends Element> AutomaticIndex<T> createAutomaticIndex(final String indexName, final Class<T> indexClass, final Set<String> indexKeys) {
        if (this.autoIndices.containsKey(indexName))
            throw new RuntimeException("Index already exists: " + indexName);

        final BdbAutomaticIndex index = new BdbAutomaticIndex<BdbElement>(this, indexName, (Class<BdbElement>) indexClass, indexKeys); // Your constructor may vary.
        this.autoIndices.put(index.getIndexName(), index);
        // You may also need to put into manualIndices depending on implementation.

        return index;
    }

    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        Index<?> index = this.manualIndices.get(indexName);
        if (null == index) {
            index = this.autoIndices.get(indexName);
            if (null == index)
                throw new RuntimeException("No such index exists: " + indexName);
        }

        if (indexClass.isAssignableFrom(index.getIndexClass()))
            return (Index<T>) index;
        else
            throw new RuntimeException("Can not convert " + index.getIndexClass() + " to " + indexClass);
    }

    public Iterable<Index<? extends Element>> getIndices() {
        final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
        for (Index<?> index : this.manualIndices.values()) {
            list.add(index);
        }
        for (Index<?> index : this.autoIndices.values()) {
            list.add(index);
        }
        return list;
    }

    public void dropIndex(final String iIndexName) {
        if (this.manualIndices.remove(iIndexName) == null)
            this.autoIndices.remove(iIndexName);

        // And of course, drop the actual index from the raw graph.
    }

    */

}
