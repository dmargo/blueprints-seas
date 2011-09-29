package com.tinkerpop.blueprints.pgm.impls.bdb;

import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.SecondaryConfig;
import com.sleepycat.db.SecondaryDatabase;
import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.bdb.util.*;

import java.io.File;

/**
 * A Blueprints implementation of Berkeley database (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 * @author Elaine Angelino (http://www.eecs.harvard.edu/~elaine/)
 */
public class BdbGraph implements Graph {
    private Environment dbEnv;
    
    private Database classDb;
    private StoredClassCatalog classCatalog;
    protected SerialBinding<Object> serialBinding;
    
    public BdbPrimaryKeyBinding primaryKeyBinding;
    protected BdbEdgeKeyBinding edgeKeyBinding;
    
    protected Database graphDb;
    public SecondaryDatabase vertexDb;
    public SecondaryDatabase outDb;
    protected SecondaryDatabase inDb;
    protected SecondaryDatabase vertexPropertyDb;
    protected SecondaryDatabase edgePropertyDb;
    
    //protected Mode txnMode;
    //protected Transaction txn;

    /**
     * Creates a new instance of a BdbGraph at directory.
     *
     * @param directory The database environment's persistent directory name.
     */
    public BdbGraph(final String directory) {
        try {
        	File envHome = new File(directory);
        	envHome.mkdirs();
        	
        	EnvironmentConfig envConf = new EnvironmentConfig();
            envConf.setAllowCreate(true);
            envConf.setCacheMax(256);
            envConf.setInitializeCache(true);
            //envConf.setInitializeLocking(true);
            //envConf.setTransactional(true);
            dbEnv = new Environment(envHome, envConf);        
     
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setType(DatabaseType.BTREE);
            
            classDb = dbEnv.openDatabase(null, "class.db", null, dbConfig);
            classCatalog = new StoredClassCatalog(classDb);
            serialBinding = new SerialBinding<Object>(classCatalog, Object.class);
            
            primaryKeyBinding = new BdbPrimaryKeyBinding();
            dbConfig.setBtreeComparator(new BdbPrimaryKeyComparator(primaryKeyBinding));
            graphDb = dbEnv.openDatabase(null, "graph.db", null, dbConfig);
            
            SecondaryConfig secConfig = new SecondaryConfig();
            secConfig.setAllowCreate(true);
            secConfig.setAllowPopulate(true);
            secConfig.setSortedDuplicates(true);
            secConfig.setType(DatabaseType.BTREE);
            
            secConfig.setKeyCreator(new BdbVertexKeyCreator(primaryKeyBinding));
            vertexDb = dbEnv.openSecondaryDatabase(null, "vertex.db", null, graphDb, secConfig);

            secConfig.setKeyCreator(new BdbOutKeyCreator(primaryKeyBinding));
            outDb = dbEnv.openSecondaryDatabase(null, "out.db", null, graphDb, secConfig);

            secConfig.setKeyCreator(new BdbInKeyCreator(primaryKeyBinding));
            inDb = dbEnv.openSecondaryDatabase(null, "in.db", null, graphDb, secConfig);

            secConfig.setKeyCreator(new BdbVertexPropertyKeyCreator(primaryKeyBinding));
            vertexPropertyDb = dbEnv.openSecondaryDatabase(null, "vertexProperty.db", null, graphDb, secConfig);

            edgeKeyBinding = new BdbEdgeKeyBinding();
            secConfig.setKeyCreator(new BdbEdgePropertyKeyCreator(primaryKeyBinding, edgeKeyBinding));
            edgePropertyDb = dbEnv.openSecondaryDatabase(null, "edgeProperty", null, graphDb, secConfig);
            
            //txnMode = Mode.AUTOMATIC;
            //txn = null;

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
            final Vertex vertex = new BdbVertex(this);
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
    	try {
    		return new BdbVertex(this, id);
    	} catch (Exception e) {
			return null;
		}
    }

    public Iterable<Vertex> getVertices() {
        return new BdbVertexSequence(this);
    }

    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((BdbVertex) vertex).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {    	
        try {
            //autoStartTransaction();
            final Edge edge = new BdbEdge(this, (BdbVertex) outVertex, (BdbVertex) inVertex, label);
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
    	try {
    		return new BdbEdge(this, id);
    	} catch(Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
        return new BdbEdgeSequence(this);
    }

    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((BdbEdge) edge).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void clear() {
        try {
            graphDb.truncate(null, false);
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
            
            graphDb.close();
            graphDb = null;
            
            classCatalog.close();
            classCatalog = null;
            //classDb.close(); classCatalog.close() does this implicitly
            classDb = null;
            
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
    		return "bdbgraph[" + dbEnv.getHome() + "]";
    	} catch (Exception e) {
    		return "bdbgraph[?]";
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
