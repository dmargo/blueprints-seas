package com.tinkerpop.blueprints.pgm.impls.sql;

import com.tinkerpop.blueprints.pgm.*;
import com.tinkerpop.blueprints.pgm.impls.sql.util.*;

import java.sql.*;

/**
 * A Blueprints implementation of an SQL database (http://www.oracle.com)
 *
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public class SqlGraph implements Graph {
	private String addr = null;
	public Connection connection = null;

    /**
     * Creates a new instance of a SqlGraph at directory.
     *
     * @param file The database environment's persistent file name.
     */
    public SqlGraph(final String addr) {
    	this.addr = addr;
    	
    	try {
        	Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.connection = DriverManager.getConnection(
            		"jdbc:mysql:" + this.addr + "&autoDeserialize=true");        
            Statement statement = this.connection.createStatement();
            
            // Create structure tables.
            statement.executeUpdate(
            		"create table if not exists vertex(" +
            			"vid serial primary key)");
            
            statement.executeUpdate(
            		"create table if not exists edge(" +
	            		"eid serial primary key," +
	            		"outid bigint unsigned not null references vertex(vid)," +
	            		"inid bigint unsigned not null references vertex(vid)," +
	            		"label varchar(255))");
            
            // Create property tables.
            statement.executeUpdate(
            		"create table if not exists vertexproperty(" +
	            		"vid bigint unsigned not null references vertex(vid)," +
	            		"pkey varchar(255) not null," +
	            		"value blob," +
	            		"unique(vid,pkey))");
            
            statement.executeUpdate(
            		"create table if not exists edgeproperty(" +
	            		"eid bigint unsigned not null references edge(eid)," +
	            		"pkey varchar(255) not null," +
	            		"value blob," +
	            		"unique(eid,pkey))");
        	
        	// Create the shortest path procedure.
            statement.executeUpdate("DROP PROCEDURE IF EXISTS dijkstra");
        	statement.executeUpdate(
        			"CREATE PROCEDURE dijkstra( sourceid BIGINT UNSIGNED, targetid BIGINT UNSIGNED)\r\n" + 
        			"BEGIN\r\n" + 
        			"    DECLARE vid BIGINT UNSIGNED;\r\n" + 
        			"    DROP TEMPORARY TABLE IF EXISTS paths;\r\n" + 
        			"    CREATE TEMPORARY TABLE paths (\r\n" + 
        			"        id BIGINT UNSIGNED NOT NULL PRIMARY KEY,\r\n" + 
        			"        prev BIGINT UNSIGNED\r\n" + 
        			"    );\r\n" + 
        			"\r\n" + 
        			"    INSERT INTO paths (id, prev) VALUES (sourceid, NULL);\r\n" + 
        			"    SET vid = (sourceid);\r\n" + 
        			"    WHILE vid IS NOT NULL DO\r\n" + 
        			"    BEGIN\r\n" + 
        			"        INSERT IGNORE INTO paths (id, prev)\r\n" + 
        			"        SELECT (inid, outid)\r\n" + 
        			"        FROM edge\r\n" + 
        			"        WHERE vid = edge.outid;\r\n" + 
        			"\r\n" + 
        			"        IF EXISTS (SELECT id FROM paths WHERE targetid = paths.id)\r\n" + 
        			"        THEN\r\n" + 
        			"            SET vid = NULL;\r\n" + 
        			"        ELSE\r\n" + 
        			"            SET vid = (\r\n" + 
        			"                SELECT inid\r\n" + 
        			"                FROM edge\r\n" + 
        			"                WHERE vid = edge.outid\r\n" + 
        			"            );\r\n" + 
        			"        END IF;\r\n" + 
        			"    END;\r\n" + 
        			"    END WHILE;\r\n" + 
        			"\r\n" + 
        			"    DROP TEMPORARY TABLE IF EXISTS result;\r\n" + 
        			"    CREATE TEMPORARY TABLE result (\r\n" + 
        			"        id BIGINT UNSIGNED NOT NULL PRIMARY KEY\r\n" + 
        			"    );\r\n" + 
        			"\r\n" + 
        			"    SET vid = (targetid);\r\n" + 
        			"    WHILE vid IS NOT NULL DO\r\n" + 
        			"    BEGIN\r\n" + 
        			"        INSERT INTO result (id) VALUES (vid);\r\n" + 
        			"\r\n" + 
        			"        SET vid = (\r\n" + 
        			"            SELECT prev\r\n" + 
        			"            FROM paths\r\n" + 
        			"            WHERE vid = paths.id);\r\n" + 
        			"    END;\r\n" + 
        			"    END WHILE;\r\n" + 
        			"\r\n" + 
        			"    SELECT id FROM result;\r\n" + 
        			"END;");
        	statement.close();
        	
        	// Create prepared statements.
        	this.addVertexStatement = this.connection.prepareStatement(
        			"insert into vertex values(default)",
        			Statement.RETURN_GENERATED_KEYS);
        	
        	this.getVertexStatement = this.connection.prepareStatement(
        			"select exists(select * from vertex where vid=?)");
        	
        	this.removeVertexStatement = this.connection.prepareStatement(
        			"delete from vertex where vid=?");
        	this.removeVertexPropertiesStatement = this.connection.prepareStatement(
        			"delete from vertexproperty where vid=?");
        	
        	this.getVertexPropertyStatement = this.connection.prepareStatement(
        			"select value from vertexproperty where vid=? and pkey=?");
        	
        	this.getVertexPropertyKeysStatement = this.connection.prepareStatement(
					"select pkey from vertexproperty where vid=?");
        	
        	this.setVertexPropertyStatement = this.connection.prepareStatement(
        			"replace into vertexproperty values(?,?,?)");
        	
        	this.removeVertexPropertyStatement = this.connection.prepareStatement(
    				"delete from vertexproperty where vid=? and pkey=?");
        	
        	this.getEdgeVerticesStatement = this.connection.prepareStatement(
					"select exists(select * from vertex where vid=?) " +
        			   "and exists(select * from vertex where vid=?)");
        	this.addEdgeStatement = this.connection.prepareStatement(
        			"insert into edge(outid,inid,label) values(?,?,?)");
        	
        	this.getEdgeStatement = this.connection.prepareStatement(
        			"select outid,inid,label from edge where eid=?");
        	
        	this.removeEdgeStatement = this.connection.prepareStatement(
        			"delete from edge where eid=?");
        	this.removeEdgePropertiesStatement = this.connection.prepareStatement(
        			"delete from edgeproperty where eid=?");
        	
        	this.getEdgePropertyStatement = this.connection.prepareStatement(
        			"select value from edgeproperty where eid=? and pkey=?");
        	
        	this.getEdgePropertyKeysStatement = this.connection.prepareStatement(
        			"select pkey from edgeproperty where eid=?");
        	
        	this.setEdgePropertyStatement = this.connection.prepareStatement(
        			"replace into edgeproperty values(?,?,?)");
        	
        	this.removeEdgePropertyStatement = this.connection.prepareStatement(
        			"delete from edgeproperty where eid=? and pkey=?");
        			
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }        
    }

    // BLUEPRINTS GRAPH INTERFACE
    protected PreparedStatement addVertexStatement;
    public Vertex addVertex(final Object id) {        
        try {
            //autoStartTransaction();
            final Vertex vertex = new SqlVertex(this);
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

    protected PreparedStatement getVertexStatement;
    public Vertex getVertex(final Object id) {
    	try {
    		return new SqlVertex(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Vertex> getVertices() {
        return new SqlVertexSequence(this);
    }

    protected PreparedStatement removeVertexStatement;
    protected PreparedStatement removeVertexPropertiesStatement;
    public void removeVertex(final Vertex vertex) {
        if (vertex == null || vertex.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((SqlVertex) vertex).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getVertexPropertyStatement;
    protected PreparedStatement getVertexPropertyKeysStatement;
    protected PreparedStatement setVertexPropertyStatement;
    protected PreparedStatement removeVertexPropertyStatement;

    protected PreparedStatement getEdgeVerticesStatement;
    protected PreparedStatement addEdgeStatement;
    public Edge addEdge(
		final Object id,
		final Vertex outVertex,
		final Vertex inVertex,
		final String label)
    {    	
        try {
            //autoStartTransaction();
            final Edge edge = new SqlEdge(
        		this,
        		(SqlVertex) outVertex,
        		(SqlVertex) inVertex,
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

    protected PreparedStatement getEdgeStatement;
    public Edge getEdge(final Object id) {
    	try {
    		return new SqlEdge(this, id);
    	} catch (Exception e) {
    		return null;
    	}
    }

    public Iterable<Edge> getEdges() {
        return new SqlEdgeSequence(this);
    }

    protected PreparedStatement removeEdgeStatement;
    protected PreparedStatement removeEdgePropertiesStatement;
    public void removeEdge(final Edge edge) {
        if (edge == null || edge.getId() == null)
            return;
        try {
            //autoStartTransaction();
            ((SqlEdge) edge).remove();
            //autoStopTransaction(TransactionalGraph.Conclusion.SUCCESS);
        } catch (RuntimeException e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw e;
        } catch (Exception e) {
            //autoStopTransaction(TransactionalGraph.Conclusion.FAILURE);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    protected PreparedStatement getEdgePropertyStatement;
    protected PreparedStatement getEdgePropertyKeysStatement;
    protected PreparedStatement setEdgePropertyStatement;
    protected PreparedStatement removeEdgePropertyStatement;
    
    public Iterable<Vertex> getShortestPath(final Vertex source, final Vertex target) {
    	return new SqlVertexSequence(this, ((SqlVertex) source).vid, ((SqlVertex) target).vid);
    }

    public void clear() {
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("delete from edgeproperty");
        	statement.executeUpdate("delete from vertexproperty");
        	statement.executeUpdate("delete from edge");
        	statement.executeUpdate("delete from vertex");
    		statement.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }
    
    public void delete() {
        try {
        	Statement statement = this.connection.createStatement();
        	statement.executeUpdate("drop table edgeproperty");
        	statement.executeUpdate("drop table vertexproperty");
        	statement.executeUpdate("drop table edge");
        	statement.executeUpdate("drop table vertex");
    		statement.close();
    		this.connection.close();
    	} catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }      
    }

    public void shutdown() {
        try {
    		this.connection.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }   
    }

    public String toString() {
    	try {
    		return "sqlgraph[" + this.addr + "]";
    	} catch (Exception e) {
    		return "sqlgraph[?]";
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
