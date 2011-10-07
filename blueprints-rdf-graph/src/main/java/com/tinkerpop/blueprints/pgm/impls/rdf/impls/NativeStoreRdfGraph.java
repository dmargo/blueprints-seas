package com.tinkerpop.blueprints.pgm.impls.rdf.impls;

import com.tinkerpop.blueprints.pgm.impls.rdf.RdfGraph;
import org.openrdf.sail.nativerdf.NativeStore;

import java.io.File;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class NativeStoreRdfGraph extends RdfGraph {

    public NativeStoreRdfGraph(final String directory) {
    	super(new NativeStore(new File(directory), "cspo,cops"));
    }

    public NativeStoreRdfGraph(final String directory, final String tripleIndices) {
    	super(new NativeStore(new File(directory), tripleIndices));

    }
    
    public String toString() {
        String type = this.rawGraph.getClass().getSimpleName().toLowerCase();
        return "nativestorerdfgraph[" + type + "]";
    }    
}
