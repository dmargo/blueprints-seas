package com.tinkerpop.blueprints.pgm.impls.rdf.impls;

import com.tinkerpop.blueprints.pgm.impls.rdf.RdfGraph;
import net.fortytwo.linkeddata.sail.LinkedDataSail;
import net.fortytwo.ripple.Ripple;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LinkedDataRdfGraph extends RdfGraph {

    public LinkedDataRdfGraph(final RdfGraph storageGraph) {
        try {
            Ripple.initialize();
            this.startSail(new LinkedDataSail(storageGraph.getRawGraph()));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
