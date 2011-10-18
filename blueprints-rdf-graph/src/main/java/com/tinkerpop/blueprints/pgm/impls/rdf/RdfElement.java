package com.tinkerpop.blueprints.pgm.impls.rdf;

import com.tinkerpop.blueprints.pgm.Element;
import com.tinkerpop.blueprints.pgm.impls.sail.SailTokens;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;

public abstract class RdfElement implements Element {

	public static final URI propertyPred = new URIImpl(RdfGraph.RDFGRAPH_NS + "property");
	
    protected static Map<String, String> classToDataType = new HashMap<String, String>();
    protected static Map<String, String> dataTypeToClass = new HashMap<String, String>();

    static {
    	classToDataType.put("java.lang.String", SailTokens.XSD_NS + "string");
    	classToDataType.put("java.lang.Integer", SailTokens.XSD_NS + "integer");
    	classToDataType.put("java.lang.Float", SailTokens.XSD_NS + "float");
    	classToDataType.put("java.lang.Double", SailTokens.XSD_NS + "double");
        dataTypeToClass.put(SailTokens.XSD_NS + "string", "java.lang.String");
        dataTypeToClass.put(SailTokens.XSD_NS + "int", "java.lang.Integer");
        dataTypeToClass.put(SailTokens.XSD_NS + "integer", "java.lang.Integer");
        dataTypeToClass.put(SailTokens.XSD_NS + "float", "java.lang.Float");
        dataTypeToClass.put(SailTokens.XSD_NS + "double", "java.lang.Double");
    }
    
    public static Object castLiteral(final Literal literal) {
        if (null != literal.getDatatype()) {
            String className = dataTypeToClass.get(literal.getDatatype().stringValue());
            if (null == className)
                return literal.getLabel();
            else {
                try {
                    Class c = Class.forName(className);
                    if (c == String.class) {
                        return literal.getLabel();
                    } else if (c == Float.class) {
                        return Float.valueOf(literal.getLabel());
                    } else if (c == Integer.class) {
                        return Integer.valueOf(literal.getLabel());
                    } else if (c == Double.class) {
                        return Double.valueOf(literal.getLabel());
                    } else {
                        return literal.getLabel();
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return literal.getLabel();
                }
            }
        } else {
            return literal.getLabel();
        }
    }

    public static Literal castObject(final Object object) {
    	String typeString = classToDataType.get(object.getClass().getName());
    	if (typeString == null)
    		throw new RuntimeException("Object " + " is not of a valid data type.");
    	URI dataType = new URIImpl(typeString);
    	return new LiteralImpl(object.toString(), dataType);
    }
    
	public abstract Object getId();

	public abstract Object getProperty(String key);

	public abstract Set<String> getPropertyKeys();

	public abstract void setProperty(String key, Object value);
	
	public abstract Object removeProperty(String key);
}
