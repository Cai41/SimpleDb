package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	private final Type[] typeAr;
	private final String[] fieldAr;
	private final Map<String, Integer> mapToIndex = new HashMap<String, Integer>();
	private final String descriptor;
	private int size = 0;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
    	int len1 = td1.numFields(), len2 = td2.numFields();
    	Type[] typeAr = new Type[len1 + len2];
    	String[] fieldAr = new String[len1 + len2];
    	
    	Type[] typeAr1 = td1.typeAr, typeAr2 = td2.typeAr;
    	String[] fieldAr1 = td1.fieldAr, fieldAr2 = td2.fieldAr;
    	for (int i = 0; i < len1; i++) {
    		typeAr[i] = typeAr1[i];
    		fieldAr[i] = fieldAr1[i];
    	}
    	for (int i = 0; i < len2; i++) {
    		typeAr[len1 + i] = typeAr2[i];
    		fieldAr[len1 + i] = fieldAr2[i];    		
    	}
    	return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.typeAr = Arrays.copyOf(typeAr, typeAr.length);
        this.fieldAr = Arrays.copyOf(fieldAr, fieldAr.length);
        for (int i = 0; i < fieldAr.length; i++) {
        	if (fieldAr[i] != null) mapToIndex.put(fieldAr[i], i);
        }
        for (Type type : typeAr) {
        	size += type.getLen();
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < typeAr.length; i++) {
        	sb.append(typeAr[i]).append("(").append(fieldAr[i]).append(")");
        	if (i < typeAr.length - 1) sb.append(", ");
        }
        descriptor = sb.toString();
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= typeAr.length) {
        	throw new NoSuchElementException(String.format(
        			"i should be smallerthan the number of fields, numFields: %d, i: %d",
        			typeAr.length,
        			i
        		));
        }
        return fieldAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (mapToIndex.containsKey(name)) {
        	return mapToIndex.get(name);
        } else {
        	throw new NoSuchElementException(String.format(
        			"no field with a matching name: %s is found",
        			name
        		));
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        if (i >= typeAr.length) {
        	throw new NoSuchElementException(String.format(
        			"i should be smallerthan the number of fields, numFields: %d, i: %d",
        			typeAr.length,
        			i
        		));
        }
        return typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (this == o) return true;
    	if (!(o instanceof TupleDesc)) return false;
        TupleDesc tupleDesc = (TupleDesc)o;
        
        if (tupleDesc.getSize() != size) return false;
        
        Type[] typeAr1 = tupleDesc.typeAr;
        if (typeAr1.length != typeAr.length) return false;
        for (int i = 0; i < typeAr.length; i++) {
        	if (!typeAr1[i].equals(typeAr[i])) return false;
        }
        return true;
    }

    public int hashCode() {
    	int result = 17;
    	result = 31 * result + size;
    	for (Type type : typeAr) {
    		result = 31 * result + type.hashCode();
    	}
    	return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        return descriptor;
    }
}
