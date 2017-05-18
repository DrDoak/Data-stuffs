package simpledb;

import java.io.Serializable;
import java.util.*;
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    public ArrayList<TDItem> mItems;
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // ---
        return mItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // ---
    	if (typeAr.length <= 0) {
    		System.out.println("Temporary Error: Length of typeAr less than 1");
    	}
    	
    	mItems = new ArrayList<TDItem>();
    	for (int i = 0; i < typeAr.length; i ++) {
    		TDItem newItem = new TDItem(typeAr[i],fieldAr[i]);
    		mItems.add(newItem);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	mItems = new ArrayList<TDItem>();
    	for (int i = 0; i < typeAr.length; i ++) {
    		TDItem newItem = new TDItem(typeAr[i],null);
    		mItems.add(newItem);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return mItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= mItems.size()) {
    		throw new NoSuchElementException();
    	}
        return mItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i < 0 || i >= mItems.size()) {
    		throw new NoSuchElementException();
    	}
        return mItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
    	for (int i = 0; i < mItems.size(); i ++) {
    		if (mItems.get(i) != null && name.equals(mItems.get(i).fieldName)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int totalSize = 0;
    	for (int i = 0; i < mItems.size(); i ++) {
    		totalSize = totalSize + mItems.get(i).fieldType.getLen();
    	}
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	Type [] tArray = new Type [0];
        TupleDesc newTupleDesc = new TupleDesc(tArray);
    	newTupleDesc.mItems.addAll(td1.mItems);
    	newTupleDesc.mItems.addAll(td2.mItems);
        return newTupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	if (o instanceof TupleDesc){
    		TupleDesc other = (TupleDesc)o;
    		if (other.mItems.size() != mItems.size())
    			return false;
    		for (int i = 0; i < other.mItems.size();i++) {
    			if (!(other.mItems.get(i).fieldType.getLen() == mItems.get(i).fieldType.getLen()))
    				return false;
    		}
    		return true;
    	}
    	else
    		return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hash = this.toString().hashCode();
    	return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String returnStr = "";
    	for (int i = 0; i < mItems.size(); i ++) {
    		returnStr += mItems.get(i).fieldType + "(" + mItems.get(i).fieldName + "),";
    	}
        return returnStr;
    }
}
