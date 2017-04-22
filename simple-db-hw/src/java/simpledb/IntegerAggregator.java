package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    int mGBField;
    Type mGBFieldType;
    int mAField;
    Op mOp;
    Map<Field,List<Integer>> mMap;
    Map<Field, Integer> mResult;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	mGBField = gbfield;
    	mGBFieldType = gbfieldtype;
    	mAField = afield;
    	mOp = what;
    	mMap = new HashMap<Field,List<Integer>>();
    	mResult = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field curKey;
    	if (mGBField == NO_GROUPING){
    		curKey = null;
    	} else {
    		curKey = tup.getField(mGBField);
    	}
    	Integer curValue = ((IntField)tup.getField(mAField)).getValue();
    	if (!mMap.containsKey(curKey)) { 
    		List<Integer> newL = new ArrayList<Integer>();
    		newL.add(curValue);
    		mMap.put(curKey, newL);
    		if (mOp == Op.COUNT) {
    			mResult.put(curKey, 1);
    		} else {
    			mResult.put(curKey, curValue);
    		}
    	} else {
    		mMap.get(curKey).add(curValue);
    		if (mOp == Op.AVG) {
    			int sum = 0;
    			for (int i =0; i<mMap.get(curKey).size();i++) {
    				sum = sum + mMap.get(curKey).get(i);
    			}
    			mResult.put(curKey, sum /mMap.get(curKey).size());
    		}else if (mOp == Op.COUNT) {
    			mResult.put(curKey, (mResult.get(curKey) + 1));
    		}else if (mOp == Op.MIN) {
    			mResult.put(curKey, Math.min(mResult.get(curKey), curValue));
    		}else if (mOp == Op.MAX) {
    			mResult.put(curKey, Math.max(mResult.get(curKey), curValue));
    		}else if (mOp == Op.SUM) {
    			mResult.put(curKey, (mResult.get(curKey) + curValue));     			
    		}
    	}
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	List<Tuple> newTups = new ArrayList<Tuple>();
    	TupleDesc td;
    	if (mGBField != NO_GROUPING) { 
	    	Type[] typeList = new Type[2];
	    	String[] fieldAr = new String[2];
	    	typeList[0] = Type.INT_TYPE;
	    	typeList[1] = Type.INT_TYPE;
	    	fieldAr[0] = "groupVal";
	    	fieldAr[1] = "AggregateVal";
	    	td = new TupleDesc(typeList,fieldAr);
	    	for (Field key : mResult.keySet()) {
	    		Tuple nTup = new Tuple(td);
	    		nTup.setField(0, key);
	    		nTup.setField(1, new IntField(mResult.get(key)));
	    		newTups.add(nTup);
	    	}
    	} else {
    		Type[] typeList = new Type[1];
	    	String[] fieldAr = new String[1];
	    	typeList[0] = Type.INT_TYPE;
	    	fieldAr[0] = "AggregateVal";
	    	td = new TupleDesc(typeList,fieldAr);
	    	for (Field key : mResult.keySet()) {
	    		Tuple nTup = new Tuple(td);
	    		nTup.setField(0, new IntField(mResult.get(key)));
	    		newTups.add(nTup);
	    	}
    	}
    	TupleIterator newIter = new TupleIterator(td,newTups);
    	return newIter;
    }

}
