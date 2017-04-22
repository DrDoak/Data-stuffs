package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    int mGBField;
    Type mGBFieldType;
    int mAField;
    Op mOp;
    Map<Field, Integer> mResult;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException{
        // some code goes here
    	mGBField = gbfield;
    	mGBFieldType = gbfieldtype;
    	mAField = afield;
    	mOp = what;
    	if (mOp != Op.COUNT) {
    		throw new IllegalArgumentException();
    	}
    	mResult = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field curKey;
    	if (mGBField == NO_GROUPING){
    		curKey = null;
    	} else {
    		curKey = tup.getField(mGBField);
    	};
    	if (!mResult.containsKey(curKey)) { 
    		mResult.put(curKey, 1);
    	} else {
    		mResult.put(curKey, (mResult.get(curKey) + 1));
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
