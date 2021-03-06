package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    DbIterator[] mChild;
    JoinPredicate mPred;
    boolean tup1NoSkip;
    boolean tup2NoSkip;
    Tuple nTup1;
    Tuple nTup2;
    
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
    	mChild = new DbIterator[2];
    	mChild[0] = child1;
    	mChild[1] = child2;
    	mPred = p;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return mPred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return mChild[0].toString();
        //return "";
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return mChild[1].toString();
        //return "";
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return simpledb.TupleDesc.merge(mChild[0].getTupleDesc(), mChild[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	mChild[0].open();
    	mChild[1].open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	mChild[0].close();
    	mChild[1].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mChild[0].rewind();
    	mChild[1].rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	while (mChild[0].hasNext() || tup1NoSkip){
    		if (tup1NoSkip) {
    			tup1NoSkip = false;
    		} else {
    			nTup1 = mChild[0].next();
    		}
    		while (mChild[1].hasNext()) {
    			if (tup2NoSkip) {
        			tup2NoSkip = false;
        		} else {
        			nTup2 = mChild[1].next();
        		}
    			//System.out.println("tup1: " + nTup1.toString() + " tup2: " + nTup2.toString());
	    		if (mPred.filter(nTup1, nTup2)) {
	    			//System.out.println("Succeeeded");
		    		Tuple newTup = new Tuple(getTupleDesc());
		    		Iterator<Field> nIter1 = nTup1.fields();
		    		Iterator<Field> nIter2 = nTup2.fields();
		    		int ind = 0;
		    		while (nIter1.hasNext()) {
		    			newTup.setField(ind, nIter1.next());
		    			ind ++;
		    		}
		    		while (nIter2.hasNext()) {
		    			newTup.setField(ind, nIter2.next());
		    			ind ++;
		    		}
		    		tup1NoSkip = true;
		    		//tup2NoSkip = true;
	    			return newTup;
	    		} else {}
    		}
    		mChild[1].rewind();
    	}
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return mChild;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	mChild = children;
    }

}
