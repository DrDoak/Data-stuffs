package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate mPred;
    DbIterator[] mChild;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	mPred = p;
    	mChild = new DbIterator[1];
    	mChild[0] = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return mPred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return mChild[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	mChild[0].open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	mChild[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mChild[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	while (mChild[0].hasNext()){
    		Tuple nTup = mChild[0].next();
    		if (mPred.filter(nTup)){
    			return nTup;
    		} else {}
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
