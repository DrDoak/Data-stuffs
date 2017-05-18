package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    
    TransactionId mTrans;
    DbIterator mChild;
    DbIterator [] children;
    int mTableId;
    TupleDesc mTD;
    boolean alreadyCalled;
    
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	mTrans = t;
    	children = new DbIterator[1];
    	children[0] = child;
    	mChild = child;
    	Type[] newTypes = new Type[1];
    	newTypes[0] = Type.INT_TYPE;
    	mTD = new TupleDesc(newTypes);
    	alreadyCalled = false;
    }

    public TupleDesc getTupleDesc() {
    	return mTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	mChild.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	mChild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mChild.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (alreadyCalled) {
    		return null;
    	}
		alreadyCalled = true;
		int numDeleted = 0;
		while (children[0].hasNext()) {
			try {
				Database.getBufferPool().deleteTuple(mTrans, children[0].next());
			} catch (NoSuchElementException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			numDeleted ++;
		}
		Tuple newTuple = new Tuple(mTD);
		IntField newField = new IntField(numDeleted);
		newTuple.setField(0, newField);
		return newTuple;
    }

    @Override
    public DbIterator[] getChildren() {
    	// some code goes here
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	// some code goes here
    	this.children = children;
    }

}
