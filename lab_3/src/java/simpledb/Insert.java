package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    TransactionId mTrans;
    DbIterator mChild;
    DbIterator [] children;
    int mTableId;
    TupleDesc mTD;
    boolean alreadyCalled;
    
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
    	mTrans = t;
    	children = new DbIterator[1];
    	children[0] = child;
    	mChild = child;
    	mTableId = tableId;
    	Type[] newTypes = new Type[1];
    	newTypes[0] = Type.INT_TYPE;
    	mTD = new TupleDesc(newTypes);
    	alreadyCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return mTD;
        //return Database.getCatalog().getDatabaseFile(mTableId).getTupleDesc();
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (alreadyCalled) {
    		return null;
    	} else {
    		alreadyCalled = true;
    		int numInserted = 0;
    		while (children[0].hasNext()) {
    			try {
					Database.getBufferPool().insertTuple(mTrans, mTableId, children[0].next());
				} catch (NoSuchElementException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			numInserted ++;
    		}
    		Tuple newTuple = new Tuple(mTD);
    		IntField newField = new IntField(numInserted);
    		newTuple.setField(0, newField);
    		return newTuple;
    	}
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
