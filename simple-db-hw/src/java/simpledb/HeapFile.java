package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	
	//Custom class to implement DbFileIterator, and subclass of AbstractDbFileIterator
	public static class HeapFileIter extends AbstractDbFileIterator{
		public HeapFile mHeapFile;
		public TransactionId mTid;
		public Iterator<Tuple> mTupleIter;
		int currentPageNo;
		public HeapFileIter(HeapFile hf, TransactionId tid){
			mHeapFile = hf;
			mTid = tid;
		}
		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			currentPageNo = 0;
			HeapPageId firstHeapPageId = new HeapPageId(mHeapFile.getId(), currentPageNo);
	    	Page currPage = Database.getBufferPool().getPage(mTid, firstHeapPageId, Permissions.READ_ONLY);
	    	HeapPage currHeapPage = (HeapPage) currPage;
			mTupleIter = currHeapPage.iterator();
		}
		
		@Override
		public void close() {
		//	mHeapFile = null;
			mTid = null;
			mTupleIter = null;
			currentPageNo = 0;
			super.close();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
			
		}

		@Override
		protected Tuple readNext() throws DbException, TransactionAbortedException {
			if (mTupleIter==null){
				return null;
			}
			if (mTupleIter.hasNext()) {
				return mTupleIter.next();
			} else {
				currentPageNo ++;
				if (currentPageNo < mHeapFile.numPages()) {
					HeapPageId pageID = new HeapPageId(mHeapFile.getId(), currentPageNo);
			    	Page currPage = Database.getBufferPool().getPage(mTid, pageID, Permissions.READ_ONLY);
			    	HeapPage currHeapPage = (HeapPage) currPage;
					mTupleIter = currHeapPage.iterator();
					return readNext();
				} else {
					return null;
				}
			}
		}		
	}
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	File mFile;
	RandomAccessFile mRandom;
	TupleDesc mTupleDesc;
	Integer mID;
	Map<PageId, HeapPage> mPages;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	//mRandom = new RandomAccessFile(f, "r");
    	mFile = f;
    	mTupleDesc = td;    
    	mPages = new HashMap<PageId,HeapPage>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return mFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return mFile.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return mTupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	
        // some code goes here
    	if (mPages.containsKey(pid)) {
    		return mPages.get(pid);
    	} else {
	    	RandomAccessFile newRandom;
			try {
				newRandom = new RandomAccessFile(mFile,"r");
				byte [] newAr = new byte[BufferPool.getPageSize()];
				long startPos = pid.getPageNumber() * BufferPool.getPageSize();
				newRandom.seek(startPos);
		    	newRandom.read(newAr);
		    	HeapPage newPage = new HeapPage((HeapPageId)pid,newAr);
		    	newRandom.close();
		    	mPages.put(pid, newPage);
		        return newPage;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	return mPages.get(pid);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)mFile.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIter(this, tid);
    }

}

