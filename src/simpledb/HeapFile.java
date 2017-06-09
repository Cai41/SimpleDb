package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private final File file;
	private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgNo = pid.pageno();
        long pgOffset = pgNo * BufferPool.PAGE_SIZE;
        HeapPage heapPage = null;
        try {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			raf.seek(pgOffset);
			byte[] data = new byte[BufferPool.PAGE_SIZE];
			for (int i = 0; i < BufferPool.PAGE_SIZE; i++) {
				data[i] = raf.readByte();
			}
			HeapPageId id = new HeapPageId(pid.getTableId(), pid.pageno());
			heapPage = new HeapPage(id, data);
			raf.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        return heapPage;
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
        return (int)(file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator();
    }
    
    class HeapFileIterator implements DbFileIterator {
    	private int pageIndex = 0;
    	private Iterator<Tuple> iterator = null;
    	private int numPages = 0;
    	private final BufferPool pool;
    	
    	public HeapFileIterator() {
    		numPages = numPages();
    		pageIndex = numPages;
    		pool = Database.getBufferPool();
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			reset();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			while ((iterator == null || !iterator.hasNext()) && pageIndex < numPages) {
				HeapPage page = (HeapPage) pool.getPage(new TransactionId(), 
						new HeapPageId(getId(), pageIndex++), 
						Permissions.READ_WRITE);
				iterator = page.iterator();
			}
			return iterator != null && iterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext()) {
				throw new NoSuchElementException("no more tuples");
			}
			return iterator.next();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			reset();
		}

		@Override
		public void close() {
			pageIndex = numPages;
			iterator = null;
		}
		
		private void reset() throws TransactionAbortedException, DbException {
			pageIndex = 0;
			HeapPage page = (HeapPage) pool.getPage(new TransactionId(), 
					new HeapPageId(getId(), pageIndex++), 
					Permissions.READ_WRITE);
			iterator = page.iterator();
		}
    	
    }
    
}

