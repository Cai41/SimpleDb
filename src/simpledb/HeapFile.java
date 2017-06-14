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
	private final int Id;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.Id = file.getAbsoluteFile().hashCode();
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
        return Id;
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
    	if (pid.getTableId() != getId()) throw new IllegalArgumentException();
        int pgNo = pid.pageno();
        long pgOffset = pgNo * BufferPool.PAGE_SIZE;
        HeapPage heapPage = null;
        try {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			raf.seek(pgOffset);
			byte[] data = new byte[BufferPool.PAGE_SIZE];
			raf.read(data);
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
    	int pgNo = page.getId().pageno();
    	long pgOffset = pgNo * BufferPool.PAGE_SIZE;
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		raf.seek(pgOffset);
		raf.write(page.getPageData());
		raf.close();
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
    	int numPages = numPages();
    	BufferPool bufferPool = Database.getBufferPool();
    	ArrayList<Page> list = new ArrayList<Page>();
    	int index = 0;
    	HeapPage page = null;
    	while (index < numPages) {
    		page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(getId(), index), Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) break;
    		else index++;
    	}
    	if (index == numPages) {
    		FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
    		BufferedWriter bw = new BufferedWriter(fw);
    		for (int i = 0; i < BufferPool.PAGE_SIZE; i++) bw.write(0);
    		bw.close();
    		fw.close();
    		page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(getId(), numPages), Permissions.READ_WRITE);
    	}
    	page.addTuple(t);
    	list.add(page);
    	return list;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	page.deleteTuple(t);
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }
    
    class HeapFileIterator implements DbFileIterator {
    	private int pageIndex = 0;
    	private Iterator<Tuple> iterator = null;
    	private int numPages = 0;
    	private final BufferPool pool;
    	private final TransactionId tid;
    	
    	public HeapFileIterator(TransactionId tid) {
    		numPages = numPages();
    		pageIndex = numPages;
    		pool = Database.getBufferPool();
    		this.tid = tid;
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			reset();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			while ((iterator == null || !iterator.hasNext()) && pageIndex < numPages) {
				HeapPage page = (HeapPage) pool.getPage(tid, 
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
			HeapPage page = (HeapPage) pool.getPage(tid, 
					new HeapPageId(getId(), pageIndex++), 
					Permissions.READ_WRITE);
			iterator = page.iterator();
		}
    	
    }
    
}

