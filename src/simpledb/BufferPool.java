package simpledb;

import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private final Map<PageId, Node> idToPage;
    private final DoubleLinkedList dList;
    private final int numPages;
    
    private final LockManager lockManager;
    
    public class LockManager {
    	static final int LOCK_WAIT = 10;

    	final Map<PageId, Permissions> page2perm;
    	final Map<PageId, Set<TransactionId>> page2tids;
    	final Map<TransactionId, Set<PageId>> tid2pages;
    	final Map<TransactionId, Vector<TransactionId>> waitsFor;
    	
    	/**
    	 * Sets up the lock manager to keep track of page-level locks for transactions
    	 * Should initialize state required for the lock table data structure(s)
    	 */
    	private LockManager() {
    		page2perm = new HashMap<PageId, Permissions>();
    		page2tids = new HashMap<PageId, Set<TransactionId>>();
    		tid2pages = new HashMap<TransactionId, Set<PageId>>();
    		waitsFor = new HashMap<TransactionId, Vector<TransactionId>>();
    	}
    	
    	public boolean checkWaitsForDeadlock(TransactionId tid) {
    		if (!waitsFor.containsKey(tid)) return false;
    		Deque<TransactionId> stack = new ArrayDeque<TransactionId>();
    		for (TransactionId t : waitsFor.get(tid)) if (!t.equals(tid)) stack.push(tid);
    		while (!stack.isEmpty()) {
    			Vector<TransactionId> trans = null;
    			if ((trans = waitsFor.get(stack.pop())) == null || trans.size() == 0) continue;
    			for (TransactionId t : trans) {
    				if (t.equals(tid)) return true;
    				stack.push(t);
    			}
    		}
    		return false;
    	}
    	    	
    	/**
    	 * Tries to acquire a lock on page pid for transaction tid, with permissions perm. 
    	 * If cannot acquire the lock, waits for a timeout period, then tries again. 
    	 * This method does not return until the lock is granted, or an exception is thrown
    	 *
    	 * In Exercise 5, checking for deadlock will be added in this method
    	 * Note that a transaction should throw a DeadlockException in this method to 
    	 * signal that it should be aborted.
    	 *
    	 * @throws DeadlockException after on cycle-based deadlock
    	 */
    	public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
    	    throws DeadlockException {
    	    
    	    while(!lock(tid, pid, perm)) { // keep trying to get the lock
    		
    	    	synchronized(this) {
    	    		// you don't have the lock yet, deadlock detection
    	    		if (checkWaitsForDeadlock(tid)) throw new DeadlockException();
    	    	}
    		
    	    	try {
    	    		// couldn't get lock, wait for some time, then try again
    	    		Thread.sleep(LOCK_WAIT); 
    	    	} catch (InterruptedException e) { // do nothing
    	    	}
    	    }
    	    	    
    	    synchronized(this) {
    	    	waitsFor.remove(tid);
    	    }
    	    
    	    return true;
    	}
    	
    	/**
    	 * Release all locks corresponding to TransactionId tid.
    	 * This method is used by BufferPool.transactionComplete()
    	 */
		public synchronized void releaseAllLocks(TransactionId tid) {
			Set<PageId> pages = tid2pages.remove(tid);
			if (pages == null)
				return;
			for (PageId pid : pages) {
				Set<TransactionId> trans = null;
				if ((trans = page2tids.get(pid)) != null) {
					trans.remove(tid);
					if (trans.size() == 0) {
						page2perm.remove(pid);
						page2tids.remove(pid);
					}
				}
			}
			waitsFor.remove(tid);
			for (Vector<TransactionId> trans : waitsFor.values()) trans.remove(tid);
		}
    	
    	public synchronized Set<PageId> pagesLockedByTid(TransactionId tid) {
    		Set<PageId> set = null;
    		return (set = tid2pages.get(tid)) == null ? new HashSet<PageId>() : set;
    	}
    	
    	/** Return true if the specified transaction has a lock on the specified page */
    	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
    		Set<PageId> pages = null;
    	    return (pages = tid2pages.get(tid)) != null && pages.contains(p);
    	}
    	
    	/**
    	 * Answers the question: is this transaction "locked out" of acquiring lock on this page with this perm?
    	 * Returns false if this tid/pid/perm lock combo can be achieved (i.e., not locked out), true otherwise.
    	 * 
    	 * Logic:
    	 *
    	 * if perm == READ_ONLY
    	 *  if tid is holding any sort of lock on pid, then the tid can acquire the lock (return false).
    	 *
    	 *  if another tid is holding a READ lock on pid, then the tid can acquire the lock (return false).
    	 *  if another tid is holding a WRITE lock on pid, then tid can not currently 
    	 *  acquire the lock (return true).
    	 *
    	 * else
    	 *   if tid is THE ONLY ONE holding a READ lock on pid, then tid can acquire the lock (return false).
    	 *   if tid is holding a WRITE lock on pid, then the tid already has the lock (return false).
    	 *
    	 *   if another tid is holding any sort of lock on pid, then the tid cannot currenty acquire the lock (return true).
    	 */
    	private synchronized boolean locked(TransactionId tid, PageId pid, Permissions perm) {
    	    if (perm.equals(Permissions.READ_ONLY)) {
        		Set<PageId> pages = null;
        		Permissions permission = null;
    	    	if ((pages = tid2pages.get(tid)) != null && pages.contains(pid)) return false;
    	    	if ((permission = page2perm.get(pid)) != null) {
    	    		if (permission.equals(Permissions.READ_ONLY)) return false;
    	    		if (!waitsFor.containsKey(tid)) waitsFor.put(tid, new Vector<TransactionId>());
    	    		waitsFor.get(tid).addAll(page2tids.get(pid));
    	    		return true;
    	    	}
    	    	return false;
    	    } else {
    	    	Set<TransactionId> trans = null;
    	    	if ((trans = page2tids.get(pid)) != null && trans.size() > 0) {
    	    		if (trans.size() == 1 && trans.contains(tid)) return false;
    	    		if (!waitsFor.containsKey(tid)) waitsFor.put(tid, new Vector<TransactionId>());
    	    		waitsFor.get(tid).addAll(trans);
    	    		return true;
    	    	}
    	    	return false;
    	    }    	    
    	}
    	
    	
    	/**
    	 * Releases whatever lock this transaction has on this page
    	 * Should update lock table data structure(s)
    	 *
    	 * Note that you do not need to "wake up" another transaction that is waiting for a lock on this page,
    	 * since that transaction will be "sleeping" and will wake up and check if the page is available on its own
    	 * However, if you decide to change the fact that a thread is sleeping in acquireLock(), you would have to wake it up here
    	 */
    	public synchronized void releaseLock(TransactionId tid, PageId pid) {
    		Set<PageId> pages = null;
    		Set<TransactionId> trans = null;
    		if ((pages = tid2pages.get(tid)) != null) {
    			pages.remove(pid);
    			if ((trans = page2tids.get(pid)) != null) {
    				trans.remove(tid);
    				if (trans.size() == 0) {
        				page2perm.remove(pid);
        				page2tids.remove(pid); 					
    				}
    			}
    		}
    	}

    	/**
    	 * Attempt to lock the given PageId with the given Permissions for this TransactionId
    	 * Should update the lock table data structure(s) if successful
    	 *
    	 * Returns true if the lock attempt was successful, false otherwise
    	 */
    	private synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
    	    
    	    if(locked(tid, pid, perm)) 
    		return false; // this transaction cannot get the lock on this page; it is "locked out"

    	    // Else, this transaction is able to get the lock, update lock table
    	    Set<TransactionId> trans;
    	    Set<PageId> pages;
    	    page2perm.put(pid, perm);
    	    if ((trans = page2tids.get(pid)) == null) {
    	    	trans = new HashSet<TransactionId>();
    	    	page2tids.put(pid, trans);
    	    }
    	    trans.add(tid);
    	    if ((pages = tid2pages.get(tid)) == null) {
    	    	pages = new HashSet<PageId>();
    	    	tid2pages.put(tid, pages);
    	    }
    	    pages.add(pid);
    	    return true;
    	}
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.idToPage = new ConcurrentHashMap<PageId, Node>();
        this.dList = new DoubleLinkedList();
        this.numPages = numPages;
        this.lockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	try {
			lockManager.acquireLock(tid, pid, perm);
		} catch (DeadlockException e) {
			throw new TransactionAbortedException();
		}
    	Node n = null;
		synchronized (this) {
			if (idToPage.containsKey(pid)) {
				n = idToPage.get(pid);
				dList.removeNode(n);
				dList.insertToHead(n);
			} else {
				if (idToPage.size() == numPages) evictPage();
				n = new Node(pid, Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid));
				idToPage.put(pid, n);
				dList.insertToHead(n);
			}
		}
       	return n.page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
    	lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
    	Set<PageId> pages = lockManager.pagesLockedByTid(tid);
    	if (commit) {  		
    		for (PageId pid : pages) flushPage(pid);
    	} else {
    		for (PageId pid : pages) {
    			Node n = null;
    			if ((n = idToPage.get(pid)) != null){
    				DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    				n.page = dbFile.readPage(pid);
    			}
    		}
    	}
    	lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	DbFile dbFile = Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> list = dbFile.addTuple(tid, t);
		synchronized (this) {
			for (Page page : list) {
				page.markDirty(true, tid);
				getPage(tid, page.getId(), Permissions.READ_ONLY);
			}
		}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	PageId pid = t.getRecordId().getPageId();
    	DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
    	Page page = dbFile.deleteTuple(tid, t);
    	synchronized (this) {
			page.markDirty(true, tid);
			getPage(tid, page.getId(), Permissions.READ_ONLY);
    	}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (Node n : idToPage.values()) {
    		if (n.page.isDirty() != null) flushPage(n.pid);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	Node n = idToPage.get(pid);
    	if (n == null) return;
    	Page page = n.page;
    	Database.getCatalog().getDbFile(pid.getTableId()).writePage(page);
    	page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    	Iterator<Node> iterator = dList.iterator();
    	while (iterator.hasNext()) {
    		Node n = iterator.next();
    		if (n.page.isDirty() == null) {
    			try {
					flushPage(n.pid);
				} catch (IOException e) {
					throw new DbException("flush failed, PageId: " + n.pid);
				}
    			dList.removeNode(n);
    			idToPage.remove(n.pid);
    			return;
    		}
    	}
    	throw new DbException("can't find clean page to evict");
    }
    
    class Node {
    	PageId pid;
    	Page page;
    	Node prev, next;
    	
    	public Node (PageId pid, Page page) {
    		this.page = page;
    		this.pid = pid;
    	}
    }
    
    class DoubleLinkedList {
    	private final Node head, tail;
    	
    	public DoubleLinkedList() {
    		head = new Node(null, null);
    		tail = new Node(null, null);
    		head.next = tail;
    		tail.prev = head;
    	}
    	
    	public void removeNode(Node n) {
    	    if (n == head || n == tail) return;
    		n.prev.next = n.next;
    		n.next.prev = n.prev;
    	}
    	
    	public void insertToHead(Node n) {
    		n.next = head.next;
    		n.next.prev = n;
    		head.next = n;
    		n.prev = head;
    	}
    	
    	public Node removeFromTail() {
    		Node n = tail.prev;
    		if (n == head) return null;
    		tail.prev = n.prev;
    		tail.prev.next = tail;
    		return n;
    	}
    	
    	public Iterator<Node> iterator() {
    		return new Iterator<Node>() {
    			Node current = tail.prev;

				@Override
				public boolean hasNext() {
					return current != head;
				}

				@Override
				public Node next() {
					if (current == head) return null;
					current = current.prev;
					return current.next;
				}
			};
    	}
    }

}
