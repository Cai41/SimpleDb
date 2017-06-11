package simpledb;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
	private static final TupleDesc TUPLE_DESC = new TupleDesc(new Type[]{Type.INT_TYPE});
	
	private final DbIterator child;
	private final TransactionId tid;
	private final int tableId;
	private Tuple result;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
    	this.child = child;
    	this.tid = t;
    	this.tableId = tableid;
    	this.result = null;
    	DbFile dbFile = Database.getCatalog().getDbFile(tableid);
    	if (!child.getTupleDesc().equals(dbFile.getTupleDesc())) throw new DbException("TupleDesc do not match");
    }

    public TupleDesc getTupleDesc() {
        return TUPLE_DESC;
    }

    public void open() throws DbException, TransactionAbortedException {
    	child.open();
    }

    public void close() {
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
    	if (result != null) return null;
    	BufferPool bufferPool = Database.getBufferPool();
    	int total = 0;
    	try {
    		while (child.hasNext()) {
    			bufferPool.insertTuple(tid, tableId, child.next());
    			total++;
    		}
    	} catch (Exception e) {
    		throw new DbException("Exception caught.");
    	}
        result = new Tuple(TUPLE_DESC);
        result.setField(0, new IntField(total));
        return result;
    }
}
