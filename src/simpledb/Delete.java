package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	private static final TupleDesc TUPLE_DESC = new TupleDesc(new Type[] {Type.INT_TYPE});
	
	private final TransactionId tid;
	private final DbIterator child;
	private Tuple result;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	this.tid = t;
    	this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (result != null) return null;
        BufferPool bufferPool = Database.getBufferPool();
        int total = 0;
        try {
        	while (child.hasNext()) {
        		bufferPool.deleteTuple(tid, child.next());
        		total++;
        	}
        } catch (Exception e) {
        	throw new DbException("Exception caught");
        }
        result = new Tuple(TUPLE_DESC);
        result.setField(0, new IntField(total));
        return result;
    }
}
