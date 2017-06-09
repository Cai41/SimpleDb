package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends AbstractDbIterator {
	private final Predicate predicate;
	private final DbIterator dbIterator;

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
    	this.predicate = p;
    	this.dbIterator = child;
    }

    public TupleDesc getTupleDesc() {
        return dbIterator.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
    	dbIterator.open();
    }

    public void close() {
    	super.close();
    	dbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	dbIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple readNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
    	while (dbIterator.hasNext()) {
    		Tuple tuple = dbIterator.next();
    		if (predicate.filter(tuple)) return tuple;
    	}
    	return null;
    }
}
