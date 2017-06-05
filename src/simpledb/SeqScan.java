package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	private final DbFile dbFile;
	private final DbFileIterator iterator;
	private final TupleDesc tupleDesc;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid The transaction this scan is running as a part of.
     * @param tableid the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.dbFile = Database.getCatalog().getDbFile(tableid);
        this.iterator = dbFile.iterator(tid);
        
        TupleDesc td = dbFile.getTupleDesc();
        int numFields = td.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        for (int i = 0; i < numFields; i++) {
        	typeAr[i] = td.getType(i);
        	fieldAr[i] = tableAlias + "." + td.getFieldName(i);
        }
        tupleDesc = new TupleDesc(typeAr, fieldAr);
    }

    public void open()
        throws DbException, TransactionAbortedException {
    	iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
    	return iterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        return iterator.next();
    }

    public void close() {
    	iterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        iterator.rewind();
    }
}
