package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private final Map<Field, Integer> count;
	private final int gbfield;
	private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	if (what != Op.COUNT) throw new IllegalArgumentException("op shoudl be COUNT");
    	count = new HashMap<Field, Integer>();
    	this.gbfield = gbfield;
    	if (gbfield == NO_GROUPING) {
    		tupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
    	} else {
    		tupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
    	Field fieldByGroup = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
    	if (count.containsKey(fieldByGroup)) {
    		count.put(fieldByGroup, count.get(fieldByGroup) + 1);
    	} else {
    		count.put(fieldByGroup, 1);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	return new TupleIterator();
    }
    
    class TupleIterator implements DbIterator {
    	Iterator<Map.Entry<Field, Integer>> iterator;

		@Override
		public void open() throws DbException, TransactionAbortedException {
			iterator = count.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return iterator != null && iterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (iterator == null || !iterator.hasNext()) throw new NoSuchElementException("No more elements");
			Map.Entry<Field, Integer> entry = iterator.next();
			int aggregateFieldOrder = gbfield == NO_GROUPING ? 0 : 1;
			Tuple tuple = new Tuple(tupleDesc);
			tuple.setField(aggregateFieldOrder, new IntField(entry.getValue()));
		    if (gbfield != NO_GROUPING)	tuple.setField(0, entry.getKey());
		    return tuple;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			iterator = count.entrySet().iterator();
		}

		@Override
		public void close() {
			iterator = null;
		}

		@Override
		public TupleDesc getTupleDesc() {
			return tupleDesc;
		}
    }
}
