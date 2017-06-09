package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	private final Map<Field, Integer> map;
	private final Map<Field, Integer> count;
	private final int gbfield;
	private final int afield;
	private final Op op;
	private final TupleDesc tupleDesc;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	map = new HashMap<Field, Integer>();
    	count = new HashMap<Field, Integer>();
    	this.gbfield = gbfield;
    	this.afield = afield;
    	this.op = what;
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
    	int valueOfAggregateField = ((IntField)tup.getField(afield)).getValue();
    	if (map.containsKey(fieldByGroup)) { 
    		count.put(fieldByGroup, count.get(fieldByGroup) + 1);
    	    int aggregateValue = map.get(fieldByGroup);
    	    switch (op) {
			case MIN:
				map.put(fieldByGroup, Math.min(aggregateValue, valueOfAggregateField));
				break;
			case MAX:
				map.put(fieldByGroup, Math.max(aggregateValue, valueOfAggregateField));
				break;
			case SUM:
			case AVG:
				map.put(fieldByGroup, aggregateValue + valueOfAggregateField);
				break;
			default:
				break;
			}
    	} else {
    		map.put(fieldByGroup, valueOfAggregateField);
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
    	private Iterator<Map.Entry<Field, Integer>> iterator;

		@Override
		public void open() throws DbException, TransactionAbortedException {
			iterator = map.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return iterator != null && iterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (iterator == null || !iterator.hasNext()) throw new NoSuchElementException("No more elements.");
			Map.Entry<Field, Integer> entry = iterator.next();
			int aggregateFieldOrder = gbfield == NO_GROUPING ? 0 : 1;
			Tuple tuple = new Tuple(tupleDesc);
			switch (op) {
			case MIN:
			case MAX:
			case SUM:
				tuple.setField(aggregateFieldOrder, new IntField(entry.getValue()));
				break;
			case AVG:
				tuple.setField(aggregateFieldOrder, new IntField(entry.getValue() / count.get(entry.getKey())));
				break;	
			case COUNT:
				tuple.setField(aggregateFieldOrder, new IntField(count.get(entry.getKey())));
				break;
			default:
				break;
			}
			if (gbfield != NO_GROUPING) {
				tuple.setField(0, entry.getKey());
			}
			return tuple;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			iterator = map.entrySet().iterator();
		}

		@Override
		public TupleDesc getTupleDesc() {
			return tupleDesc;
		}

		@Override
		public void close() {
			iterator = null;
		}
    }
}
