package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;
    
    private final Map<Integer, IntHistogram> intHistograms;
    private final Map<Integer, StringHistogram> stringHistograms;
    private final int ioCostPerPage;
    private final HeapFile table;
    private int numTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	this.ioCostPerPage = ioCostPerPage;
    	intHistograms = new HashMap<Integer, IntHistogram>();
    	stringHistograms = new HashMap<Integer, StringHistogram>();
    	table = (HeapFile)Database.getCatalog().getDbFile(tableid);
    	TupleDesc td = table.getTupleDesc();
    	numTuples = 0;
    	
    	int numFields = td.numFields();
    	Set<Integer> intTypes = new HashSet<Integer>();
    	for (int i = 0; i < numFields; i++) if (td.getType(i).equals(Type.INT_TYPE)) intTypes.add(i);
    	
    	Map<Integer, Integer> mins = new HashMap<Integer, Integer>(), maxs = new HashMap<Integer, Integer>();
    	Transaction t = new Transaction(); 
    	t.start();
    	SeqScan s = new SeqScan(t.getId(), tableid, "t"); 
    	try {
			s.open();
	    	while (s.hasNext()) {
	    		numTuples++;
	    		Tuple tuple = s.next();
	    		for (int i : intTypes) {			
	    			int val = ((IntField) tuple.getField(i)).getValue();
	    			if (!mins.containsKey(i)) mins.put(i, val);
	    			else if (mins.get(i) > val) mins.put(i, val);
	    			if (!maxs.containsKey(i)) maxs.put(i, val);
	    			else if (maxs.get(i) < val) maxs.put(i, val);
	    		}
	    	}
	    	for (int i = 0; i < numFields; i++) {
	    		if (intTypes.contains(i)) {
	    			intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, mins.get(i), maxs.get(i)));
	    		} else {
	    			stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
	    		}
	    	}
	    	s.rewind();
	    	while (s.hasNext()) {
	    		Tuple tuple = s.next();
	    		for (int i = 0; i < numFields; i++) {			
	    			Field f = tuple.getField(i);
	    			if (intTypes.contains(i)) intHistograms.get(i).addValue(((IntField)f).getValue());
	    			else stringHistograms.get(i).addValue(((StringField)f).getValue());
	    		}
	    	}
	    	s.close();
	    	t.commit();
		} catch (DbException e1) {
			throw new RuntimeException(e1);
		} catch (TransactionAbortedException e2) {
			throw new RuntimeException(e2);
		} catch (IOException e3) {
			throw new RuntimeException(e3);
		}
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
        return table.numPages() * ioCostPerPage;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(numTuples * selectivityFactor);
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	if (intHistograms.containsKey(field)) {
    		return intHistograms.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
    	} else {
    		return stringHistograms.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
    	}
    }

}
