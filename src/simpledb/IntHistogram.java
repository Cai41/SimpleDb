package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	private final int[] histogram;
	private final double min;
	private final double max;
	private final double bucketWidth;
	private int ntuples;
	private final double max1;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	this.histogram = new int[buckets + 2];
    	this.bucketWidth = Math.ceil(1.0 * (max - min + 1) / buckets);
    	this.max1 = min + bucketWidth * buckets;
    	ntuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int bucketNo = (int)((v - min + bucketWidth) / bucketWidth);
    	if (bucketNo == histogram.length - 1) bucketNo--;
    	histogram[bucketNo]++;
    	ntuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	double v1 = v;
    	if (v1 < min) v1 = min - bucketWidth / 2.0;
    	else if (v1 > max) v1 = max1 + bucketWidth / 2.0;
    	int bucketNo = (int)((v1 - min + bucketWidth) / bucketWidth);
    	if (v1 == max && bucketNo == histogram.length - 1) bucketNo--;
    	double fracEqualToVal = (histogram[bucketNo] / bucketWidth) / ntuples;
    	if (op.equals(Predicate.Op.EQUALS)) {
    		return fracEqualToVal;
    	} else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		double bucketPart = histogram[bucketNo] == 0.0 ? 0 : ((bucketNo + 1) * bucketWidth - v1) / histogram[bucketNo];
    		double bucketFraction = histogram[bucketNo] * 1.0 / ntuples;
    		double f = 0.0;
    		for (int i = bucketNo + 1; i < histogram.length; i++) f += histogram[i];
    		f /= ntuples;
    		f += (bucketPart * bucketFraction);
    		if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) f += fracEqualToVal;
    		return f;
    	} else if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
    		double bucketPart = histogram[bucketNo] == 0 ? 0 : (v1 - bucketNo * bucketWidth) / histogram[bucketNo];
    		double bucketFraction = histogram[bucketNo] * 1.0 / ntuples;
    		double f = 0.0;
    		for (int i = 0; i < bucketNo; i++) f += histogram[i];
    		f /= ntuples;
    		f += bucketPart * bucketFraction;
    		if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) f += fracEqualToVal;
    		return f;
    	} else {
    		return 1 - (histogram[bucketNo] / bucketWidth) / ntuples;
    	}
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format("min = %d, max = %d, # bucket = ", min, max, histogram.length);
    }
}
