package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
	int mMin;
	int mMax;
	int[] mBuckets;
	double interval;
	int totalEntries;
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	mBuckets = new int[buckets];
    	mMin = min;
    	mMax = max;
    	interval = (mMax - mMin)/(1.0f * buckets);
    	totalEntries = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	//calculating bin number
    	if (v < mMin || v >= mMax)
    		return;
    	int correctIndex = (int)Math.floor((v - mMin) / interval);
    	mBuckets[correctIndex]++;
    	totalEntries++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
    	double validEntries = 0.0;
    	if (v < mMin) {
    		switch(op){
			case GREATER_THAN:
				return 1.0;
			case GREATER_THAN_OR_EQ:
				return 1.0;
			case LESS_THAN:
				return 0.0;
			case LESS_THAN_OR_EQ:
				return 0.0;
			case EQUALS:
				return 0.0;
			case NOT_EQUALS:
				return 1.0;
			default:
				return 0.0f;
    		}
    	}
    	if (v >= mMax) {
    		switch(op){
			case GREATER_THAN:
				return 0.0;
			case GREATER_THAN_OR_EQ:
				return 0.0;
			case LESS_THAN:
				return 1.0;
			case LESS_THAN_OR_EQ:
				return 1.0;
			case EQUALS:
				return 0.0;
			case NOT_EQUALS:
				return 1.0;
			default:
				return 0.0;
    		}
    	}
    	int i = (int)Math.floor((v - mMin) / interval);
		double leftc = mMin + i * interval;
		double rightc = leftc + interval;
		if (v < rightc && v >= leftc){
			switch(op){
			case GREATER_THAN:
				validEntries += (rightc - v)/interval * mBuckets[i];
				for (int j = i + 1 ; j < mBuckets.length; j++){
					validEntries += mBuckets[j];
				}
				return validEntries/totalEntries;
			case GREATER_THAN_OR_EQ:
				validEntries += (rightc - v + 1)/interval * mBuckets[i];
				for (int j = i + 1 ; j < mBuckets.length; j++){
					validEntries += mBuckets[j];
				}
				return validEntries/totalEntries;
			case LESS_THAN:
				validEntries += (v - leftc )/interval * mBuckets[i];
				for (int j = 0 ; j < i; j++){
					validEntries += mBuckets[j];
				}
				return validEntries/totalEntries;
			case LESS_THAN_OR_EQ:
				validEntries += (v - leftc + 1)/interval * mBuckets[i];
				for (int j = 0 ; j < i; j++){
					validEntries += mBuckets[j];
				}
				return validEntries/totalEntries;
			case EQUALS:
				validEntries += 1/interval * mBuckets[i];
				return validEntries/totalEntries;
			case NOT_EQUALS:
				validEntries = totalEntries - ( 1/interval * mBuckets[i]);
				return validEntries/totalEntries;
			default:
				return 0.0;
			}
		}
		return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
    	double sum = 0.0;
		System.out.println("selectivity calculations");
		for (int c = mMin; c < mMax; c++) {
			sum += estimateSelectivity(Op.EQUALS, c);
		}
        return sum/(mMax-mMin);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	String returnStr = "";
    	for (int i = 0; i < mBuckets.length;i++) {
    		returnStr += "|" + (mMin + (interval * i)) + "| n=" + mBuckets[i] + " ";    		 		
    	}
    	returnStr += "|" + (mMax) + "|";
        return returnStr;
    }
}
