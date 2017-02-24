package HiveMaxNAvg;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
/**
* @author Preetu Singh ID: 18910
*
*/
public class Mean extends UDAF {
 public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
 public static class PartialResult {
 double sum;
 long count;
 }
 private PartialResult partial;
 public void init() {
 partial = null;
 }
////////////////////////////////////////////////////////////////////
//iterate() is similar to Map
//- Find the partial maximum
////////////////////////////////////////////////////////////////////
 public boolean iterate(DoubleWritable value) {
     if (value == null) {
       return true;
     }
     if (partial == null) {
       partial = new PartialResult();
     }
     partial.sum += value.get();
     partial.count++;
     return true;
   }

 public PartialResult terminatePartial() {
 return partial;
 }
////////////////////////////////////////////////////////////////////
//merge() is similar to Reduce
//- merge() method is the same as iterate() in this example.
////////////////////////////////////////////////////////////////////
 public boolean merge(PartialResult other) {
	
	      if (other == null) {
	        return true;
	      }
	      if (partial == null) {
	        partial = new PartialResult();
	      }
	      partial.sum += other.sum;
	      partial.count += other.count;
	      return true;
	    }

	 public DoubleWritable terminate() {
	 if (partial == null) {
	 return null;
	 }
	 return new DoubleWritable(partial.sum / partial.count);
	 }
	 }
	}
