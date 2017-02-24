package HiveMaxNAvg;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;
/**
* @author Preetu Singh ID: 18910
*/
public class Maximum extends UDAF {
 public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
 private IntWritable result;
 public void init() {
 result = null;
 }
////////////////////////////////////////////////////////////////////
//iterate() is similar to Map
//- Find the partial maximum
////////////////////////////////////////////////////////////////////
 public boolean iterate(IntWritable value) {
     if (value == null) {
        return true;
     }
     if (result == null) {
        result = new IntWritable(value.get());
     } else {
        result.set(Math.max(result.get(), value.get()));
     }
     return true;
  }
  public IntWritable terminatePartial() {
     return result;
  }


////////////////////////////////////////////////////////////////////
//merge() is similar to Reduce
//- merge() method is the same as iterate() in this example.
////////////////////////////////////////////////////////////////////
 public boolean merge(IntWritable other) {
 return iterate(other);
 }
 public IntWritable terminate() {
 return result;
 }
 }
}

