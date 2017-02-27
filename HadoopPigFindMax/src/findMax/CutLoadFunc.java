

package findMax;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import
org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
/**
* @author Preetu Singh ID: 18910
*/
//vv CutLoadFunc
public class CutLoadFunc extends LoadFunc {
private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);
private final List<Range> ranges;
private final TupleFactory tupleFactory = TupleFactory.getInstance();
private RecordReader reader;
//The string passed to CutLoadFunc is the column specification;
//- Each comma-separated range defines a field, which is
//assigned a name and type in the AS clause.
public CutLoadFunc(String cutPattern) {
//CutLoadFunc is constructed with a string that specifies
//the column ranges to use for each field.
//- The logic for parsing this string and creating a
//list of internal Range objects that encapsulates
//these ranges is contained in the Range class
ranges = Range.parse(cutPattern);
}
@Override
//Pig calls setLocation() on a LoadFunc to pass the input location
//to the loader.
//- Since CutLoadFunc uses a TextInputFormat to break the input
//into lines, we just pass the location to set the input
//path using a static method on FileInputFormat.
public void setLocation(String location, Job job)
throws IOException {
FileInputFormat.setInputPaths(job, location);
}
@Override
//Pig calls the getInputFormat() method to create a
//RecordReader for each split, just like in MapReduce.
//- Pig passes each RecordReader to the prepareToRead()
//method of CutLoadFunc, which we store a reference to,
//so we can use it in the getNext() method for iterating
//through the records.
public InputFormat getInputFormat() {
return new TextInputFormat();
}
@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
	this.reader = reader;
	}
@Override
//- The Pig runtime calls getNext() repeatedly, and the load
//function reads tuples from the reader until the reader
//reaches the last record in its split.
//- At this point, it returns null to signal that there are
//no more tuples to be read.
public Tuple getNext() throws IOException {
try {
if (!reader.nextKeyValue()) {
return null;
}
Text value = (Text) reader.getCurrentValue();
String line = value.toString();
//- It is the responsibility of the getNext() implementation
//to turn lines of the input file into Tuple objects.
//It does this by means of a TupleFactory, a Pig class for
//creating Tuple instances.
//- The newTuple() method creates a new tuple with the
//required number of fields, which is just the number of
//Range classes, and the fields are populated using
//substrings of the line, which are determined by
//the Range objects.
Tuple tuple = tupleFactory.newTuple(ranges.size());
for (int i = 0; i < ranges.size(); i++) {
Range range = ranges.get(i);
if (range.getEnd() > line.length()) {
LOG.warn(String.format(
"Range end (%s) is longer than line length (%s)",
range.getEnd(), line.length()));
continue;
}
//Let's now consider the type of the fields being loaded.
//- If the user has specified a schema, then the fields
//need to be converted to the relevant types.
//- However, this is performed lazily by Pig, and so
//the loader should always construct tuples of type
//bytearrary, using the DataByteArray type.
tuple.set(i, new DataByteArray(range.getSubstring(line)));
}
return tuple;
} catch (InterruptedException e) {
//- We need to think about what to do when the line is
//shorter than the range asked for.
//+ One option is to throw an exception and stop further
//processing.
//o This is appropriate if your application cannot tolerate
//incomplete or corrupt records.
//+ In many cases, it is better to return a tuple with null
//fields and let the Pig script handle the incomplete
//data as it sees fit.
//o This is the approach we take here; by exiting the
//for loop if the range end is past the end of the line,
//we leave the current field and any subsequent fields
//in the tuple with their default value of null.
throw new ExecException(e);
}
}
}
