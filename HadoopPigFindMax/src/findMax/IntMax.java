package findMax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
* @author Preetu Singh ID: 18910
*/

public class IntMax extends EvalFunc<Integer> {
@Override
public Integer exec(Tuple input) throws IOException {
if(input == null || input.size() == 0) {
return null;
}
try{
DataBag value = (DataBag) input.get(0);
if(value.size() == 0){
return null;
}
int max = Integer.MIN_VALUE;
for(Iterator<Tuple> it = value.iterator();it.hasNext();){
Tuple t = it.next();
int d = (Integer)t.get(0);
if(d > max)
max = d;
}
return max;
} catch (ExecException e) {
throw new IOException(e);
}
}
@Override
public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
List<FuncSpec> funcList = new ArrayList<FuncSpec>();
funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new
Schema.FieldSchema(null,DataType.BAG))));
return funcList;
}
}



