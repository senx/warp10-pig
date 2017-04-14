package io.warp10.pig;

import java.io.IOException;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import io.warp10.pig.utils.PigUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class ADD extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private io.warp10.script.binary.ADD originalADD = new io.warp10.script.binary.ADD("+");
  
  public ADD(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object elt = stack.pop();
    Object bag = stack.peek();
    
    if (!(bag instanceof DataBag)) {
      stack.push(elt);
      return originalADD.apply(stack);
    }
    
    if (elt instanceof Tuple) {
      ((DataBag) bag).add((Tuple) elt);
    } else {
      try {
        Object pig = PigUtils.toPig(elt);
        if (pig instanceof Tuple) {
          ((DataBag) bag).add((Tuple) pig);
        } else {
          Tuple t = TupleFactory.getInstance().newTuple();
          t.append(pig);
          ((DataBag) bag).add((Tuple) t);
        }        
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " encountered an error while converting element.");
      }
    }
    
    return stack;
  }  
}
