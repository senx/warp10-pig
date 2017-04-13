package io.warp10.pig;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class NEWBAG extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public NEWBAG(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();

    stack.push(bag);
    
    return stack;
  }  
}
