package io.warp10.pig;

import io.warp10.pig.utils.PigUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class PIGTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public PIGTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    stack.push(PigUtils.fromPig(top));
    
    return stack;
  }  
}
