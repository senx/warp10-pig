package io.warp10.pig;

import java.io.IOException;

import io.warp10.pig.utils.PigUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class TOPIG extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public TOPIG(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    try {
      stack.push(PigUtils.toPig(top));
    } catch (IOException ioe) {
      throw new WarpScriptException(getName() + " caught exception while attempting to convert object.", ioe);
    }
    
    return stack;
  }  
}
