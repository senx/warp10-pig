package io.warp10.pig;

import java.util.HashMap;
import java.util.Map;

import io.warp10.warp.sdk.WarpScriptExtension;

public class PigWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions;
  
  static {
    functions = new HashMap<String,Object>();
    
    functions.put("->PIG", new TOPIG("->PIG"));
    functions.put("PIG->", new PIGTO("PIG->"));
    functions.put("NEWBAG", new NEWBAG("NEWBAG"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
