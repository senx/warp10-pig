package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Properties;

/**
 * Use Warpscript to generate an UUID
 */
public class UUID extends EvalFunc<String> {

  public UUID() { }

  /**
   * Execute a Warpscript
   *
   * @param input
   * @return Tuple
   * @throws java.io.IOException
   */
  @Override
  public String exec(Tuple input) throws IOException {

    reporter.progress();

    //
    // No parameter is attempted
    //

    String uuid = null;

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    try {

      stack.exec("UUID ");

      uuid = (String) stack.pop();

    } catch (WarpScriptException e) {
      throw new IOException(e);
    }

    return uuid;
  }

  @Override
  public Schema outputSchema(Schema input) {

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("uuid", DataType.CHARARRAY);

    return new Schema(fieldSchema);

  }

}