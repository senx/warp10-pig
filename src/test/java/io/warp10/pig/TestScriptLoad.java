package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptStack;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class TestScriptLoad {

  @Test
  public void testScriptLoad() throws IOException {

    String scriptMc2 = "@/mnt/czd/workspace/lepton/src/test/pig/warpscript/test.mc2";

    WarpScriptRun warpscriptRun = new WarpScriptRun(null);

    Tuple tuple = TupleFactory.getInstance().newTuple(2);

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    tuple.set(0, scriptMc2);
    tuple.set(1, 5.0);

    warpscriptRun.exec(tuple);

    warpscriptRun.exec(tuple);

  }

}
