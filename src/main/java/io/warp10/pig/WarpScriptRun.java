package io.warp10.pig;

import io.warp10.continuum.Configuration;
import io.warp10.crypto.SipHashInline;
import io.warp10.pig.utils.PigUtils;
import io.warp10.pig.utils.WarpScriptUtils;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStopException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;

/**
 * UDF to run Warpscript onto an WarpscriptStack
 */
public class WarpScriptRun extends EvalFunc<Tuple> {

  // FIXME declare it as public in WarpScriptExecutor
  private static final String WARP10_CONFIG = "warp10.config";

  //
  // variable to register Warpscript filename
  //
  public static final String WARPSCRIPT_FILE_VARIABLE = "warpscript.file";

  //
  // variable to register hash computed onto Warpscript commands
  //
  public static final String WARPSCRIPT_ID_VARIABLE = "warpscript.id";

  //
  // Hash key to compute Sip64 hash on MC2 script
  //

  protected static long[] SIPKEY_SCRIPT =  new long[] {0xF117F9642AF54BAEL, 0x80D1E8A854D22E42L};

  //
  // Hash key to compute Sip64 hash on data
  //

  protected static long[] SIPKEY_UUID =  new long[] {0xF102F5622CF54CAEL, 0x1217A4C4BC129A21L};


  //
  // Default timeunits
  // ns means nanoseconds
  // us means microseconds
  // ms means milliseconds
  //

  public static final String DEFAULT_TIME_UNITS_PER_MS = "us";

  //
  // WarpScriptExecutor
  //

  protected WarpScriptExecutor executor = null;

  public WarpScriptRun() {
    System.setProperty(Configuration.WARP_TIME_UNITS, DEFAULT_TIME_UNITS_PER_MS);
    System.setProperty(Configuration.WARPSCRIPT_MAX_GTS, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_OPS, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_SYMBOLS, String.valueOf(Integer.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_BUCKETS, String.valueOf(Integer.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_DEPTH, String.valueOf(Integer.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_FETCH, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_LOOP_DURATION, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_PIXELS, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_WEBCALLS, String.valueOf(Long.MAX_VALUE));
    System.setProperty(Configuration.WARPSCRIPT_MAX_RECURSION, String.valueOf(Integer.MAX_VALUE));
    System.setProperty(WarpScriptStack.ATTRIBUTE_HADOOP_PROGRESSABLE, String.valueOf(Long.MAX_VALUE));
  }

  /**
   * Exec a Warpscript file or Warpscript commands onto a Stack with input data
   * Data are pushed onto the top of the stack according to their level.
   * Warpscript file (XXX.mc2) should start with '@': '@file.mc2'
   * To use a macro we put a space before the '@': ' @macro'
   *
   * @param input A tuple with N elements ('@mc2' or 'NOW..', obj1, obj2, ...)
   * @return DataBag {(uuid,level,object)}
   * @throws java.io.IOException
   */
  @Override
  public Tuple exec(Tuple input) throws IOException {

    boolean hasProgress = null != this.getReporter();

    //
    // Bag that represents the stack after the Warpscript exec
    //

    Tuple stackOut = null;

    //
    // The stack after this run
    //

    List<Object> stackResult = null;

    if (input.size() < 1) {
      throw new IOException("Invalid input, expecting a tuple with at least one element '@mc2 or 'Warpscript commands' and (optional) data: ('@mc2 or 'Warpscript commands': chararray, object: any, ...)");
    }

    //
    // Get first field (script or Warpscript commands)
    //

    String mc2 = (String) input.get(0);

    try {

      //
      // data (input)
      //

      DataBag data = null;

      //
      // List that represents the Warpscript stack (with sorted elements)
      //

      List<Object> stackInput = new ArrayList<>();

      if (input.size() > 1) {

        //
        // Push input data (Object) onto the stack
        // We have to consider the level of each element (Tuple index)
        // We push all of these data with 1 Mark at first (Then it's quite easy to create a list with WarpScript)
        // Reverse list to be more compliant with a stack representation
        //

        for (int level=1; level<input.size(); level++) {
          reporter.progress();
          
          stackInput.add(PigUtils.fromPig(input.get(level)));
        }
      }

      //
      // Script or Warpscript commands ?
      //

      if (mc2.startsWith("@")) {

        //
        // delete the @ character
        //

        String filePath = mc2.substring(1);
        String mc2FileContent = "'" + filePath + "' '" + WARPSCRIPT_FILE_VARIABLE + "' STORE " + WarpScriptUtils.parseScript(filePath);

        executor = new WarpScriptExecutor(WarpScriptExecutor.StackSemantics.NEW, mc2FileContent, null, PigStatusReporter.getInstance());
      } else {

        //
        // String with Warpscript commands
        //

        //
        // Compute the hash against String content to identify this run
        //

        byte[] keyHash = mc2.getBytes(StandardCharsets.UTF_8);
        Long hashMacro = SipHashInline.hash24(SIPKEY_SCRIPT[0], SIPKEY_SCRIPT[1], keyHash, 0, keyHash.length);

        String mc2Content = "'" + String.valueOf(hashMacro) + "' '" + WARPSCRIPT_ID_VARIABLE + "' STORE " + mc2;

        executor = new WarpScriptExecutor(WarpScriptExecutor.StackSemantics.NEW, mc2Content, null, PigStatusReporter.getInstance());
      }

      stackResult = executor.exec(stackInput);

      //
      // Dump stack to Tuple
      //

      stackOut = WarpScriptUtils.stackToPig(stackResult);

    } catch (WarpScriptStopException wse) {
      //
      // this is not an error
      //
      if (null != stackResult) {
        stackOut = WarpScriptUtils.stackToPig(stackResult);
      }
    } catch(Exception ee) {
      throw new IOException(ee);
    }

    return stackOut;

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("stack", DataType.TUPLE);
    return new Schema(fieldSchema);
  }

}
