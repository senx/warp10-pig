package io.warp10.pig;

import io.warp10.continuum.Configuration;
import io.warp10.pig.utils.StackElementComparator;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptStack;
import io.warp10.crypto.SipHashInline;
import io.warp10.pig.utils.WarpScriptUtils;
import io.warp10.script.WarpScriptStopException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * UDF to run Warpscript onto an WarpscriptStack
 */
public class WarpScriptRun extends EvalFunc<DataBag> {

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
   * @param input A tuple with 2 elements ('@mc2' or 'NOW..', { (level, element) })
   * @return DataBag {(uuid,level,object)}
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    boolean hasProgress = null != this.getReporter();

    //
    // Bag that represents the stack after the Warpscript exec
    //

    DataBag stackOut = null;

    //
    // The stack after this run
    //

    List<Object> stackResult = null;

    if (input.size() < 1) {
      throw new IOException("Invalid input, expecting a tuple with at least one element ('@mc2 or 'Warpscript commands': chararray) and (optional) data: {(level: int, element: any)}");
    }

    //
    // Get first field (script or Warpscript commands)
    //

    String mc2 = (String) input.get(0);

    try {

      //
      // data (input) and the related sorted Bag
      //

      DataBag data = null;
      DataBag sortedDataBag = null;

      if (2 == input.size()) {

        //
        // data must be into a Bag
        //

        if (DataType.findType(input.get(1)) != DataType.BAG) {
          throw new IOException("data must be a Bag: {(level: int, element: any)}");
        }

        data = (DataBag) input.get(1);

        if (data.isSorted()) {
          //
          // Bag is already sorted
          //
          sortedDataBag = data;
        } else {
          sortedDataBag = new SortedDataBag(new StackElementComparator());
          sortedDataBag.addAll(data);
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

      //
      // Push input data (Object) onto the stack
      // We have to consider the level of each element (Sort)
      // We push all of these data with 1 Mark at first (Then it's quite easy to create a list with WarpScript)
      //

      //
      // List with data we will push onto the stack
      //
      List<Object> stackInput = new ArrayList<>();
      if (null != sortedDataBag) {
        stackInput.add(new WarpScriptStack.Mark());
        Iterator<Tuple> iter = sortedDataBag.iterator();
        while (iter.hasNext()) {
          Tuple tuple = iter.next();
          //
          // We can ignore level
          //
          Object currentElement = tuple.get(1);
//          System.out.println("type: " + currentElement.getClass().getSimpleName());
          if (DataType.BYTEARRAY == DataType.findType(currentElement)) {
            stackInput.add(((DataByteArray) currentElement).get());
          } else {
            stackInput.add(currentElement);
          }
        }
      }

      stackResult = executor.exec(stackInput);

      //
      // Dump stack to Bag
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

  /**
   *
   * @param input
   * @return Schema - stack: {(level: int,element: Object)}
   */
  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema outputBag = new Schema.FieldSchema("stack", DataType.BAG);
    return new Schema(outputBag);
  }

}
