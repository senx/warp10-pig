package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.SipHashInline;
import io.warp10.pig.utils.LeptonUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * UDF to run Warpscript onto an WarpscriptStack
 */
public class WarpscriptRun extends EvalFunc<DataBag> {

  //
  // key : Sip64 Hash based on script name or Warpscript commands
  // value : Warpscript macro
  //

  protected java.util.Map<Long, WarpScriptStack.Macro> macros = new HashMap<>();

  //
  // Hash key to compute Sip64 hash on MC2 script
  //

  protected static long[] SIPKEY_SCRIPT =  new long[] {0xF117F9642AF54BAEL, 0x80D1E8A854D22E42L};

  //
  // Hash key to compute Sip64 hash on data
  //

  protected static long[] SIPKEY_UUID =  new long[] {0xF102F5622CF54CAEL, 0x1217A4C4BC129A21L};

  public WarpscriptRun() {  }

  /**
   * Exec a Warpscript file or Warpscript commands onto a Stack with a Bag of data (GTS)
   * Warpscript file (XXX.mc2) should start with @
   *
   * @param input A tuple with 2 elements ('@mc2' or 'NOW..', Bag:{})
   * @return DataBag {(uuid,level,{object})}
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    boolean hasProgress = null != this.getReporter();

    if (input.size() != 2) {
      throw new IOException("Invalid input, expecting a tuple with 2 elements ('@mc2 or 'Warpscript commands',data:{})");
    }

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    //
    // Get first field (script or Warpscript commands)
    //

    String mc2 = (String) input.get(0);

    //
    // data
    //

    Object data =  input.get(1);

    byte pigDataType = DataType.findType(data);


    //
    // Macro created with Warpscript commands
    // Commands get from String or after script parsing
    //

    WarpScriptStack.Macro macroMc2 = null;

    //
    // Bag that represents the stack after the Warpscript exec
    //

    DataBag stackAsBag = null;


    //
    // Hash keys against data and script to be idempotent and determinist
    // Thus we can store intermediate results
    // With the macro hash key we can use a cache of Macros
    //
    long hashData = Long.MIN_VALUE;
    long hashMacro = Long.MIN_VALUE;


    //
    // data must be a Bag (other types not yet supported)
    //

    if (pigDataType != DataType.BAG) {
      throw new IOException("data must be a Bag of GTS (encoded)");
    }

    try {

      //
      // Script or Warpscript commands ?
      //

      if (mc2.startsWith("@")){

        //
        // delete the @ character
        //

        String filePath = mc2.substring(1);
        byte[] keyHash = filePath.getBytes(StandardCharsets.UTF_8);

        //
        // Compute the hash against filepath
        //

        hashMacro = SipHashInline.hash24(SIPKEY_SCRIPT[0], SIPKEY_SCRIPT[1], keyHash, 0, keyHash.length);

        macroMc2 = macros.get(hashMacro);

        if (null == macroMc2) {
          macroMc2 = LeptonUtils.parseScript(stack, filePath);
          macros.put(hashMacro, macroMc2);
        }

      } else { // String with Warpscript commands

        byte[] keyHash = mc2.getBytes(StandardCharsets.UTF_8);

        //
        // Compute the hash against String content
        //

        hashMacro = SipHashInline.hash24(SIPKEY_SCRIPT[0], SIPKEY_SCRIPT[1], keyHash, 0, keyHash.length);

        macroMc2 = macros.get(hashMacro);

        if (null == macroMc2) {
          macroMc2 = LeptonUtils.parseString(stack, mc2);
          macros.put(hashMacro, macroMc2);
        }

      }

      //
      // Second field : data (Bag)
      //

      DataBag bag = (DataBag) data;

      //
      // Iterate on the list of tuples
      // FIXME : limit size 1 !!
      //

      if (bag.size() > 1) {
        throw new IOException("Bag must contain only one GTSWrapper");
      }

      Iterator<Tuple> iter = bag.iterator();

      //
      // FIXME : only one GTSWraper has been accepted as input of the stack - how to compute hash on a list of data objects ?
      //

      while (iter.hasNext()) {

        Tuple t = iter.next();

        DataByteArray encoded = (DataByteArray)t.get(0);

        if (DataType.findType(encoded) == DataType.BYTEARRAY) {

          GTSWrapper gtsWrapper = LeptonUtils.encodedToGTSWrapper(encoded.get());
          hashData = computeHash(gtsWrapper);

          GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);

          int nvalues = GTSHelper.nvalues(gts);

          if (0 != nvalues) {
            stack.push(gts);
          } else{
            System.err.println("WARN - WarpscriptRun: GTS is empty. Ignore it !");
          }

        } else{
          throw new IOException(DataType.findType(encoded) + " cannot yet be put onto the stack with Pig");
        }

        if (hasProgress) {
          this.progress();
        }

      }

 /*     if (DataType.isAtomic(pigDataType)) {

      //
      // bytearray (cast to GeotimeSerie), chararray, integer, long, float, or boolean.
      //

        stack.push(PigToWarpscript.atomicToWarpscript(data));

      } else {  // Complex : bag, tuple, or map.

        switch (pigDataType) {

          case DataType.TUPLE:
          case DataType.MAP:

            stack.push(PigToWarpscript.complexToWarpscript(data));

            break;

          case DataType.BAG:

            DataBag bag = (DataBag) data;

            //
            // Iterate on the list of tuples
            // FIXME : limit size ?
            //

            Iterator<Tuple> iter = bag.iterator();

            //
            // Cast each tuple to Warpscript and push it onto the stack
            //

            while (iter.hasNext()) {

              Tuple t = iter.next();

              stack.push(PigToWarpscript.pigTupleToWarpscript(t));

              if (hasProgress) {
                this.progress();
              }

            }

            break;

          default:

            throw new IOException(DataType.findTypeName(pigDataType)
                + " cannot be cast to Warpscript yet..");

        }
      }
 */

      //
      // Do not execute this script is no GTS has been pushed ontot the stack
      //
      if (stack.depth() == 0) {
        System.err.println("WARN - WarpscriptRun: no GTS has been pushed onto the stack - But we still execute the script...");
      }

      stack.exec(macroMc2);

      //
      // Dump stack to Bag
      //

      stackAsBag = LeptonUtils.stackToPig(stack, hashMacro, hashData);

    } catch (WarpScriptException ee) {
      throw new IOException(ee);
    }

    return stackAsBag;

  }

  /**
   *
   * @param input
   * @return Schema - stack: {(scriptId: long,inputId: long,level: int,value: {()})}
   */
  @Override
  public Schema outputSchema(Schema input) {

    Schema bagSchema = new Schema(LeptonUtils.stackLevelSchema());

    Schema.FieldSchema outputBag = new Schema.FieldSchema("stack", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);
  }

  /**
   * Compute Hash onto GTSWrapper
   * @param gts - GTSWrapper
   * @return long - Hash key
   */
  protected long computeHash(GTSWrapper gts) throws WarpScriptException {
    return SipHashInline.hash24(SIPKEY_UUID[0], SIPKEY_UUID[1], gts.getEncoded(), 0, gts.getEncoded().length);
  }

}
