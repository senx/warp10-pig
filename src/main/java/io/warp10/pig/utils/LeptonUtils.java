package io.warp10.pig.utils;

import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSSplitter;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import com.google.common.base.Charsets;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LeptonUtils {

  //
  // Chunk label name
  //
  public static final String chunkIdLabelName = "chunkId";

  protected LeptonUtils() { }

  /**
   * Parse Warpscript file and create a Macro
   * @param stack current Warpscript stack
   * @param warpscriptName name of the script to parse
   * @return Macro
   */
  public static WarpScriptStack.Macro parseScript(WarpScriptStack stack, String warpscriptName)
      throws IOException, WarpScriptException {

    //
    // Load the Warpscript file
    //

    InputStream fis = new FileInputStream(warpscriptName);
    BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

    StringBuffer scriptSB = new StringBuffer();

    stack.exec(WarpScriptStack.MACRO_START);

    while(true) {

      String line = br.readLine();

      if (null == line) {
        break;
      }

      scriptSB.append(line).append("\n");

      stack.exec(line);

    }

    fis.close();

    stack.exec(WarpScriptStack.MACRO_END);

    WarpScriptStack.Macro macro = (WarpScriptStack.Macro) stack.pop();

    return macro;

  }

  /**
   * Convert String with Warpscript Commands to Macro
   * @param stack current Warpscript stack
   * @param mc2Commands String with Warpscript commands
   * @return Macro
   */
  public static WarpScriptStack.Macro parseString(WarpScriptStack stack, String mc2Commands) throws WarpScriptException {

    stack.exec(WarpScriptStack.MACRO_START);

    stack.exec(mc2Commands);

    stack.exec(WarpScriptStack.MACRO_END);

    WarpScriptStack.Macro macro = (WarpScriptStack.Macro) stack.pop();

    return macro;

  }

  /**
   * Return a Tuple that represents a current stack level
   *
   * @param hashMacro
   * @param hashData
   * @param level
   * @param obj
   * @return Tuple - (scriptId: long,inputId: long,level: int,value: {()})
   *
   */
  public static Tuple stackLevelToPig(long hashMacro,long hashData, int level,Object obj) throws IOException {

    //
    // Cast object to Pig type
    //

    Object pigObj = WarpscriptToPig.cast(obj);

    //
    // The tuple that contains the current level of the stack
    // (uuid,level,{(object)})
    //

    Tuple tupleElt = TupleFactory.getInstance().newTuple(4);

    //
    // Bag and tuple that contains the object at the current level of the stack
    //
    Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
    DataBag innerBag = new DefaultDataBag();

    //
    // uuids
    //

    tupleElt.set(0, hashMacro);

    tupleElt.set(1, hashData);

    //
    // level
    //

    tupleElt.set(2, level);

    //
    // object
    //

    innerTuple.set(0,pigObj);
    innerBag.add(innerTuple);

    tupleElt.set(3, innerBag);

    return tupleElt;

  }

  /**
   * Return a Bag of tuples that represents the stack
   *
   * @param hashMacro - hash of MC2 script
   * @param hashData - hash of data
   * @param stack
   * @return DataBag (SortedDataBag) - stack: {(uuid: chararray,level: int,object: {})}
   *
   */
  public static DataBag stackToPig(WarpScriptStack stack, long hashMacro, long hashData) throws IOException {

    //
    // Try to use the DefaultComparator
    //

    DataBag stackAsBag = new SortedDataBag(null);

    int level = 0;
    int size = stack.depth();

    while (level < size) {

      Object obj = stack.pop();

      stackAsBag.add(stackLevelToPig(hashMacro,hashData, level,obj));

      level++;

    }

    return stackAsBag;

  }

  /**
   *
   * @param encoded
   * @return GTSWrapper
   */
  public static GTSWrapper encodedToGTSWrapper(byte[] encoded)
      throws IOException {

    GTSWrapper gtsWrapper = new GTSWrapper();

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    try {
      deserializer.deserialize(gtsWrapper, encoded);
    } catch (TException te) {
      throw new IOException(te);
    }

    Metadata metadata = null;
    if (null != gtsWrapper.getMetadata()) {
      metadata = new Metadata(gtsWrapper.getMetadata());
    } else {
      metadata = new Metadata();
    }

    gtsWrapper.setMetadata(metadata);

    return gtsWrapper;
  }

  /**
   * Return the Pig schema that represents one level of the stack (duplicate code is the evil !
   * @return Schema (scriptId: long,inputId: long,level: int,value: {()})
   */
  public static Schema stackLevelSchema() {
    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("scriptId", DataType.LONG));
    fields.add(new Schema.FieldSchema("inputId", DataType.LONG));
    fields.add(new Schema.FieldSchema("level", DataType.INTEGER));
    fields.add(new Schema.FieldSchema("value", DataType.BAG));

    Schema tupleSchema = new Schema(fields);

    Schema.FieldSchema outputTuple = new Schema.FieldSchema("stackLevel", DataType.TUPLE);

    outputTuple.schema = tupleSchema;

    return new Schema(outputTuple);
  }


  /**
   * chunk a GTSWrapper instance
   *
   * @param gtsWrapper
   * @return a list of chunks (GTSWrapper instances)
   * @throws java.io.IOException
   */
  public static List<GTSWrapper> chunk(GTSWrapper gtsWrapper, long clipFrom, long clipTo, long chunkwidth) throws IOException {

    //
    // Chunk this wrapper
    //

    GTSDecoder decoder = new GTSDecoder(gtsWrapper.getBase(), ByteBuffer
        .wrap(gtsWrapper.getEncoded()));

    long tsboundary = 0L;
    String fileprefix = "lepton";
    long maxsize = Long.MAX_VALUE;
    long maxgts = Long.MAX_VALUE;
    float lwmratio = 0.0F;

    List<GTSWrapper> gtsWrappers = new ArrayList<GTSWrapper>();

    try {

      List<GTSWrapper> chunkWrappers = GTSSplitter
          .chunk(decoder, clipFrom, clipTo, tsboundary,
              chunkwidth, chunkIdLabelName, fileprefix, maxsize, maxgts,
              lwmratio);

      return chunkWrappers;

    } catch(WarpScriptException ee) {
      throw new IOException(ee);
    }
  }

}
