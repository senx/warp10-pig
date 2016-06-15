package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.script.WarpScriptException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WarpScriptUtils {

  //
  // Chunk label name
  //
  public static final String chunkIdLabelName = "chunkId";

  protected WarpScriptUtils() { }

  /**
   * Parse Warpscript file and return its content as String
   * @param warpscriptName name of the script to parse
   * @return String
   */
  public static String parseScript(String warpscriptName)
      throws IOException, WarpScriptException {

    //
    // Load the Warpscript file
    //

    InputStream fis = WarpScriptUtils.class.getClassLoader().getResourceAsStream(warpscriptName);

    BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

    StringBuffer scriptSB = new StringBuffer();

    while(true) {
      String line = br.readLine();
      if (null == line) {
        break;
      }
      scriptSB.append(line).append("\n");
    }

    fis.close();

    return scriptSB.toString();

  }

  /**
   * Return a Tuple that represents a current stack level
   *
   * @param level
   * @param obj
   * @return Tuple - (level: int,element: Object)
   *
   */
  public static Tuple stackLevelToPig(int level,Object obj) throws IOException {

    //
    // Cast object to Pig type
    //

    Object pigObj = WarpscriptToPig.cast(obj);

    //
    // The tuple that contains the current level of the stack
    // (level,object)
    //

    Tuple tupleElt = TupleFactory.getInstance().newTuple(2);


    //
    // level
    //

    tupleElt.set(0, level);

    //
    // object
    //

    tupleElt.set(1, pigObj);

    return tupleElt;

  }

  /**
   * Return a Bag of tuples that represents the stack
   *
   * @param stack (List)
   * @return DataBag - stack: {(level: int,element: Object)}
   *
   */
  public static DataBag stackToPig(List<Object> stack) throws IOException {

    //
    // Sorted Databag
    //

    DataBag stackAsBag = new SortedDataBag(new StackElementComparator());

    //
    // Invert order to really represents the Stack
    //

    Collections.reverse(stack);

    int level = 0;
    for (Object obj: stack) {
      stackAsBag.add(stackLevelToPig(level,obj));
//    System.out.println("level: " + level);
//    System.out.println("elt class: " + obj.getClass().getSimpleName());
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
   * Return the Pig schema that represents one level of the stack
   * @return Schema (level: int,object: Object)
   */
  public static Schema stackLevelSchema() {
    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("stackLevel", DataType.TUPLE);
    return new Schema(fieldSchema);
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

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

    long tsboundary = 0L;
    String fileprefix = "warp10-pig";
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
