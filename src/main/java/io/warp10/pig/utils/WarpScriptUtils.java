package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.script.WarpScriptException;
import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSSplitter;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import com.google.common.base.Charsets;
import org.apache.pig.data.*;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.*;
import java.util.ArrayList;
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
   * Return a Tuple that represents the stack
   *
   * @param stack (List)
   * @return DataBag - stack: (obj1, obj2, ...)
   *
   */
  public static Tuple stackToPig(List<Object> stack) throws IOException {

    Tuple stackAstuple = TupleFactory.getInstance().newTuple(stack.size());

    //
    // Take object in reverse order to really represents the Stack
    //

    for (int level = 0; level < stack.size(); level++) {
      //
      // Cast object to Pig type
      //

      Object pigObj = WarpscriptToPig.cast(stack.get(level));

      //
      // level
      //

      stackAstuple.set(level, pigObj);

    }

    return stackAstuple;

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
