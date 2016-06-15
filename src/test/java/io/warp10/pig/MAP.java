package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.pig.utils.WarpScriptUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Class used to execute Warpscript function Map on GTS
 */
public class MAP extends EvalFunc<DataByteArray> {



  private final TupleFactory tupleFactory = TupleFactory.getInstance();

  //
  // WarpScript (parsed)
  //
  private String warpscript;


  public MAP(String... args) {
    this(args[0]);
  }

  /**
   *
   * @param warpscript Name of the Warpscript used to perform this Reduce
   */
  protected MAP(String warpscript) {
    this.warpscript = warpscript;
  }

  /**
   * Execute a Warpscript
   *
   * @param input
   * @return DataByteArray
   * @throws java.io.IOException
   */
  @Override
  public DataByteArray exec(Tuple input) throws IOException {

    if (input == null || input.size() < 1) {
      System.err.println("Waiting for 1 parameter : 1 tuple with one Bag. This bag contains 1 tuple with 4 fields : labels, gtsId, chunkId and gtsWrapper(encoded)!");
      return null;
    }

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    //
    // Encoded
    //

    DataByteArray serialized = (DataByteArray) input.get(0);

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (serialized.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    if (null == gtsWrapper.getMetadata()) {
      gtsWrapper.setMetadata(new Metadata());
    }


    //
    // Add gtsId in the GTS to keep it during Map op
    //

    Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());

    gtsWrapper.setMetadata(metadataChunk);

    //
    // No encoded : ignore it
    //
    if (null != gtsWrapper.getEncoded()) {
      GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);

      try {

        //
        // Push the GTS
        //

        stack.push(gts);

        //
        // Execute the WarpScript onto the stack
        //

        WarpScriptStack.Macro macro = WarpScriptUtils
            .parseScript(stack, this.warpscript);

        stack.exec(macro);

        List<GeoTimeSerie> gtsResults = (List<GeoTimeSerie>) stack.pop();

        //
        // One GTS pushed => one GTS in result
        //

        GeoTimeSerie gtsResult = gtsResults.get(0);

        //
        // We return a DataByteArray of one GTSWrapper encoded
        //

        //
        // Convert these GTS to GTSWrapper
        //

        GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gtsResult);

        //
        // Encode this GTSWrapper
        //

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

        byte[] gtsWrapperEncoded = null;
        try {
          gtsWrapperEncoded = serializer.serialize(wrapper);
        } catch (TException te) {
          throw new IOException(te);
        }

        DataByteArray wrapperValue = new DataByteArray(gtsWrapperEncoded);

        return wrapperValue;

      } catch (WarpScriptException we) {
        throw new IOException(we);
      }
    }

    return null;
  }

  @Override
  public Schema outputSchema(Schema input) {

    //
    // GTSWrapper : bytearray
    //

    Schema.FieldSchema wrapperSchema = new Schema.FieldSchema("encoded", DataType.BYTEARRAY);;

    return new Schema(wrapperSchema);

  }

}