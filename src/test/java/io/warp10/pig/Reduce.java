package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.pig.utils.LeptonUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.*;

/**
 * Class used to execute Warpscript function on GTS
 */
public class Reduce extends EvalFunc<DataBag> {

  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  
  //
  // Warpscript
  //
  private String warpscript;

  //
  // ReduceUdf label name to merge GTS after a reduce or apply
  //
  private static final String reduceLabelName = "reduceId";


  public Reduce(String... args) {
    this(args[0]);
  }

  /**
   *
   * @param warpscript Name of the Warpscript used to perform this Reduce
   */
  protected Reduce(String warpscript) {
    this.warpscript = warpscript;
  }

  /**
   * Execute a Warpscript
   *
   * @param input
   * @return DataBag
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (input == null || input.size() < 1) {
      System.err.println("Waiting for 1 parameter : 1 tuple with one Bag. This bag contains 1 tuple with 4 fields : labels, chunkId, reduceId and gtsWrapper(encoded)!");
      return null;
    }

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    List<GeoTimeSerie> geoTimeSeries = new ArrayList<>();

    //
    // First bag : List of chunks ie Tuple with 4 fields (labels, chunkId, reduceId, encoded)
    //

    DataBag groupBag = (DataBag) input.get(0);

    //
    // We return a bag of one Tuple (GTSWrapper encoded)
    //

    DataBag gtsResultBag = BagFactory.getInstance().newDefaultBag();

    Iterator<Tuple> iter = groupBag.iterator();

    //
    // Push all chunks on the stack and launch op
    //

    while(iter.hasNext()) {

      Tuple t  = iter.next();

      //
      // Get chunkId
      //

      String chunkId = (String) t.get(1);

      //
      // Get reduceId
      //

      String reduceId = (String) t.get(2);

      //
      // Get encoded : 4th field
      //

      DataByteArray serialized = (DataByteArray) t.get(3);

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
      // Add reduceId in the GTS to keep it during reduce op
      //

      Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());
      metadataChunk.putToLabels(reduceLabelName, reduceId);

      gtsWrapper.setMetadata(metadataChunk);

      GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);
      geoTimeSeries.add(gts);

    }


    try {

      //
      // Push the list of GTS for the same partition onto the stack
      //

      stack.push(geoTimeSeries);

      //
      // Execute the Warpscript onto the stack
      //

      WarpScriptStack.Macro macro = LeptonUtils.parseScript(stack, this.warpscript);

      stack.exec(macro);

      List<GeoTimeSerie> gtsResult = (List<GeoTimeSerie>) stack.pop();

;

      //
      // Convert these GTS to GTSWrapper and add it to the bag through one Tuple
      //

      for (GeoTimeSerie gts: gtsResult) {

        Tuple resultTuple = TupleFactory.getInstance().newTuple(2);
        GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gts);

        //
        // Encode GTSWrapper
        //

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

        byte[] gtsWrapperEncoded = null;
        try {
          gtsWrapperEncoded = serializer.serialize(wrapper);
        } catch (TException te) {
          throw new IOException(te);
        }

        DataByteArray wrapperValue =  new DataByteArray(gtsWrapperEncoded);

        //
        // Add the reduceId and the wrapper encoded in the response
        //

        String reduceIdAfterOp = gts.getLabel(reduceLabelName);

        resultTuple.set(0, reduceIdAfterOp);
        resultTuple.set(1, wrapperValue);

        gtsResultBag.add(resultTuple);

      }

    } catch (WarpScriptException e) {
      throw new IOException(e);
    }

    return gtsResultBag;
  }

  @Override
  public Schema outputSchema(Schema input) {

    //
    // ReduceId
    //
    Schema.FieldSchema reduceIdSchema = new Schema.FieldSchema("reduceId", DataType.CHARARRAY);


    //
    // GTSWrapper : bytearray
    //

    Schema.FieldSchema wrapperSchema = new Schema.FieldSchema("encoded", DataType.BYTEARRAY);

    //
    // Put this reduceId and GTSWrapper encoded in one tuple (cannot put a bytearray in one bag)
    //

    Schema tupleSchema = new Schema(reduceIdSchema);
    tupleSchema.add(wrapperSchema);

    //
    // More than one occurence - use a bag
    //

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("gts", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }

}