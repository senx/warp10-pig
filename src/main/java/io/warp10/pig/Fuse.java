package io.warp10.pig;

import io.warp10.script.WarpScriptException;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Fuse GTS
 */
public class Fuse extends EvalFunc<DataBag> {

  //
  // Chunk label name
  //

  private static final String chunkLabelName = "chunkId";

  public Fuse() {  }

  /**
   * Fuse a list of GTS
   *
   * @param input
   * @return DataBag with a tuple that contains the GTSWrapper
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (input.size() != 1) {
      throw new IOException("Invalid input, expecting a Bag {(gtswrappers)}");
    }

    if (!(input.get(0) instanceof DataBag)) {
      throw new IOException("Invalid input, expecting a Bag {(gtswrappers)}");
    }

    DataBag bag = (DataBag) input.get(0);

    Iterator<Tuple> iter = bag.iterator();

    List<GeoTimeSerie> geoTimeSeries = new ArrayList<>();

    DataBag outBag;
    Tuple tuple;

    //
    // Occurs on the list of GTS chunk related to the same GTS
    //

    while(iter.hasNext()) {

      Tuple t  = iter.next();

      if (null == t || 0 == t.size()) {
        continue;
      }
      
      //
      // Tuple with 1 field : GTSWrapper (encoded)
      //

      DataByteArray serialized = (DataByteArray) t.get(0);

      if (null == serialized) {
        continue;
      }
      
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

      GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);

      geoTimeSeries.add(gts);
    }

    try {

      GeoTimeSerie gtsFused = GTSHelper.fuse(geoTimeSeries);

      //
      // We return a GTSWrapper encoded
      //

      //
      // Convert this GTS to GTSWrapper
      //

      GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gtsFused);

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

      byte[] gtsWrapperEncoded = null;
      try {
        gtsWrapperEncoded = serializer.serialize(wrapper);
      } catch (TException te) {
        throw new IOException(te);
      }

      DataByteArray wrapperValue =  new DataByteArray(gtsWrapperEncoded);

      outBag = new DefaultDataBag();
      tuple = TupleFactory.getInstance().newTuple(1);
      tuple.set(0, wrapperValue);
      outBag.add(tuple);

    } catch (WarpScriptException e) {
      throw new IOException(e);
    }

    return outBag;

  }

  @Override
  public Schema outputSchema(Schema input) {

    Schema tupleSchema = new Schema(new Schema.FieldSchema("gts", DataType.BYTEARRAY));

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("fuse", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }

}