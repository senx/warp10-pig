package io.warp10.pig;

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
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns a tuple with the first and the max timestamp
 * key : GTSWraper without ticks
 * value : GeoTimeSerie (with ticks)
 **/
public class TicksBoundaryFromSF extends EvalFunc<Tuple> {

  public TicksBoundaryFromSF() {  }

  /**
   * Input : 2 bytearray fields (k,v)
   *
   * @param input
   * @return
   * @throws java.io.IOException
   */
  public Tuple exec(Tuple input) throws IOException {
    if (null == input || input.size() != 2) {
      System.err.println("2 parameters are required !");
      return null;
    }

    DataByteArray gtsWrapperBytes = (DataByteArray) input.get(0);
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapperPartial = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapperPartial, (gtsWrapperBytes.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    DataByteArray gtsEncodedData = (DataByteArray) input.get(1);
    gtsWrapperPartial.setEncoded(gtsEncodedData.get());

    if (null == gtsWrapperPartial.getMetadata()) {
      gtsWrapperPartial.setMetadata(new Metadata());
    }

    GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapperPartial);

    Tuple tuple = TupleFactory.getInstance().newTuple(2);

    //
    // Sort timestamps in reverse order so we can produce all chunks in O(n)
    //

    GTSHelper.sort(gts, true);

    //
    // If lastchunk is 0, use lastbucket or the most recent tick
    //

    long firstTick = GTSHelper.firsttick(gts);
    long lastTick = GTSHelper.lasttick(gts);

    tuple.set(0, firstTick);
    tuple.set(1, lastTick);

    return tuple;

  }

  @Override
  public Schema outputSchema(Schema input) {

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
    fields.add(new Schema.FieldSchema("firstTick", DataType.LONG));
    fields.add(new Schema.FieldSchema("lastTick", DataType.LONG));

    Schema tupleSchema = new Schema(fields);

    Schema.FieldSchema outputTuple = new Schema.FieldSchema("gts", DataType.TUPLE);

    outputTuple.schema = tupleSchema;

    return new Schema(outputTuple);

  }

}
