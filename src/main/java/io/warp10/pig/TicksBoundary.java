package io.warp10.pig;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Returns a tuple with the first and the max timestamp
 **/
public class TicksBoundary extends EvalFunc<Tuple> {

  public TicksBoundary() {  }

  /**
   * Input : (GTSWrapper)
   *
   * @param input
   * @return
   * @throws IOException
   */
  public Tuple exec(Tuple input) throws IOException {
    if (null == input || input.size() != 1) {
      System.err.println("1 parameter is required ! - GTSWrapper");
      return null;
    }

    reporter.progress();

    DataByteArray gtsWrapperBytes = (DataByteArray) input.get(0);
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (gtsWrapperBytes.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    if (null == gtsWrapper.getMetadata()) {
      gtsWrapper.setMetadata(new Metadata());
    }

    GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);

    Tuple tuple = TupleFactory.getInstance().newTuple(4);

    //
    // Sort timestamps in reverse order so we can produce all chunks in O(n)
    //

    GTSHelper.sort(gts, true);

    long firstTick = GTSHelper.firsttick(gts);
    long lastTick = GTSHelper.lasttick(gts);

    List<Long> ids = GTSWrapperPigHelper.genIds(gtsWrapper);

    tuple.set(0, ids.get(0));
    tuple.set(1, ids.get(1));
    tuple.set(2, firstTick);
    tuple.set(3, lastTick);

    return tuple;

  }

  @Override
  public Schema outputSchema(Schema input) {

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();
    fields.add(new Schema.FieldSchema("classId", DataType.LONG));
    fields.add(new Schema.FieldSchema("labelsId", DataType.LONG));
    fields.add(new Schema.FieldSchema("firstTick", DataType.LONG));
    fields.add(new Schema.FieldSchema("lastTick", DataType.LONG));

    Schema tupleSchema = new Schema(fields);

    Schema.FieldSchema outputTuple = new Schema.FieldSchema("ticks", DataType.TUPLE);

    outputTuple.schema = tupleSchema;

    return new Schema(outputTuple);

  }

}
