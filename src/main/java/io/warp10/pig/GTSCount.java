package io.warp10.pig;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;

/**
 * Return the number of datapoints in the GTS (tests)
 * key : GTSWraper without ticks
 * value : GeoTimeSerie (with ticks)
 **/
public class GTSCount extends EvalFunc<Long> {

  public GTSCount() { }

  /**
   * Input : 2 bytearray = key (GTSWrapper) and value (GTS)
   *
   * @param input
   * @return
   * @throws java.io.IOException
   */
  public Long exec(Tuple input) throws IOException {

    if (input.size() != 1) {
      throw new IOException("Tuple with 1 field is required: (encoded)");
    }

    reporter.progress();

    //
    // GTSWrapper instance
    //

    DataByteArray gtsWrapperBytes = (DataByteArray) input.get(0);

    TDeserializer deserializer = new TDeserializer(
        new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    long nbTicks = 0L;
    try {
      deserializer.deserialize(gtsWrapper, (gtsWrapperBytes.get()));
    } catch (TException te) {
      throw new IOException(te);
    }
    nbTicks = gtsWrapper.getCount();

    if (0 == nbTicks) {
      GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);
      nbTicks = gts.size();
    }

    return nbTicks;

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("nbTicks", DataType.INTEGER);

    return new Schema(fieldSchema);

  }

}
