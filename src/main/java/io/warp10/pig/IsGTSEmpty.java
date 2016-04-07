package io.warp10.pig;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
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
 * Return true if the number of datapoints in the GTS equals to 0
 * key : GTSWraper without ticks
 * value : boolean
 **/
public class IsGTSEmpty extends EvalFunc<Boolean> {

  public IsGTSEmpty() { }

  /**
   * Input : 1 bytearray = encoded (GTSWrapper)
   *
   * @param input
   * @return
   * @throws java.io.IOException
   */
  public Boolean exec(Tuple input) throws IOException {

    if (input.size() != 1) {
      throw new IOException("Tuple with 1 field is required : (encoded)");
    }

    //
    // GTSWrapper instance
    //

    DataByteArray gtsWrapperBytes = (DataByteArray) input.get(0);

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (gtsWrapperBytes.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    return (gtsWrapper.getCount() == 0L);

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("isEmpty", DataType.BOOLEAN);

    return new Schema(fieldSchema);

  }

}
