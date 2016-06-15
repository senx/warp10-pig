package io.warp10.pig;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.crypto.OrderPreservingBase64;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;

/**
 * Unwrap GTSWrapper with PreserveOrderBase64
 **/
public class DecodeWrapperB64 extends EvalFunc<DataByteArray> {

  public DecodeWrapperB64() { }

  /**
   * Input :1 bytearray (GTSWrapper Base64 encoded)
   *
   * @param input
   * @return
   * @throws IOException
   */
  public DataByteArray exec(Tuple input) throws IOException {

    if (input.size() != 1) {
      throw new IOException("Tuple with 1 field is required: (encoded)");
    }

    reporter.progress();

    //
    // Unwrap Base64 => byte[]
    //

    DataByteArray gtsWrapperB64Encoded = (DataByteArray) input.get(0);

    byte[] gtsWrapperBytes = OrderPreservingBase64.decode(gtsWrapperB64Encoded.get());

    return new DataByteArray(gtsWrapperBytes);

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("encoded", DataType.BYTEARRAY);

    return new Schema(fieldSchema);

  }

}
