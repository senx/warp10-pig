package io.warp10.pig;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.List;

/**
 * Output a map with Metadata from a GTSWrapper
 */
public class GTSWrapperToSF extends EvalFunc<Tuple> {

  @Override
  public Tuple exec(Tuple input) throws IOException {

    if (1 != input.size()) {
      throw new IOException("Invalid input, expecting a GTS instance.");
    }
    
    if (!(input.get(0) instanceof DataByteArray)) {
      throw new IOException("Invalid input, expecting a GTS instance.");
    }

    byte[] data = ((DataByteArray) input.get(0)).get();

    //
    // Deserialize
    //

    GTSWrapper wrapper = new GTSWrapper();

    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

    try {
      deser.deserialize(wrapper, data);
    } catch (TException te) {
      throw new IOException(te);
    }

    List<DataByteArray> kv = GTSWrapperPigHelper.gtsWrapperToSF(wrapper);

    Tuple tuple = TupleFactory.getInstance().newTuple(kv);

    return tuple;

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema schema = new Schema();

    schema.add(new Schema.FieldSchema("key", DataType.BYTEARRAY));
    schema.add(new Schema.FieldSchema("value", DataType.BYTEARRAY));

    return new Schema(new Schema.FieldSchema("kv", schema));

  }

}
