package io.warp10.pig;

import java.io.IOException;
import java.util.Map;

import io.warp10.pig.utils.GTSWrapperPigHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.store.thrift.data.GTSWrapper;

/**
 * Output a map with Metadata from a GTSWrapper
 */
public class UnpackGTSMetadata extends EvalFunc<Map<String, Object>> {
  @Override
  public Map<String, Object> exec(Tuple input) throws IOException {

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

    return GTSWrapperPigHelper.metadata2pig(wrapper);
  }
}
