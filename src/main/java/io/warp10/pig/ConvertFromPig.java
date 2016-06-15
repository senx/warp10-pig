package io.warp10.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.store.thrift.data.GTSWrapper;

/**
 * Convert a GTS content (wrapper encoded) to a Pig tuple containing
 * a map of metadata and a bag of tuples with
 * 
 * (ts, lat, lon, elev, value)
 * (ts, lat, lon, value)
 * (ts, elev, value)
 * (ts, value)
 * 
 * depending on the presence or not of lat/lon and elev.
 * 
 * If the UDF is initialized with 'true' then null will be substituted for missing
 * lat/lon/elev so the output tuples will all contain 5 elements
 */
public class ConvertFromPig extends EvalFunc<DataBag> {
  
  private final boolean nullify;
  
  public ConvertFromPig() {
    this.nullify = false;
  }
  
  public ConvertFromPig(String nullify) {
    this.nullify = "true".equals(nullify);
  }
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    
    if (1 != input.size()) {
      throw new IOException("Invalid input, expecting a single GTS.");
    }

    if (!(input.get(0) instanceof DataByteArray)) {
      String type = DataType.findTypeName(DataType.findType(input.get(0)));
      throw new IOException("Invalid input type (" + type + "), expecting a single GTS (encoded)");
    }

    DataBag out = new DefaultDataBag();

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    GTSWrapper wrapper = new GTSWrapper();
    
    try {
      deserializer.deserialize(wrapper, ((DataByteArray) input.get(0)).get());
    } catch (TException te) {
      throw new IOException(te);
    }
    
    ByteBuffer encoded = wrapper.bufferForEncoded();
    
    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(wrapper);
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    
    t.set(0, GTSWrapperPigHelper.metadata2pig(wrapper));
    t.set(1, GTSWrapperPigHelper.decoder2bag(decoder, nullify, this));

    out.add(t);

    return out;

  }

  /**
   *
   * @param input
   * @return Schema - gts: ([class#GTS1,lastbucket#0,base#0,labels#{label0=42}],{(ts,val),(ts,val)}
   */
  @Override
  public Schema outputSchema(Schema input) {

    Schema schema = new Schema();

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("metadata", DataType.MAP));
    fields.add(new Schema.FieldSchema("values", DataType.BAG));

    //Schema tupleSchema = new Schema(fields);

    //Schema.FieldSchema outputTuple = new Schema.FieldSchema("gts", DataType.TUPLE);

    //outputTuple.schema = tupleSchema;

    //return new Schema(outputTuple);

    Schema tupleSchema = new Schema(fields);

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("gts", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }

}
