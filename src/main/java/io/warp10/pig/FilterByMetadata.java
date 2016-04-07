package io.warp10.pig;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Filter a Bag of GTSWrapper on class name
 * Bag contains tuple with GTSWrapper
 */
public class FilterByMetadata extends EvalFunc<DataBag> {

  private String filterType;
  private String filterValue;

  public FilterByMetadata(String... args) {
    filterType = args[0];
    filterValue = args[1];
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (1 != input.size()) {
      throw new IOException("Invalid input, expecting a Bag of GTS instance.");
    }
    
    if (!(input.get(0) instanceof DataBag)) {
      throw new IOException("Invalid input, expecting a Bag of GTS instance.");
    }

    DataBag inbag = (DataBag) input.get(0);
    DataBag outbag = new DefaultDataBag();

    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

    Iterator<Tuple> iter = inbag.iterator();

    while(iter.hasNext()) {
      Tuple t = iter.next();

      //
      // GTSWrapper
      //

      byte[] wrapperData = ((DataByteArray) t.get(0)).get();

      //
      // Deserialize metadata
      //

      GTSWrapper wrapper = new GTSWrapper();

      try {
        deser.deserialize(wrapper, wrapperData);
      } catch (TException te) {
        throw new IOException(te);
      }

      switch (filterType) {
        case "name":
          if (wrapper.getMetadata().getName().equals(filterValue)) {
            outbag.add(t);
          }
          break;
        default:
          throw new IOException("This type of filter (" + filterType + ") is not yet implemented");
      }

    }

    return outbag;
  }

  @Override
  public Schema outputSchema(Schema input) {
    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("gts", DataType.BYTEARRAY));

    Schema tupleSchema = new Schema(fields);

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("bag", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }
}
