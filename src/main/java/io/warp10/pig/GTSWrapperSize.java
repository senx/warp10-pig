package io.warp10.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * Returns size of key and value for each record
 **/
public class GTSWrapperSize extends EvalFunc<Tuple> {

  public GTSWrapperSize() { }

  /**
   * Read one SF record
   * @param input - Tuple : key (bytearray), value (optional)
   * @return Tuple
   * @throws IOException
   */
  public Tuple exec(Tuple input) throws IOException {

    if (null == input) {
      throw new IOException("Invalid input, tuples should have at least 1 element : GTSWrapper");
    }

    reporter.progress();

    byte[] key = null;
    byte[] value = null;
    Tuple tuple = null;

    if (1 == input.size()) {
      tuple = TupleFactory.getInstance().newTuple(1);
      value = ((DataByteArray) input.get(0)).get();

      tuple.set(0, value.length);

    } else if  (2 == input.size()){
      tuple = TupleFactory.getInstance().newTuple(3);
      key = ((DataByteArray) input.get(0)).get();
      value = ((DataByteArray) input.get(1)).get();

      tuple.set(0, key.length);
      tuple.set(1, value.length);
      tuple.set(2, key.length + value.length);
    } else {
      throw new IOException("Invalid input, tuples should have at least 1 element : GTSWrapper");
    }

    return tuple;

  }

  @Override
  public Schema outputSchema(Schema input) {
    if (1 == input.size()) {
      Schema.FieldSchema fieldSchema = new Schema.FieldSchema("dataSize", DataType.INTEGER);

     return new Schema(fieldSchema);
    } else {
      Schema schema = new Schema();

      schema.add(new Schema.FieldSchema("keySize", DataType.INTEGER));
      schema.add(new Schema.FieldSchema("valueSize", DataType.INTEGER));
      schema.add(new Schema.FieldSchema("totalSize", DataType.INTEGER));

      return new Schema(new Schema.FieldSchema("size", schema));
    }
  }

}
