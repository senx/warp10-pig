package io.warp10.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

/**
 * From a Bag with (key,value) tuples where key is a GTSWrapper and value encoded data
 * Transform it in a Bag of GTSwrapper
 */
public class GTSWrapperMerge extends EvalFunc<DataBag> {

  public GTSWrapperMerge(String... args) {
    
  }

  @Override
  public DataBag exec(Tuple input) throws IOException {
    return null;
  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema tupleSchema = new Schema(new Schema.FieldSchema("wrapper", DataType.BYTEARRAY));

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("gts", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);
  }
}
