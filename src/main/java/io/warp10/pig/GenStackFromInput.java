package io.warp10.pig;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.pig.utils.GTSWrapperPigHelper;
import io.warp10.pig.utils.StackElement;
import io.warp10.pig.utils.StackElementComparator;
import io.warp10.script.WarpScriptStack;
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
import java.util.Map;

/**
 * Generate a Sorted Bag with different level to insert elements onto a WarpScript stack
 * Input : {(element)} A bag of any object
 */
public class GenStackFromInput extends EvalFunc<DataBag> {

  public GenStackFromInput() { }

  /**
   *
   * @param input : Object
   * @return Bag (SortedDatabag)
   * @throws IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (null == input || input.size() != 1) {
      System.err.println("Tuple with one field (Bag) is required !: {(object: any)}");
      return null;
    }

    DataBag inputBag = (DataBag) input.get(0);

    DataBag sortedDataBag = new SortedDataBag(new StackElementComparator());

    Iterator<Tuple> iter = inputBag.iterator();
    int level = 0;
    while (iter.hasNext()) {
      reporter.progress();
      Tuple tuple = iter.next();
      Tuple outTuple = TupleFactory.getInstance().newTuple(2);
      outTuple.set(0, level);

      Object currentElement = tuple.get(0);
      if (DataType.BYTEARRAY == DataType.findType(currentElement)) {
        outTuple.set(1, ((DataByteArray) currentElement).get());
      } else {
        outTuple.set(1, currentElement);
      }
      sortedDataBag.add(outTuple);
      level++;
    }

    return sortedDataBag;

  }

  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema outputBag = new Schema.FieldSchema("stack", DataType.BAG);
    return new Schema(outputBag);
  }

}