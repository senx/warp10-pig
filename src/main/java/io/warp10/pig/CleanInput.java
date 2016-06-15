package io.warp10.pig;

import io.warp10.pig.utils.StackElement;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Clean Input Bag before a WarpScriptRun. Delete tuples without level for example
 * Transform a Bag like this { value, (1, obj1), ... } into {((1, obj1), ... )}
 * Input : { value, (1, obj1), ... }
 */
public class CleanInput extends EvalFunc<DataBag> {

  public CleanInput() { }

  /**
   *
   * @param input : { value, (1, obj1), ... }
   * @return {((1, obj1), ... )}
   * @throws IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (null == input || input.size() != 1) {
      System.err.println("Tuple with one field (Bag) is required !: ({ value, (1, obj1), ... })");
      return null;
    }

    DataBag inputBag = (DataBag) input.get(0);

    DataBag outBag = BagFactory.getInstance().newDefaultBag();

    //
    // Delete tuple without level
    //

    Iterator<Tuple> iter = inputBag.iterator();
    while (iter.hasNext()) {
      reporter.progress();
      Tuple tuple = iter.next();

      Tuple innerTuple1 = (Tuple) tuple.get(0);

      for (Object elt: innerTuple1.getAll()) {
        if (null != elt) {
          if ((2 == ((Tuple) elt).size())) {
            int level = (int) ((Tuple) elt).get(0);
            Object currentElement = ((Tuple) elt).get(1);

            Tuple outTuple = TupleFactory.getInstance().newTuple(2);
            outTuple.set(0, level);
            outTuple.set(1, currentElement);
            outBag.add(outTuple);
          }
        }
      }
    }

    return outBag;

  }

  /**
   * @return  {(1, obj1), ... )}
   */
  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema outputBag = new Schema.FieldSchema("stack", DataType.BAG);
    return new Schema(outputBag);
  }

}