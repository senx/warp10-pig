package io.warp10.pig;

import io.warp10.pig.utils.StackElement;
import io.warp10.pig.utils.StackElementComparator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Transform a Tuple like this { (0, meta), (1, obj1), ... } into (( meta, (1, obj1), ... ))
 * Then, it's quite easy to group elements concerning the same metadata
 * We duplicate the first element (level 0 = metadata)
 * Input : { (0, meta), (1, obj1), ... }
 */
public class FlattenFirstTuple extends EvalFunc<Tuple> {

  public FlattenFirstTuple() { }

  /**
   *
   * @param input : { (0, meta), (1, obj1), ... }
   * @return (( meta, (1, obj1), ... ))
   * @throws IOException
   */
  @Override
  public Tuple exec(Tuple input) throws IOException {

    if (null == input || input.size() != 1) {
      System.err.println("Tuple with one field (Bag) is required !: ({ (0, meta), (1, obj1), ... })");
      return null;
    }

    DataBag inputBag = (DataBag) input.get(0);

    //
    // Order objects in this Bag and add them to a List
    //

    List<StackElement> sortedList = new ArrayList<>();

    Iterator<Tuple> iter = inputBag.iterator();
    while (iter.hasNext()) {
      reporter.progress();
      Tuple tuple = iter.next();

      int level = (int) tuple.get(0);
      Object currentElement = tuple.get(1);
      StackElement stackElement = new StackElement(level, currentElement);
      sortedList.add(stackElement);
    }

    Collections.sort(sortedList);

    Tuple outTuple = TupleFactory.getInstance().newTuple(sortedList.size() + 1);

    //
    // Current element of the stack (output Tuple)
    //
    int level = 0;
    for (StackElement element: sortedList) {

      //
      // Get the first element: add it in the output Bag without level
      //

      if (0 == level) {
        Tuple meta = TupleFactory.getInstance().newTuple(1);
        meta.set(0, element.getElement());
        outTuple.set(level, meta);
      } else {
        Tuple currentObject = TupleFactory.getInstance().newTuple(2);
        currentObject.set(0, element.getLevel());
        currentObject.set(1, element.getElement());
        outTuple.set(level, currentObject);
      }

      level++;

    }

    return outTuple;

  }

  /**
   * @return  (( meta, (1, obj1), ... ))
   */
  @Override
  public Schema outputSchema(Schema input) {
    Schema.FieldSchema outputBag = new Schema.FieldSchema("stack", DataType.TUPLE);
    return new Schema(outputBag);
  }

}