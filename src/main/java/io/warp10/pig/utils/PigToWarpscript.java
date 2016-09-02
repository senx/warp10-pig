package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * Pig Caster - Pig type to Warpscript type
 */
public class PigToWarpscript {

  /**
   *
   */
  protected PigToWarpscript(String... args) { }

  /**
   * Cast Atomic type values for Warpscript
   * @param value
   * @return
   */
  public static Object atomicToWarpscript(Object value) throws IOException {

    Object objectCasted = value;

    byte pigDataType = DataType.findType(value);

    if (DataType.BYTEARRAY == pigDataType) {
      objectCasted = ((DataByteArray) value).get();
    }

    //
    // Otherwise we let the default Pig Cast to be applied
    //

    return objectCasted;

  }

  /**
   * Cast Complex type values for Warpscript
   * @param value
   * @return
   */
  public static Object complexToWarpscript(Object value) throws IOException {

    Object objectCasted = null;

    byte pigDataType = DataType.findType(value);

    switch (pigDataType) {

      case DataType.TUPLE :

        Tuple tuple = (Tuple) value;

        objectCasted = pigTupleToWarpscript(tuple);

        break;

      case DataType.MAP :

        java.util.Map<String, Object> map = (java.util.Map<String, Object>)(value);

        long mapSize = Long.valueOf(map.keySet().size());

        //
        // Map value must be atomic
        // java.util.Map => Map (Warpscript)
        //

        java.util.Map<String, Object> mapCasted = new LinkedHashMap<>();

        for (java.util.Map.Entry<String, Object> entry : map.entrySet()) {

          Object o = entry.getValue();

          if (DataType.isAtomic(o)) {
            mapCasted.put(entry.getKey(), atomicToWarpscript(o));
          } else if (DataType.NULL == DataType.findType(o)) {
            mapCasted.put(entry.getKey(), null);
          } else {
            System.err.println(DataType.findTypeName(o) + " cannot be cast to Warpscript type - Type of Map value nust be atomic.");
          }

        }

        objectCasted = mapCasted;

          break;

      default:

        if (DataType.BAG == pigDataType) {
          throw new ExecException(DataType.findTypeName(value) + " : iterate on Bag");
        } else {
          throw new ExecException(DataType.findTypeName(value) + " cannot be cast to Warpscript yet..");
        }

    }

    return objectCasted;
  }

  protected static Object pigTupleToWarpscript(Tuple tuple) throws IOException {

    Object objectCasted = null;

    if (tuple.size() > 0) {
      //
      // List of casted objects (atomic type)
      //

      List<Object> elementsCasted = new ArrayList<>();

      //
      // We have to iterate : if bytearray => cast to GeoTimeSerie
      //

      for (int i = 0; i < tuple.size(); i++) {
        Object currentObject = tuple.get(i);
        if (DataType.isAtomic(currentObject)) {
          elementsCasted.add(atomicToWarpscript(currentObject));
        } else if (DataType.NULL == DataType.findType(currentObject)) {
          elementsCasted.add(null);
        } else {
          elementsCasted.add(complexToWarpscript(currentObject));
        }
      }

      //
      // return the list
      //

      objectCasted = elementsCasted;
    } else {
      //
      // Tuple is empty
      //

      objectCasted = new ArrayList<>();
    }

    return objectCasted;
  }

}
