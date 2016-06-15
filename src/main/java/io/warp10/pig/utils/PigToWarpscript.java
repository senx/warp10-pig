package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import org.apache.pig.backend.executionengine.ExecException;
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

      GTSWrapper gtsWrapper = WarpScriptUtils
          .encodedToGTSWrapper(((DataByteArray) value).get());

      GeoTimeSerie gts = GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper);

      objectCasted = gts;

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

          } else {

            System.err.println(DataType.findTypeName(o) + " cannot be cast to Warpscript type - Type of Map value nust be atomic.");

          }

        }

        objectCasted = mapCasted;

          break;

      default:

        if (DataType.BAG == pigDataType) {

          //
          // Iterate on Bag and provide tuples to avoid misunderstanding
          //
          throw new ExecException(DataType.findTypeName(value) + " : iterate on tuples");

        } else {

          throw new ExecException(DataType.findTypeName(value) + " cannot be cast to Warpscript yet..");

        }

    }

    return objectCasted;
  }

  protected static Object pigTupleToWarpscript(Tuple tuple) throws IOException {

    Object objectCasted;

    //
    // If one field (singleton) put the element onto the stack
    // Otherwise create a list
    // We suppose elements in the list or the singleton is an atomic type
    //

    if (tuple.size() > 1) {

      //
      // List of elements in the current tuple
      //

      List<Object> inputElements = tuple.getAll();

      //
      // List of casted objects (atomic type)
      //
      List<Object> elementsCasted = new ArrayList<>();

      //
      // We have to iterate : if bytearray => cast to GeoTimeSerie
      // Important : Only atomic types have been accepted in tuple
      //
      for (Object inputElement : inputElements) {
        elementsCasted.add(atomicToWarpscript(inputElement));
      }

      //
      // return the list
      //

      objectCasted = elementsCasted;

    } else {

      //
      // element (singleton) must be an atomic type
      //

      Object singleElt = tuple.get(0);

      objectCasted = atomicToWarpscript(singleElt);

    }

    return objectCasted;

  }

}
