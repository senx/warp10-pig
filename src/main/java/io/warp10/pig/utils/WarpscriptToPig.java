package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.script.WarpScriptStack;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Warpscript Caster to Pig
 */
public class WarpscriptToPig {


  public WarpscriptToPig() { }

  /**
   *
   * @param warpscriptObj
   * @return
   */
  public static Object cast(Object warpscriptObj) throws IOException {

    Object objCasted = warpscriptObj;

    if (null == objCasted) {
      return null;
    }
    
    if (! warpscriptObj.getClass().isPrimitive()) {

      //
      // Map => Map : can contain complex object
      // List => Tuple
      //

      if (warpscriptObj instanceof Map) {
        HashMap convertMap = new HashMap();

        Iterator<Object> iter = ((Map) warpscriptObj).keySet().iterator();
        while (iter.hasNext()) {
          Object key = iter.next();
          Object value = ((Map) warpscriptObj).get(key);
          if (value instanceof List) {
            convertMap.put(key, cast(value));
          } else {
            convertMap.put(key, value);
          }
        }

        objCasted = convertMap;

      } else if (warpscriptObj instanceof Set || warpscriptObj instanceof Vector) {
        
        DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
        
        for (Object o: (Collection<Object>) warpscriptObj) {
          Tuple t = TupleFactory.getInstance().newTuple(1);
          
          t.set(0, cast(o));
          
          bag.add(t);
        }
        
        objCasted = bag;
      } else if (warpscriptObj instanceof List) {

        Tuple tuple = TupleFactory.getInstance().newTuple(((List) warpscriptObj).size());


        for (int i=0; i<((List) warpscriptObj).size(); i++) {
          Object elt = ((List) warpscriptObj).get(i);

          //
          // GeoTimeSerie : convert it to bytearray (GTSWrapper) and then to Pig
          // We keep it as encoded to be more efficient - Use GTStoPig to dump it
          //

          if (elt instanceof GeoTimeSerie) {

            tuple.set(i, geoTimeSerietoGTSWrapper((GeoTimeSerie)elt));

          } else {

            tuple.set(i, cast(elt));

          }
        }
        objCasted = tuple;
      } else if (warpscriptObj instanceof GeoTimeSerie) {

        objCasted = geoTimeSerietoGTSWrapper((GeoTimeSerie)warpscriptObj);

      } else if (warpscriptObj instanceof byte[]) {

        objCasted = new DataByteArray((byte[]) warpscriptObj);

      } else {
        objCasted = warpscriptObj.toString();
      }
    }

    return objCasted;

  }

  /**
   * Convert GeoTimeSerie to GTSWrapper and return this GTSWrapper encoded
   *
   * @param gts
   * @return DataByteArray
   * @throws java.io.IOException
   */
  protected static DataByteArray geoTimeSerietoGTSWrapper(GeoTimeSerie gts) throws IOException {

    GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gts);

    //
    // Encode GTSWrapper
    //
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    byte[] wrapperSerialized = new byte[0];
    try {
      wrapperSerialized = serializer.serialize(wrapper);
    } catch (TException te) {
      throw new IOException(te);
    }

    DataByteArray wrapperEncoded =  new DataByteArray(wrapperSerialized);

    return wrapperEncoded;

  }

}
