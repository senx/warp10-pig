package io.warp10.pig.utils;

import com.google.common.base.Charsets;
import io.warp10.continuum.gts.*;
import io.warp10.continuum.store.thrift.data.GTSWrapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import io.warp10.crypto.OrderPreservingBase64;
import org.apache.commons.lang3.ClassUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.joda.time.DateTime;

/**
 * Warpscript Caster to Pig
 */
public class PigUtils {


  public PigUtils() { }

  /**
   *
   * @param warpscriptObj
   * @return
   */
  public static Object toPig(Object warpscriptObj) throws IOException {

    Object objCasted = warpscriptObj;

    if (null == objCasted) {
      return null;
    }
    
    if (!ClassUtils.isPrimitiveOrWrapper(warpscriptObj.getClass())) {

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
          convertMap.put(key.toString(), toPig(value));
        }

        objCasted = convertMap;

      } else if (warpscriptObj instanceof Set || warpscriptObj instanceof Vector) {
        
        DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
        
        for (Object o: (Collection<Object>) warpscriptObj) {
          
          Object pigo = toPig(o);
          
          if (pigo instanceof Tuple) {
            bag.add((Tuple) pigo);
          } else {
            Tuple t = TupleFactory.getInstance().newTuple(1);
            t.set(0, pigo);
            bag.add(t);
          }
          
        }
        
        objCasted = bag;
      } else if (warpscriptObj instanceof List) {

        Tuple tuple = TupleFactory.getInstance().newTuple(((List) warpscriptObj).size());

        for (int i=0; i<((List) warpscriptObj).size(); i++) {
          Object elt = ((List) warpscriptObj).get(i);

          tuple.set(i, toPig(elt));
        }
        objCasted = tuple;
      } else if (warpscriptObj instanceof GeoTimeSerie) {
        objCasted = geoTimeSerietoGTSWrapper((GeoTimeSerie) warpscriptObj);
      } else if (warpscriptObj instanceof byte[]) {
        objCasted = new DataByteArray((byte[]) warpscriptObj);
      } else if (warpscriptObj instanceof BigInteger) {
        objCasted = warpscriptObj;
      } else if (warpscriptObj instanceof BigDecimal) {
        objCasted = warpscriptObj;
      } else if (warpscriptObj instanceof DateTime) {
        objCasted = warpscriptObj;
      } else {
        objCasted = warpscriptObj.toString();
      }
    }

    return objCasted;
  }

  public static Object fromPig(Object pigObj) {
    if (DataType.isAtomic(pigObj)) {
      // bytearray, bigchararray, chararray, integer, long, float, double, boolean
      
      Object objectCasted = pigObj;

      byte pigDataType = DataType.findType(pigObj);

      if (DataType.BYTEARRAY == pigDataType) {
        objectCasted = ((DataByteArray) pigObj).get();
      }

      //
      // Otherwise we let the default Pig Cast to be applied
      //

      return objectCasted;
    } else {
      byte type = DataType.findType(pigObj);
      
      switch(type) {
        case DataType.NULL:
          return null;
          
        case DataType.BAG:
          //
          // Bags are converted to vectors
          //
          
          Iterator<Tuple> iter = ((DataBag) pigObj).iterator();
          
          Vector<Object> vector = new Vector<Object>();
          
          while (iter.hasNext()) {
            Tuple tuple = iter.next();
            vector.add(fromPig(tuple));
          }

          return vector;
          
        case DataType.TUPLE:
          //
          // Tuples are converted to lists
          //
          
          List<Object> list = new ArrayList<Object>();
          
          Tuple tuple = (Tuple) pigObj;
              
          for (int i = 0; i < tuple.size(); i++) {
            try {
              list.add(fromPig(tuple.get(i)));
            } catch (ExecException ee) {
              throw new RuntimeException(ee);
            }
          }
          
          return list;
          
        case DataType.MAP:
          //
          // Maps are ... well maps.
          //
          
          Map<String,Object> pigMap = (Map<String,Object>) pigObj;
              
          Map<String,Object> map = new HashMap<String,Object>();
          
          for (Entry<String,Object> entry: pigMap.entrySet()) {
            map.put(entry.getKey(), fromPig(entry.getValue()));
          }
          
          return map;
          
        case DataType.BIGDECIMAL:
        case DataType.BIGINTEGER:
        case DataType.DATETIME:
          return pigObj;
          
        default:
          throw new RuntimeException("Unsupported Pig type.");
      }
    }
  }
  
  /**
   * Convert GeoTimeSerie to GTSWrapper and return this GTSWrapper encoded
   *
   * @param gts
   * @return DataByteArray
   * @throws java.io.IOException
   */
  protected static DataByteArray geoTimeSerietoGTSWrapper(GeoTimeSerie gts) throws IOException {

    GTSWrapper wrapper = GTSWrapperHelper.fromGTSToGTSWrapper(gts, true);

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

  /**
   * Convert a GTSEncoder wrapped as String to GTSWrapper encoded as bytearray (DataByteArray)
   *
   * @param wrappedEncoderStr
   * @return DataByteArray
   * @throws  java.io.IOException
   */
  public static DataByteArray gtsEncoderToGTSWrapper(String wrappedEncoderStr) throws  IOException {
    DataByteArray encoded = null;

    try {
      StringBuffer sb = new StringBuffer(wrappedEncoderStr);

      // Add single quotes around value if it does not contain any

      int lastwsp = wrappedEncoderStr.lastIndexOf(" ");

      sb.insert(lastwsp + 1, "'");
      sb.append("'");

      GTSEncoder encoder = GTSHelper.parse(null, sb.toString());

      GTSDecoder decoder = encoder.getDecoder(true);

      while(decoder.next()) {
        long ts = decoder.getTimestamp();
        String value = decoder.getValue().toString();

        byte[] bytes = OrderPreservingBase64.decode(value.getBytes(Charsets.UTF_8));
        decoder = new GTSDecoder(ts, ByteBuffer.wrap(bytes));
        decoder.setMetadata(encoder.getMetadata());

        break;
      }

      if (decoder.next()) {
        GTSWrapper gtsWrapper = GTSWrapperHelper.fromGTSEncoderToGTSWrapper(decoder.getEncoder(true), true);

        //
        // Encode GTSEncoder
        //

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        try {
          encoded = new DataByteArray(serializer.serialize(gtsWrapper));
        } catch (TException te) {
          throw new IOException(te);
        }
      }

    } catch (ParseException pe) {
      throw new IOException(pe);
    }
    return encoded;
  }
}
