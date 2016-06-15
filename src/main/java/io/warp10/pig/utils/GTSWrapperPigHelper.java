package io.warp10.pig.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.DummyKeyStore;
import io.warp10.crypto.KeyStore;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import com.geoxp.GeoXPLib;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

/**
 * Class with handy methods to manipulate GTSWrapper within Pig
 * [class#GTS1,lastbucket#0,base#0,labels#{label0=42}]
 */
public class GTSWrapperPigHelper {

  //
  // KeyStore
  //

  private static final KeyStore keyStore = new DummyKeyStore();

  //
  // 128 Key to compute classId and labelsId used to join GTS
  //

  // FIXME : must be set in a properties file !

  private static final String classLabelsKey = "hex:4dd3de27c81561293d857784b98c6bbc";

  public static Map<String,Object> metadata2pig(GTSWrapper wrapper) {
    Map<String,Object> map = new HashMap<String, Object>();
    
    map.put("base", wrapper.getBase());
    map.put("bucketcount", wrapper.getBucketcount());
    map.put("bucketspan", wrapper.getBucketspan());
    map.put("lastbucket", wrapper.getLastbucket());
    map.put("count", wrapper.getCount());
    if (null != wrapper.getMetadata()) {
      if (null != wrapper.getMetadata().getName()) {
        map.put("class", wrapper.getMetadata().getName());
      } else {
        map.put("class", "");
      }
      if (null != wrapper.getMetadata().getLabels()) {
        map.put("labels",  wrapper.getMetadata().getLabels());
      } else {
        map.put("labels", new HashMap<String,String>());
      }
      if (null != wrapper.getMetadata().getAttributes()) {
        map.put("attributes", wrapper.getMetadata().getAttributes());
      } else {
        map.put("attributes", new HashMap<String,String>());
      }
    }
    return map;
  }

  /**
   *
   * @param map with Metadata values
   * @return Metadata
   */
  public static Metadata pig2Metadata(Map map) {
    Metadata metadata = new Metadata();

    if (null != map.get("class")){
      metadata.setName((String)map.get("class"));
    }

    if (null != map.get("labels")){
      metadata.setLabels((Map) map.get("labels"));
    } else {
      metadata.setLabels(new HashMap<String,String>());
    }

    if (null != map.get("attributes")){
      metadata.setAttributes((Map)map.get("attributes"));
    } else {
      metadata.setAttributes(new HashMap<String, String>());
    }

    return metadata;
  }

  public static DataBag decoder2bag(GTSDecoder decoder, boolean nullify, EvalFunc from) throws IOException {
    DataBag bag = new DefaultDataBag();
    boolean hasReporter = null != from.getReporter();

    while(decoder.next()) {
      long ts = decoder.getTimestamp();
      long location = decoder.getLocation();
      long elevation = decoder.getElevation();
      Object value = decoder.getValue();

      Tuple t;
      
      if (nullify) {
        t = TupleFactory.getInstance().newTuple(5);
        t.set(0, ts);
        double latlon[] = GeoXPLib.fromGeoXPPoint(location);
        t.set(1, latlon[0]);
        t.set(2, latlon[1]);
        t.set(3, elevation);
        t.set(4, value);
      } else if (GeoTimeSerie.NO_LOCATION == location) {
        if (GeoTimeSerie.NO_ELEVATION != elevation) {
          t = TupleFactory.getInstance().newTuple(3);         
          t.set(0, ts);
          t.set(1, elevation);
          t.set(2, value);
        } else {
          t = TupleFactory.getInstance().newTuple(2);         
          t.set(0, ts);
          t.set(1, value);          
        }
      } else if (GeoTimeSerie.NO_ELEVATION == elevation) {
        t = TupleFactory.getInstance().newTuple(4); 
        t.set(0, ts);
        double latlon[] = GeoXPLib.fromGeoXPPoint(location);
        t.set(1, latlon[0]);
        t.set(2, latlon[1]);
        t.set(3, value);
      } else {
        t = TupleFactory.getInstance().newTuple(5); 
        t.set(0, ts);
        double latlon[] = GeoXPLib.fromGeoXPPoint(location);
        t.set(1, latlon[0]);
        t.set(2, latlon[1]);
        t.set(3, elevation);
        t.set(4, value);
      }
      
      bag.add(t);

      if (hasReporter) {
       // Call progress so we tell Pig we're still kicking
        from.progress();
      }
    }

    return bag;
  }

  /**
   * Return a List (classId, labelsId) against one GTSWrapper
   * @param gtsWrapper
   * @return List<Long>: (classId, labelsId>)
   */
  public static List<Long> genIds(GTSWrapper gtsWrapper) {
    String className = gtsWrapper.getMetadata().getName();
    Map labels = gtsWrapper.getMetadata().getLabels();

    Long classIdForJoin = GTSHelper.classId(keyStore.decodeKey(classLabelsKey), className);
    Long labelsIdForJoin = GTSHelper.labelsId(keyStore.decodeKey(classLabelsKey), labels);

    List<Long> ids = new ArrayList<Long>();
    ids.add(0, classIdForJoin);
    ids.add(1, labelsIdForJoin);

    return ids;
  }

  /**
   * Return the GTSWrapper instance as key,value where key is the GTSWrapper encoded (with ticks) and
   * value only the ticks (encoded field only)
   * @param gtsWrapper
   * @return List<DataByteArray> : (DataByteArray key, DataByteArray value)
   **/
  public static List<DataByteArray> gtsWrapperToSF(GTSWrapper gtsWrapper) throws IOException {

    //
    // Keep datapoints (value)
    //

    byte[] encodedField = gtsWrapper.getEncoded();

    //
    // key : GTSWrapper with no datapoints
    //

    gtsWrapper.setEncoded(new byte[0]);

    //
    // Encode GTSWrapper
    //

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    byte[] wrapperSerialized = new byte[0];
    try {
      wrapperSerialized = serializer.serialize(gtsWrapper);
    } catch (TException te) {
      throw new IOException(te);
    }

    List<DataByteArray> kv = new ArrayList<>(2);
    kv.add(new DataByteArray(wrapperSerialized));
    kv.add(new DataByteArray(encodedField));

    return kv;

  }
}
