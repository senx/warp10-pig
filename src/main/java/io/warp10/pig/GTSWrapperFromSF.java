package io.warp10.pig;

import io.warp10.continuum.gts.*;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.DummyKeyStore;
import io.warp10.crypto.KeyStore;
import io.warp10.pig.utils.WarpScriptUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merge the SequenceFile key,value in one instance of GTSWrapper
 * Returns a bag of n tuples (cf Schema)
 * key : GTSWraper without ticks
 * value : GeoTimeSerie (with ticks)
 **/
public class GTSWrapperFromSF extends EvalFunc<DataBag> {

  //
  // KeyStore
  //

  private static final KeyStore keyStore = new DummyKeyStore();

  //
  // 128 Key to compute classId and labelsId used to join GTS
  //

  // FIXME : must be set in a properties file !

  private final String classLabelsKey = "hex:4dd3de27c81561293d857784b98c6bbc";

  //
  // GTS id label name - used to merge chunks for non reduce/apply fcts like map,
  // deprecated: have to be done out of the load process (a gts can be splitted !)
  //
  //private static final String gtsIdLabelName = "gtsId";

  //
  // classId used to join GTS to get the reduceId (normal and light mode)
  //
  private static final String classIdForJoinName = "classIdForJoin";

  //
  // labelsId used to join GTS to get the reduceId (normal and light mode)
  //
  private static final String labelsIdForJoinName = "labelsIdForJoin";


  //
  // mode : light, normal or chunk
  // light : metadata
  // normal : metadata + value
  // chunk : split GTSWrapper in chunks of GTSWrapper
  private String mode = "chunk";

  //
  // Gen labelsId, classId
  //
  //
  private boolean genIds = false;

  //
  // FIXME : chunkwidth clipFrom and clipTo will have to be passed as parameter
  //

  private long clipFrom = 0L;
  private long clipTo = 0L;
  private long chunkwidth = 0L;

  /**
   * GTSWrapperFromSF - In chunk mode : 4 arguments required
   * @param mode (light, normal, chunk)
   * @param genIds (Gen labelsId, classId ?)
   * @param clipFrom (chunk mode)
   * @param clipTo (chunk mode)
   * @param chunkwidth (chunk mode)
   *
   */
  public GTSWrapperFromSF(String... args) throws IOException {
      this.mode = args[0];

      this.genIds = Boolean.valueOf(args[1]).booleanValue();

    //
    // clipFrom, clipTo and chunkwidth used only in chunk mode
    //

    if ("chunk".equals(this.mode)) {
      if ((null != args[2]) && (null != args[3]) && (null != args[4])) {
        this.clipFrom = Long.valueOf(args[2]);
        this.clipTo = Long.valueOf(args[3]);
        this.chunkwidth = Long.valueOf(args[4]);
      } else {
        throw new IOException("clipFrom, clipTo and chunkwidth are required in chunk mode");
      }
    }
  }

  /**
   * Read one SF record
   * @param input - Tuple : GTSWrapper(bytearray)
   * @return DataBag
   * @throws IOException
   */
  public DataBag exec(Tuple input) throws IOException {

    if (null == input || input.size() < 1) {
      throw new IOException("Invalid input, tuples should have 1 element : value (GTSWrapper).");
    }

    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

    //
    // Bag with one tuple of GTS or chunks
    //

    DataBag resultBag = new DefaultDataBag();
    Tuple resultTuple = TupleFactory.getInstance().newTuple(6);


    //
    // GTSWrapper instance
    //

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, ((DataByteArray) input.get(1)).get());
    } catch (TException te) {
      throw new IOException(te);
    }

    //
    // FIXME : not required for Map or quite the same fct
    // Gen classId and labelsId with a specific hash
    // Why ? because of partition op
    // We want to gen a reduce for reduce/apply
    // GTS with common labels will have the same ids
    // We use the PARTITION fct on the original GTS where we previously deleted ticks (light GTS)
    // To Join the GTS and the result of this partition op (and thus get the reduce id) we have to provide a common "id"
    // These new classId and labelsId will help us to perform this join
    //

    String className = gtsWrapper.getMetadata().getName();

    Map labels = gtsWrapper.getMetadata().getLabels();

    //
    // Register these ids as labels in the current GTS
    //

    Metadata metadata = gtsWrapper.getMetadata();

    long classIdForJoin = Long.MAX_VALUE;
    long labelsIdForJoin = Long.MAX_VALUE;

    if (genIds) {
      if (!metadata.getLabels().containsKey(classIdForJoinName)) {
        if (null != className) {
          classIdForJoin = GTSHelper
              .classId(keyStore.decodeKey(classLabelsKey), className);
          metadata
              .putToLabels(classIdForJoinName, String.valueOf(classIdForJoin));
        } else {
          classIdForJoin = GTSHelper
              .classId(keyStore.decodeKey(classLabelsKey),
                  java.util.UUID.randomUUID().toString());
        }
      }

      if (!metadata.getLabels().containsKey(labelsIdForJoinName)) {
        if (null != labels) {
          labelsIdForJoin = GTSHelper
              .labelsId(keyStore.decodeKey(classLabelsKey),
                  gtsWrapper.getMetadata().getLabels());
          metadata
              .putToLabels(labelsIdForJoinName,
                  String.valueOf(labelsIdForJoin));
        } else {
          labelsIdForJoin = GTSHelper
              .classId(keyStore.decodeKey(classLabelsKey),
                  java.util.UUID.randomUUID().toString());
        }
      }
    }

    //
    // gtsId used to merge chunks for non reduce or apply fcts (map, ..)
    // FIXME: Warning - GTS can be splitted when a max size has been reached. Thus a fuse has to be done without using this gtsId (N gtsId for the same GTS after N splits) - So, Gen of this gtsId has to be done manually (with UDF) out of the load process
    //
    //String gtsId = java.util.UUID.randomUUID().toString();
    //metadata.putToLabels(gtsIdLabelName, gtsId);

    gtsWrapper.setMetadata(metadata);

    if ("chunk".equalsIgnoreCase(this.mode)) {

      //
      // chunk : we return a bag with chunks => { tuple(labels, gtsId, chunkId, encoded)}
      //

      List<GTSWrapper> chunks = WarpScriptUtils
          .chunk(gtsWrapper, clipFrom, clipTo, chunkwidth);

      //System.out.println("nb chunks : " + chunks.size());

      for (GTSWrapper chunk : chunks) {

        //
        // Get chunk id and put it in chunk Metadata
        //

        String chunkId = chunk.getMetadata().getLabels()
            .get(WarpScriptUtils.chunkIdLabelName);
        Metadata metadataChunk = new Metadata(
            gtsWrapper.getMetadata());
        metadataChunk.putToLabels(WarpScriptUtils.chunkIdLabelName, chunkId);

        chunk.setMetadata(metadataChunk);

        if (gtsWrapper.isSetKey()) {
          chunk.setKey(gtsWrapper.getKey());
        }

        //
        // Add the current chunk to bag - We have to serialize it
        //



        byte[] chunkEncoded = null;
        try {
          chunkEncoded = serializer.serialize(chunk);
        } catch (TException te) {
          throw new IOException(te);
        }

        DataByteArray chunkData = new DataByteArray(chunkEncoded);

        //
        // Set Labels - Can be used as equiv classes
        //

        resultTuple.set(0, className);

        resultTuple.set(1, chunk.getMetadata().getLabels());

        if (genIds) {

          resultTuple.set(2, classIdForJoin);

          resultTuple.set(3, labelsIdForJoin);

          resultTuple.set(4, chunk.getMetadata().getLabels().get(
              WarpScriptUtils.chunkIdLabelName));

          resultTuple.set(5, chunkData);

        } else {

          resultTuple.set(2, chunk.getMetadata().getLabels().get(
              WarpScriptUtils.chunkIdLabelName));

          resultTuple.set(3, chunkData);

        }

        resultBag.add(resultTuple);

      }

    } else {

      //
      // normal : we return a bag with one GTS => { tuple(labels, classIdForJoin, labelsIdForJoin, encoded)}
      // light : we return a bag with one GTS (without ticks) => { tuple(labels, classIdForJoin, labelsIdForJoin, encoded)}
      //

      //
      // Serialize this wrapper
      //

      byte[] data = null;
      try {
        data = serializer.serialize(gtsWrapper);
      } catch (TException te) {
        throw new IOException(te);
      }

      DataByteArray gtsData = new DataByteArray(data);

      resultTuple.set(0, className);

      resultTuple.set(1, gtsWrapper.getMetadata().getLabels());

      if (genIds) {

        resultTuple.set(2, classIdForJoin);

        resultTuple.set(3, labelsIdForJoin);

        resultTuple.set(4, gtsData);

      } else {

        resultTuple.set(2, gtsData);

      }

      resultBag.add(resultTuple);

    }

    return resultBag;

  }

  @Override
  public Schema outputSchema(Schema input) {

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("className", DataType.CHARARRAY));
    fields.add(new Schema.FieldSchema("labels", DataType.MAP));

    if (genIds) {
      fields.add(new Schema.FieldSchema(classIdForJoinName, DataType.LONG));
      fields.add(new Schema.FieldSchema(labelsIdForJoinName, DataType.LONG));
    }

    if ("chunk".equalsIgnoreCase(this.mode)) {
      fields.add(new Schema.FieldSchema(WarpScriptUtils.chunkIdLabelName, DataType.CHARARRAY));
    }

    fields.add(new Schema.FieldSchema("encoded", DataType.BYTEARRAY));

    Schema tupleSchema = new Schema(fields);

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("gts", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }

}
