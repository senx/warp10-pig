package io.warp10.pig;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
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
import java.util.*;

/**
 * Use Warpscript to chunk GTS
 * @deprecated
 */
public class Chunk extends EvalFunc<DataBag> {

  //
  // KeyStore
  //

  private static final KeyStore keyStore = new DummyKeyStore();

  // FIXME : must be set in a properties file !
  private final String classLabelsKey = "hex:75cfe99cc78e17dc98115b0015581f65";

  //
  // GTS id label name - used to merge chunks for non reduce/apply fcts like map,
  //
  private static final String gtsIdLabelName = "gtsId";

  //
  // classId used to join GTS to get the reduceId (normal and light mode)
  //
  private static final String classIdForJoinName = "classIdForJoin";

  //
  // labelsId used to join GTS to get the reduceId (normal and light mode)
  //
  private static final String labelsIdForJoinName = "labelsIdForJoin";

  //
  // Chunk parameters
  //

  private long clipFrom = Long.MIN_VALUE;
  private long clipTo = Long.MIN_VALUE;
  private long chunkwidth = Long.MIN_VALUE;

  //
  // Mode (chunk or timeclip)
  // chunk mode by default
  //
  private static enum MODE {
    CHUNK, TIMECLIP
  }

  private MODE mode = MODE.CHUNK;

  /**
   *
   * @param args with at least 3 parameters: chunk mode ('chunk',clipFrom,clipTo,chunkwidth) / timeclip mode ('timeclip',clipFrom,clipTo)
   * @throws IOException
   */
  public Chunk(String... args) throws IOException {
    if (args.length < 1) {
      throw new IOException("At least 1 parameter (mode) required: chunk mode: ('chunk', [clipFrom], [clipTo], [chunkwidth]) timeclip mode: ('timeclip', [clipFrom], [clipTo])");
    }

    // FIXME: add keepme parameter to accept empty gts (default: false)

    if ("chunk".equalsIgnoreCase(args[0])) {
      if (4 == args.length) {
        //
        // static parameters
        //
        clipFrom = Long.parseLong(args[1]);
        clipTo = Long.parseLong(args[2]);
        chunkwidth = Long.parseLong(args[3]);
      }
      this.mode = MODE.CHUNK;
    } else if ("timeclip".equalsIgnoreCase(args[0])) {
      if (3 == args.length) {
        //
        // static parameters
        //
        clipFrom = Long.parseLong(args[1]);
        clipTo = Long.parseLong(args[2]);
      }
      this.mode = MODE.TIMECLIP;
    } else {
      throw new IOException("Unknown mode " + args[0] + " - mode must be in ('chunk', 'timeclip')");
    }
  }

  /**
   * Chunk a GTS
   *
   * @param input one GTS (encoded) and dynamic parameters if not set during init (clipFrom, clipTo, ...)
   * @return Bag with n Tuples. Cf Schema
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (null == input || input.size() != 1) {
      throw new IOException("Invalid input : one bag with one GTS required. {(gts::encoded)}");
    }

    if (!(input.get(0) instanceof DataBag)) {
      throw new IOException("Invalid input : one bag with at least one GTS required. {(gts::encoded)}");
    }

    DataBag inputBag = (DataBag) input.get(0);

    Iterator<Tuple> iter = inputBag.iterator();

    //
    // Bag with n tuple(s) of GTS or chunks
    //

    DataBag resultBag = new DefaultDataBag();

    while (iter.hasNext()) {

      reporter.progress();

      Tuple t = (Tuple) iter.next();

      DataByteArray encoded = (DataByteArray) t.get(0);

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

      TDeserializer deserializer = new TDeserializer(
          new TCompactProtocol.Factory());

      try {

        //
        // Instanciate a GTSWrapper from its encoded value
        //

        GTSWrapper gtsWrapper = new GTSWrapper();

        deserializer
            .deserialize(gtsWrapper, encoded.get());

        String className = gtsWrapper.getMetadata().getName();

        Map labels = gtsWrapper.getMetadata().getLabels();

        //
        // gtsId used to merge chunks for non reduce or apply fcts (map, ..)
        //

        String gtsId = java.util.UUID.randomUUID().toString();

        //
        // Register these ids as labels in the current GTS
        //

        Metadata metadata = gtsWrapper.getMetadata();

        metadata.putToLabels(gtsIdLabelName, gtsId);

        long classIdForJoin = Long.MAX_VALUE;

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

        long labelsIdForJoin = Long.MAX_VALUE;
        if (null != labels) {
          labelsIdForJoin = GTSHelper
              .labelsId(keyStore.decodeKey(classLabelsKey),
                  gtsWrapper.getMetadata().getLabels());
          metadata.putToLabels(labelsIdForJoinName,
              String.valueOf(labelsIdForJoin));
        } else {
          labelsIdForJoin = GTSHelper
              .classId(keyStore.decodeKey(classLabelsKey),
                  java.util.UUID.randomUUID().toString());
        }

        gtsWrapper.setMetadata(metadata);

        List<GTSWrapper> chunks = null;

        if (this.mode == MODE.CHUNK) {
          //
          // If conf has not been provided during init, get it here (dynamic mode) !
          //
          if (clipFrom == Long.MIN_VALUE) {
            clipFrom = (Long)t.get(1);
          }
          if (clipTo == Long.MIN_VALUE) {
            clipTo = (Long)t.get(2);
          }
          if (chunkwidth == Long.MIN_VALUE) {
            chunkwidth = (Long)t.get(3);
          }
          chunks = WarpScriptUtils
              .chunk(gtsWrapper, clipFrom, clipTo, chunkwidth);
        } else if (this.mode == MODE.TIMECLIP) {
          //
          // If conf has not been provided during init, get it here (dynamic mode) !
          //
          if (clipFrom == Long.MIN_VALUE) {
            clipFrom = (Long)t.get(1);
          }
          if (clipTo == Long.MIN_VALUE) {
            clipTo = (Long)t.get(2);
          }
          //
          // timeclip generates only one GTS
          //
          chunks = new ArrayList<>();

          chunks.add(GTSWrapperHelper.clip(gtsWrapper, clipFrom, clipTo));
        }

        //System.out.println("nb chunks : " + chunks.size());

        for (GTSWrapper chunk : chunks) {

          reporter.progress();

          //System.out.println("chunk size : " + chunk.getCount());

          if (chunk.getCount() > 0) {

            Tuple resultTuple = null;
            if (this.mode == MODE.CHUNK) {
              resultTuple = TupleFactory.getInstance().newTuple(7);
            } else {
              resultTuple = TupleFactory.getInstance().newTuple(6);
            }

            Metadata metadataChunk = new Metadata(
                gtsWrapper.getMetadata());

            if (this.mode == MODE.CHUNK) {
              //
              // Get chunk id and put it in chunk Metadata
              //

              String chunkId = chunk.getMetadata().getLabels()
                  .get(WarpScriptUtils.chunkIdLabelName);
              metadataChunk
                  .putToLabels(WarpScriptUtils.chunkIdLabelName, chunkId);
            }

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

            resultTuple.set(2, gtsId);

            resultTuple.set(3, classIdForJoin);

            resultTuple.set(4, labelsIdForJoin);

            if (this.mode == MODE.CHUNK) {
              resultTuple
                  .set(5, chunk.getMetadata().getLabels()
                      .get(WarpScriptUtils.chunkIdLabelName));
              resultTuple.set(6, chunkData);
            } else {
              // TIMECLIP
              resultTuple.set(5, chunkData);
            }

            resultBag.add(resultTuple);
          }
        }

        } catch (TException te) {
          throw new IOException(te);
        }
    }

    return resultBag;
  }

  @Override
  public Schema outputSchema(Schema input) {

     List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

     fields.add(new Schema.FieldSchema("className", DataType.CHARARRAY));
     fields.add(new Schema.FieldSchema("labels", DataType.MAP));
     fields.add(new Schema.FieldSchema(gtsIdLabelName, DataType.CHARARRAY));
     fields.add(new Schema.FieldSchema(classIdForJoinName, DataType.LONG));
     fields.add(new Schema.FieldSchema(labelsIdForJoinName, DataType.LONG));
     if (this.mode == MODE.CHUNK) {
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
