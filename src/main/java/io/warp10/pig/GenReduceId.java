package io.warp10.pig;

import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStack;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
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
import java.util.Properties;

/**
 * Class used to get partitions and gen reduce id
 * Input : a  bag with all GTS (light : without ticks)
 */
public class GenReduceId extends EvalFunc<DataBag> {

  private final TupleFactory tupleFactory = TupleFactory.getInstance();

  /**
   * List of labels used for equiv classes
   */
  private List<String> equivClassLabels = null;


  public GenReduceId(String... args) {
    this(args[0]);
  }

  /**
   *
   * @param equivClassLabelsStr : list of used for equiv classes separated by ',' (foo,test)
   */
  protected GenReduceId(String equivClassLabelsStr) {

    /**
     * Split
     * FIXME : How to be sure that ',' has been escaped ?
     */
    String equivStr = StringUtils.trim(equivClassLabelsStr);

    this.equivClassLabels = Lists
        .newArrayList(Splitter.on(',').trimResults().split(equivStr));

  }

  /**
   *
   * @param input : databag with all lightened GTS (without ticks !)
   * @return DataBag
   * @throws java.io.IOException
   */
  @Override
  public DataBag exec(Tuple input) throws IOException {

    if (input == null || input.size() < 1) {
      System.err.println("Waiting for 1 parameter : 1 tuple with one Bag.");
      return null;
    }

    //
    // Stack - Force No_LIMIT (batch mode)
    //

    WarpScriptStack stack = new MemoryWarpScriptStack(null, null, new Properties());

    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_OPS, Long.MAX_VALUE);
    stack.setAttribute(WarpScriptStack.ATTRIBUTE_MAX_DEPTH, Integer.MAX_VALUE);

    //
    // bag : List of GTS ie Tuple with 3 fields (classIdForJoin, labelsIdForJoin, encoded)
    //

    DataBag groupBag = (DataBag) input.get(0);

    Iterator<Tuple> iter = groupBag.iterator();

    List<GeoTimeSerie> listOfLightGts = new ArrayList<>();

    //
    // Create the bag in response
    //

    DataBag resultBag = new DefaultDataBag();

    //
    // Get the GTS from the input bag
    //
    while(iter.hasNext()) {

      Tuple t  = iter.next();

      //
      // Get encoded : 3rd field
      //

      DataByteArray serialized = (DataByteArray) t.get(2);

      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

      GTSWrapper gtsWrapper = new GTSWrapper();

      try {
        deserializer.deserialize(gtsWrapper, serialized.get());
      } catch (TException te) {
        throw new IOException(te);
      }

      if (null == gtsWrapper.getMetadata()) {
        gtsWrapper.setMetadata(new Metadata());
      }

      listOfLightGts.add(GTSWrapperHelper.fromGTSWrapperToGTS(gtsWrapper));

    }

    java.util.Map<java.util.Map<String,String>, List<GeoTimeSerie>> partitions = GTSHelper.partition(listOfLightGts, equivClassLabels);

    String reduceId = null;

    //
    // Iterate on the list of partitions
    //

    for (java.util.Map<String,String> partitionLabels: partitions.keySet()) {

      List<GeoTimeSerie> partitionSeries = partitions.get(partitionLabels);

      //
      // partitionLabels : common labels = one partition
      // partitionSeries : list of GTS related to this partition
      //

      //
      // Compute reduce id on these common labels
      //

      try {

        stack.push(partitionLabels);

        stack.exec("MAPID ");

        reduceId = (String) stack.pop();

        //
        // Iterate on the list of partitionSeries to get all GTS with classIdForJoin + labelsIdForJoin
        //

        for (GeoTimeSerie gtsPart : partitionSeries) {

          Long classIdForJoin = Long.parseLong(gtsPart.getLabel("classIdForJoin"));
          Long labelsIdForJoin = Long.parseLong(gtsPart.getLabel("labelsIdForJoin"));

          //
          // return classIdForJoin, labelsIdForJoin and reduceId for the current GTS
          //

          Tuple tupleResult = TupleFactory.getInstance().newTuple(3);
          tupleResult.set(0, classIdForJoin);
          tupleResult.set(1, labelsIdForJoin);
          tupleResult.set(2, reduceId);

          resultBag.add(tupleResult);

        }

      } catch (WarpScriptException e) {
        throw new IOException(e);
      }

    }

    //
    // {(classIdForJoin,labelsIdForJoin,reduceId)+
    //

    return resultBag;

  }

  @Override
  public Schema outputSchema(Schema input) {

    //
    // ClassId, labelsId, reduceId
    //

    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("classIdForJoin", DataType.LONG));
    fields.add(new Schema.FieldSchema("labelsIdForJoin", DataType.LONG));
    fields.add(new Schema.FieldSchema("reduceId", DataType.CHARARRAY));

    Schema tupleSchema = new Schema(fields);

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("partitions", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }

}