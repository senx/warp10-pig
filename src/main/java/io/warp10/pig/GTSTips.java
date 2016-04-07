package io.warp10.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;

/**
 * Extract 'tips' from a GTSWrapper
 * 
 * A 'tip' is a chunk of GTSWrapper which covers either the head or tail of
 * a given GTSWrapper.
 * 
 * A tip is defined by a timespan.
 *
 * This function operates on GTSWrapper instances (packed as DataByteArray)
 * 
 * It produces a DataBag with 3 tuples
 * 
 * (1,timeCell,Prefix_GTSWrapper)
 * (2,timeCell,Suffix_GTSWrapper)
 * (0,previousTimeCell1,previousTimeCell2,nextTimeCell1,nextTimeCell2,Input_GTSWrapper)
 * 
 * 'timeCell' is the time interval the first timestamp of the sub GTSWrapper belongs to. It is computed
 * by taking the first timestamp and dividing it by the timespan.
 * 
 * '{previous,next}TimeCell{1,2}' are the previous and next timescells of the input GTSWrapper.
 * The previous time cells are the time interval of the firsttick - 1 and firsttick - timespan.
 * The next time cells are the time interval of lasttick + 1 and lasttick + timespan.
 * 
 * The first field of the output tuples determine if the tuple is a 'head', a 'tail' or an entire 'GTSWrapper'
 *
 * The following PIG macro takes a relation with GTSWrapper elements as input and outputs the result of adding heads/tails to them.
 *
 
DEFINE LeptonOverlap(timespan, IN) RETURNS GTS {
  --
  -- Generate tails and heads
  --
  TAILS_AND_HEADS = FOREACH $IN GENERATE FLATTEN(GTSTips($timespan,$0));

  --
  -- Split tails, heads and GTS
  --
  SPLIT TAILS_AND_HEADS INTO HEADS_SPLIT IF $0 == 1, TAILS_SPLIT IF $0 == 2, GTS_SPLIT IF $0 == 0;

  --
  -- Clean heads/tails/GTS
  --
  HEADS = FOREACH HEADS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS cell, $4 AS gts;
  TAILS = FOREACH TAILS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS cell, $4 AS gts;
  GTS = FOREACH GTS_SPLIT GENERATE $1 AS classId, $2 AS labelsId, $3 AS prefixCell1, $4 AS prefixCell2, $5 AS suffixCell1, $6 AS suffixCell2, $7 AS gts;

  --
  -- Join heads with GTS
  --

  SUFFIXED_1 = JOIN GTS BY (classId, labelsId, suffixCell1) LEFT OUTER, HEADS BY (classId, labelsId, cell);
  SUFFIXED = JOIN SUFFIXED_1 BY (GTS::classId, GTS::labelsId, suffixCell2) LEFT OUTER, HEADS BY (classId, labelsId, cell);

  --
  -- Join tails with GTS
  --
  PREFIXED_1 = JOIN SUFFIXED BY (GTS::classId, GTS::labelsId, prefixCell1) LEFT OUTER, TAILS BY (classId, labelsId, cell);
  PREFIXED = JOIN PREFIXED_1 BY (GTS::classId, GTS::labelsId, prefixCell2) LEFT OUTER, TAILS BY (classId, labelsId, cell);
  
  --
  -- Fuse head + tail + GTS
  --
  $GTS = FOREACH PREFIXED GENERATE FLATTEN(Fuse(TOBAG($6,$10,$14,$18,$22)));
}

 */
public class GTSTips extends EvalFunc<DataBag> {
  
  private static final long[] SIPKEYS = { 0x7583A1A598F24BC1L, 0x93F818FDF11FAD6FL };
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    // Create the target databag
    DataBag bag = new DefaultDataBag();
    
    // Iterate over the input Tuple, the first element MUST be the timespan
    
    Iterator<Object> iter = input.iterator();
    
    Object ots = iter.next();
    
    if (!(ots instanceof Number)) {
      throw new IOException("Expecting a timespan as the first element of the input tuple.");
    }
    
    long timespan = ((Number) ots).longValue();
    
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
    
    while(iter.hasNext()) {
      Object o = iter.next();
      
      if (!(o instanceof DataByteArray)) {
        throw new IOException("Can only operate on binary fields representing Lepton Geo Time Series.");
      }
      
      // Create GTSDecoder
      GTSWrapper wrapper = new GTSWrapper();
      
      DataByteArray bytes = (DataByteArray) o;
      
      try {
        deserializer.deserialize(wrapper, bytes.get());                
      } catch (TException te) {
        throw new IOException(te);
      }
      
      //
      // Extract head of the GTSWrapper [firsttick,firsttick+timespan[
      //
      
      GTSEncoder head = new GTSEncoder(0L);
      ByteBuffer bb = wrapper.bufferForEncoded();
      int position = bb.position();
      
      GTSDecoder decoder = new GTSDecoder(wrapper.getBase(), bb);
      
      boolean first = true;
      long lasttick = 0L;
      long firsttick = 0L;
      long total = 0;
      
      // Loop over the ticks
      while(decoder.next()) {
        long tick = decoder.getTimestamp();
        
        if (!first && tick < lasttick) {
          throw new IOException("Lepton Geo Time Series is expected to be in chronological order, encountered tick " + tick + " after " + lasttick);
        }
        
        if (first) {
          first = false;
          firsttick = tick;
        }

        if (tick < firsttick + timespan) {
          head.addValue(tick, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
        }
        
        lasttick = tick;
        total++;
      }
      
      //
      // Now extract the tail, since we know what 'lasttick' is this is easy, we just
      // have to rescan the decoder
      //
      
      bb.position(position);
      decoder = new GTSDecoder(wrapper.getBase(), bb);
      
      GTSEncoder tail = new GTSEncoder();
      
      while(decoder.next()) {
        long tick = decoder.getTimestamp();
        
        if (tick > (lasttick - timespan)) {
          tail.addValue(tick, decoder.getLocation(), decoder.getElevation(), decoder.getValue());
        }
      }
      
      head.setMetadata(wrapper.getMetadata());
      tail.setMetadata(wrapper.getMetadata());
      
      long classId = GTSHelper.classId(SIPKEYS, wrapper.getMetadata().isSetName() ? wrapper.getMetadata().getName() : "");
      long labelsId = GTSHelper.labelsId(SIPKEYS, wrapper.getMetadata().getLabelsSize() > 0 ? wrapper.getMetadata().getLabels() : new HashMap<String, String>());
      
      //
      // Create the output Tuples to store in the bag
      //
      
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      
      //
      // Store head and tail only if they cover less than the entire input GTS
      //
      
      if (total != head.getCount()) {
        Tuple headTuple = TupleFactory.getInstance().newTuple(5);
        
        GTSWrapper headWrapper = new GTSWrapper();
        headWrapper.setMetadata(head.getMetadata());
        headWrapper.setBase(head.getBaseTimestamp());
        headWrapper.setCount(head.getCount());
        headWrapper.setEncoded(head.getBytes());
        
        headTuple.set(0, 1);
        headTuple.set(1, classId);
        headTuple.set(2, labelsId);
        headTuple.set(3, firsttick / timespan);

        try {
          byte[] ser = serializer.serialize(headWrapper);      
          headTuple.set(4, new DataByteArray(ser));
        } catch (TException te) {
          throw new IOException(te);
        }

        bag.add(headTuple);
      }

      if (total != tail.getCount()) {
        Tuple tailTuple = TupleFactory.getInstance().newTuple(5);
        
        GTSWrapper tailWrapper = new GTSWrapper();
        tailWrapper.setMetadata(tail.getMetadata());
        tailWrapper.setBase(tail.getBaseTimestamp());
        tailWrapper.setCount(tail.getCount());
        tailWrapper.setEncoded(tail.getBytes());
              
        tailTuple.set(0, 2);
        tailTuple.set(1, classId);
        tailTuple.set(2, labelsId);
        tailTuple.set(3, lasttick / timespan);
        
        try {
          byte[] ser = serializer.serialize(tailWrapper);
          tailTuple.set(4, new DataByteArray(ser));        
        } catch (TException te) {
          throw new IOException(te);
        }
        
        bag.add(tailTuple);        
      }
      
      Tuple gtsTuple = TupleFactory.getInstance().newTuple(8);
      
      gtsTuple.set(0, 0);
      gtsTuple.set(1, classId);
      gtsTuple.set(2, labelsId);
      long cell1 = (firsttick - 1) / timespan;
      gtsTuple.set(3, cell1);
      long cell2 = (firsttick - timespan) / timespan;
      gtsTuple.set(4, cell1 != cell2 ? cell2 : null);
      cell1 = (lasttick + 1) / timespan;
      gtsTuple.set(5, cell1);
      cell2 = (lasttick + timespan) / timespan; 
      gtsTuple.set(6, cell1 != cell2 ? cell2 : null);
      gtsTuple.set(7, bytes);
      
      bag.add(gtsTuple);
    }
    
    return bag;
  }
}
