package io.warp10.pig;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SelectGTS extends EvalFunc<DataBag> {
  
  private final List<Map<String,Pattern>> patterns;
  
  public SelectGTS(String... selectors) throws IOException {
    try {
      this.patterns = new ArrayList<Map<String,Pattern>>(selectors.length);
      
      for (String selector: selectors) {
        patterns.add(GTSHelper.patternsFromSelectors(selector));
      }
      
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
  
  @Override
  public DataBag exec(Tuple input) throws IOException {
    if (1 != input.size()) {
      throw new IOException("Invalid input, expecting a Bag of GTS instance.");
    }
    
    if (!(input.get(0) instanceof DataBag) && !(input.get(0) instanceof DataByteArray)) {
      throw new IOException("Invalid input, expecting a Bag of GTS instance.");
    }

    DataBag inbag = null;
    
    if (input.get(0) instanceof DataBag) {
      inbag = (DataBag) input.get(0);
    } else {
      inbag = new DefaultDataBag();
      inbag.add(input);
    }
    
    DataBag outbag = new DefaultDataBag();

    TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

    Iterator<Tuple> iter = inbag.iterator();

    while(iter.hasNext()) {
      Tuple t = iter.next();

      //
      // GTSWrapper
      //

      byte[] wrapperData = ((DataByteArray) t.get(0)).get();

      //
      // Deserialize metadata
      //

      GTSWrapper wrapper = new GTSWrapper();

      try {
        deser.deserialize(wrapper, wrapperData);
      } catch (TException te) {
        throw new IOException(te);
      }

      boolean selected = false;
      
      for (Map<String,Pattern> pattern: this.patterns) {
        Pattern classPattern = pattern.get(null);
        
        String className = wrapper.getMetadata().getName();
        
        Matcher m = classPattern.matcher(className);

        boolean ok = false;

        if (!m.matches()) {
          continue;
        }
      
        Map<String,String> labels = wrapper.getMetadata().getLabels();
        Map<String,String> attributes = wrapper.getMetadata().getAttributes();

        if (null == labels) {
          labels = new HashMap<String, String>();
        }
        
        if (null == attributes) {
          attributes = new HashMap<String,String>();
        }
        
        ok = true;
        
        for (Entry<String,Pattern> entry: pattern.entrySet()) {
          if (null == entry.getKey()) {
            continue;
          }
          
          // If the GTS does not contain the given label or attribute, exit
          if (labels.containsKey(entry.getKey())) {
            m = entry.getValue().matcher(labels.get(entry.getKey()));
            if (!m.matches()) {
              ok = false;
              break;
            }
          } else if (attributes.containsKey(entry.getKey())) {
            m = entry.getValue().matcher(attributes.get(entry.getKey()));
            if (!m.matches()) {
              ok = false;
              break;
            }
          } else {
            ok = false;
            break;
          }
        }
        
        if (ok) {
          selected = true;
          break;
        }        
      }
      
      if (selected) {
        outbag.add(t);
      }
    }

    return outbag;
  }
  
  @Override
  public Schema outputSchema(Schema input) {
    List<Schema.FieldSchema> fields = new ArrayList<Schema.FieldSchema>();

    fields.add(new Schema.FieldSchema("gts", DataType.BYTEARRAY));

    Schema tupleSchema = new Schema(fields);

    Schema bagSchema = new Schema(tupleSchema);

    Schema.FieldSchema outputBag = new Schema.FieldSchema("bag", DataType.BAG);

    outputBag.schema = bagSchema;

    return new Schema(outputBag);

  }
}
