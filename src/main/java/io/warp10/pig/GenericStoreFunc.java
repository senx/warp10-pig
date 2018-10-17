package io.warp10.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import io.warp10.hadoop.WritableUtils;
import io.warp10.pig.utils.PigUtils;

public class GenericStoreFunc extends StoreFunc {

  private static final String PIG_GENERICSTORE_OUTPUTFORMAT = "pig.genericstore.outputformat";
  private static final String PIG_GENERICSTORE_CONF_SUFFIX = "pig.genericstore.conf.suffix";
  
  private RecordWriter writer = null;

  private Job job = null;
  private final String suffix;
  
  static {
    PigWarpConfig.ensureConfig();
  }

  public GenericStoreFunc() {
    this.suffix = "";
  }
  
  public GenericStoreFunc(String suffix) {
    if (null != suffix) {
      if (!"".equals(suffix)) {
        this.suffix = "." + suffix;
      } else {
        this.suffix = "";
      }
    } else {
      this.suffix = "";
    }
  }
  
  @Override
  public OutputFormat getOutputFormat() throws IOException {
    //
    // Retrieve the class of the InputFormat we should create
    //
    
    Configuration conf = this.job.getConfiguration();
    
    String confsfx = conf.get(PIG_GENERICSTORE_CONF_SUFFIX + this.suffix, "");
    
    if (!"".equals(confsfx)) {
      confsfx = "." + confsfx;
      List<Entry<String,String>> keys = new ArrayList<Entry<String,String>>();
      Iterator<Entry<String,String>> iter = conf.iterator();
      while(iter.hasNext()) {
        Entry<String,String> entry = iter.next();
        
        if (entry.getKey().endsWith(confsfx)) {
          keys.add(entry);
        }
      }
      
      // Override or create the unsuffixed configuration parameters
      for (Entry<String,String> entry: keys) {
        String key = entry.getKey().substring(0, entry.getKey().length() - confsfx.length());
        conf.set(key, entry.getValue());
      }
    }

    String outputFormat = conf.get(PIG_GENERICSTORE_OUTPUTFORMAT + this.suffix);
        
    if (null == outputFormat) {
      outputFormat = conf.get(PIG_GENERICSTORE_OUTPUTFORMAT);
    }
    
    try {
      Class ofclass = Class.forName(outputFormat);
      OutputFormat format = (OutputFormat) ofclass.newInstance();

      return format;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
  
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }
  
  @Override
  public void putNext(Tuple t) throws IOException {
    
    if (t.size() != 2) {
      throw new IOException("Invalid tuple size, expected 2, was " + t.size());
    }
    
    try {
      Object key = WritableUtils.toWritable(PigUtils.fromPig(t.get(0), true));
      Object value = WritableUtils.toWritable(PigUtils.fromPig(t.get(1), true));

      this.writer.write(key, value);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }
  
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    this.job = job;
  }
}
