package io.warp10.pig;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

public class BytesOutputFormat extends FileOutputFormat<BytesWritable,BytesWritable> {
  
  public static class BytesRecordWriter extends RecordWriter<BytesWritable, BytesWritable> {    
    private final DataOutputStream out;
    
    public BytesRecordWriter(DataOutputStream out) {
      this.out = out;
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      this.out.close();
    }
    
    @Override
    public void write(BytesWritable key, BytesWritable value) throws IOException, InterruptedException {
      out.write(value.getBytes(), 0, value.getLength());
    }
  }
  
  @Override
  public RecordWriter<BytesWritable, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    String extension = "";
    Path file = getDefaultWorkFile(context, extension);
    FileSystem fs = file.getFileSystem(context.getConfiguration());
    FSDataOutputStream fileOut = fs.create(file, false);
    return new BytesRecordWriter(fileOut);
  }
}
