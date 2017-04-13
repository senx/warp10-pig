package io.warp10.pig.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

/**
 * Reads a list of files as input and creates a SequenceFile with the
 * content of those files.
 * 
 * If a line contains one path, the key will be the basename of the file and the value its content.
 * If a line contains two pathes, the key will be the content of the first file and the value that of the second.
 */
public class Files2SequenceFile extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    //
    // Open file list
    //
    
    InputStream in = null;
    
    if ("-".equals(args[0])) {
      in = System.in;
    } else {
      in = new FileInputStream(args[0]);
    }
    
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    
    //
    // Create SequenceFile
    //
    
    //
    // Open output SequenceFile
    //
    
    Configuration conf = getConf();
    
    //
    // Open output file
    //
    
    FSDataOutputStream out = null;
    
    if ("-".equals(args[1])) {
      out = new FSDataOutputStream(System.out, null);
    }

    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.compression(CompressionType.BLOCK, new DefaultCodec()),
      SequenceFile.Writer.keyClass(BytesWritable.class),
      SequenceFile.Writer.valueClass(BytesWritable.class),
      null == out ? SequenceFile.Writer.file(new Path(args[args.length - 1])) : SequenceFile.Writer.stream(out));

    //
    // Loop over the input
    //
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    
    long count = 0;
    long bytes = 0;
    
    long gnano = System.nanoTime();
    
    while(true) {
      
      long nano = System.nanoTime();
      
      String line = br.readLine();
      if (null == line) {
        break;
      }
      
      String[] tokens = line.trim().split("\\s");
      
      if (tokens.length < 1 || tokens.length > 2) {
        throw new RuntimeException("Invalid input: " + line);
      }
      
      byte[] key = null;
      
      String file;
      
      if (1 == tokens.length) {
        key = new File(tokens[0]).getAbsoluteFile().getName().getBytes("UTF-8");
        file = tokens[0];
        System.out.print("Converting " + tokens[0] + "... ");
      } else {
        baos.reset();
        InputStream fin = new FileInputStream(tokens[0]);
        while(true) {
          int len = fin.read(buf);
          if (len < 0) {
            break;
          }
          baos.write(buf, 0, len);
        }
        fin.close();
        key = baos.toByteArray();
        file = tokens[1];
        System.out.print("Converting " + tokens[0] + " " + tokens[1] + "... ");
      }
      
      //
      // Read value
      //
      
      baos.reset();
      InputStream fin = new FileInputStream(file);
      while(true) {
        int len = fin.read(buf);
        if (len < 0) {
          break;
        }
        baos.write(buf, 0, len);
      }
      fin.close();

      long size = key.length + baos.size();
      
      writer.append(new BytesWritable(key), new BytesWritable(baos.toByteArray()));
      nano = System.nanoTime() - nano;
      System.out.println(size + " bytes in " + (nano / 1000000.0D) + " ms");

      bytes += size;
      count++;
    }

    in.close();
    writer.close();
    
    gnano = System.nanoTime() - gnano;
    
    System.out.println("TOTAL: " + count + " entries (" + bytes + " bytes) in " + (gnano / 1000000.0D) + " ms.");
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new RuntimeException("Usage: Jar2SequenceFile [options] jarfile seqfile");
    }
    
    int exitCode = ToolRunner.run(new Files2SequenceFile(), args);
  }
}
