package io.warp10.pig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by dav on 22/02/15.
 */
public class TestHash {
  public static void main(String[] args) throws IOException {
    test(new LinkedList<String>());
    test(new ArrayList<String>());
  }

  public static void test(List<String> list) throws IOException {
    for (int i = 0; i < 10; i++) {
      list.add("hello world");
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(list);
    out.close();
    System.out.println(list.getClass().getSimpleName() +
        " used " + baos.toByteArray().length + " bytes");
  }

}
