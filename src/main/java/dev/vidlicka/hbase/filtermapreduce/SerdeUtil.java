package dev.vidlicka.hbase.filtermapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Contains helper methods for serialization and deserialization of Java objects.
 *
 * CAUTION: the purpose was to be as simple and straightforward as possible performance, safety and
 * portability was not a concern
 */
public final class SerdeUtil {

  private SerdeUtil() {}

  /**
   * Serialize an object to a byte array
   *
   * @param <T> type of object to be serialize
   * @param value object to be serialized
   * @return object serialized to bytes
   * @throws IOException
   */
  public static <T> byte[] serialize(T value) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(byteStream);
    out.writeObject(value);
    out.close();
    return byteStream.toByteArray();
  }

  /**
   * Generic object deserialization
   *
   * @param <T> type of object to be deserialized
   * @param bytes byte array to be deserialized into an object of type T
   * @return deserialized object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
    @SuppressWarnings("unchecked")
    T deserialized = (T) in.readObject();
    in.close();
    return deserialized;
  }
}
