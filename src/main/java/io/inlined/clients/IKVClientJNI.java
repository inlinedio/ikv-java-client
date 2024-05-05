package io.inlined.clients;

import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;

public final class IKVClientJNI extends ClassLoader {
  // can throw UnsatisfiedLinkError for a wrong path or incompatible binary
  private IKVClientJNI(String pathToNativeBinary) {
    System.load(pathToNativeBinary);
  }

  public static IKVClientJNI createNew(String mountDirectory) throws IOException {
    NativeBinaryManager nativeBinaryManager = new NativeBinaryManager(mountDirectory);
    try {
      Optional<String> maybePath = nativeBinaryManager.getPathToNativeBinary();
      if (maybePath.isEmpty()) {
        throw new IOException(
            "Could not find a native binary for reader instance, for this os/platform");
      }

      return new IKVClientJNI(maybePath.get());
    } finally {
      nativeBinaryManager.close();
    }
  }

  public static void main(String[] args) {
    // /home/ubuntu/ikv-store/ikv/target/release/libikv.so
    // /Users/pushkar/projects/ikv-store/ikv/target/release/libikv.dylib
    IKVClientJNI ikvClientJNI = new IKVClientJNI(args[0]);
    System.out.println(ikvClientJNI.provideHelloWorld());
  }

  /** For linkage testing. */
  public native String provideHelloWorld();

  // Open or create.
  // config: Serialized IKVStoreConfig.proto
  // RuntimeException: opening errors.
  public native long open(byte[] config) throws RuntimeException;

  public native void close(long indexHandle) throws RuntimeException;

  @Nullable
  public native byte[] readField(long indexHandle, byte[] primaryKey, String fieldName);

  // size prefixed (lower endian int) and concatenated
  // primary keys and field names (utf8 encoding)
  public native byte[] batchReadFields(long indexHandle, byte[] primaryKeys, byte[] fieldNames);

  /** Writer (to IKV data plane) methods */
  public native long openWriter(byte[] config) throws RuntimeException;

  public native void closeWriter(long handle) throws RuntimeException;

  public native void singlePartitionWrite(
      long handle, byte[] partitioningKeyAsFieldValue, byte[] ikvDataEvent) throws RuntimeException;

  public native void broadcastWrite(long handle, byte[] ikvDataEvent) throws RuntimeException;

  /** Write method - only for benchmarking. */
  public native void directWriteIKVDataEvent(long indexHandle, byte[] ikvDataEvent)
      throws RuntimeException;

  /** Flush writes method - only for benchmarking. */
  public native void flushWrites(long indexHandle) throws RuntimeException;

  /**
   * Hook to build index by consuming nearline event stream. Index is built in-place of existing
   * base index present in the mount directory.
   */
  public native void buildIndex(byte[] config) throws RuntimeException;
}
