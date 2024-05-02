package io.inlined.benchmarks.clients;

import com.google.common.collect.Lists;
import io.inlined.benchmarks.DBClient;
import io.inlined.clients.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class IKVSingleGetDBClient implements DBClient {
  private final InlineKVReader _reader;
  private final InlineKVWriter _writer;
  private final String _primaryKeyFieldName;

  public IKVSingleGetDBClient(
      boolean direct,
      String accountId,
      String accountPassKey,
      String storeName,
      String primaryKeyFieldName,
      String mountDir) {
    ClientOptions writerClientOptions =
        new ClientOptions.Builder()
            .withAccountId(accountId)
            .withAccountPassKey(accountPassKey)
            .withStoreName(storeName)
            .build();

    ClientOptions readerClientOptions =
        new ClientOptions.Builder()
            .withMountDirectory(mountDir)
            .withStoreName(storeName)
            .withAccountId(accountId)
            .withAccountPassKey(accountPassKey)
            .useBytesPrimaryKey()
            .build();

    IKVClientFactory factory = new IKVClientFactory();

    if (direct) {
      DirectJNIBenchmarkingClient client =
          factory.createDirectBenchmarkingClient(readerClientOptions);
      _reader = client;
      _writer = client;
    } else {
      _reader = factory.createNewReaderInstance(readerClientOptions);
      _writer = factory.createNewWriterInstance(writerClientOptions);
    }

    _primaryKeyFieldName = primaryKeyFieldName;
  }

  @Override
  public void startup() {
    _writer.startupWriter();
    _reader.startupReader();
  }

  @Override
  public void shutdown() {
    _writer.shutdownWriter();
    _reader.shutdownReader();
  }

  @Override
  public byte[] getValue(byte[] key, String fieldName, byte[] fieldNameUtf8Bytes) {
    return _reader.getBytesValue(key, fieldName);
  }

  @Override
  public Iterator<byte[]> multiGetValueForKeys(
      Iterator<byte[]> keys, String fieldName, byte[] fieldNameUtf8Bytes) {
    return _reader.multiGetBytesValues(
        Lists.newArrayList(keys), Collections.singletonList(fieldName));
  }

  @Override
  public void setValues(byte[] key, Map<String, byte[]> fieldValues) {
    IKVDocument.Builder builder =
        new IKVDocument.Builder().putBytesField(_primaryKeyFieldName, key); // init with pkey value

    for (Map.Entry<String, byte[]> entry : fieldValues.entrySet()) {
      builder.putBytesField(entry.getKey(), entry.getValue());
    }

    _writer.upsertFieldValues(builder.build());
  }

  @Override
  public void flushValues() {
    if (_writer instanceof DirectJNIBenchmarkingClient client) {
      client.flushWrites();
    }
  }
}
