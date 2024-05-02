package io.inlined.clients;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

public interface InlineKVReader {
  void startupReader() throws RuntimeException;

  void shutdownReader() throws RuntimeException;

  @Nullable
  byte[] getBytesValue(Object primaryKey, String fieldName);

  // Iterator on field values (returned bytes are null if particular value is not present for
  // primary-key).
  // Returned values are in (primary-key, field) order, i.e. -
  // [key0#field0][key0#field1]..[key0#fieldN][key1#field0][key1#field1]...[keyN#fieldN]
  Iterator<byte[]> multiGetBytesValues(List<Object> primaryKeys, List<String> fieldNames);

  @Nullable
  String getStringValue(Object primaryKey, String fieldName);

  @Nullable
  Integer getIntValue(Object primaryKey, String fieldName);

  @Nullable
  Long getLongValue(Object primaryKey, String fieldName);

  @Nullable
  Float getFloatValue(Object primaryKey, String fieldName);

  @Nullable
  Double getDoubleValue(Object primaryKey, String fieldName);
}
