package io.inlined.clients;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.inlineio.schemas.Common;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.annotation.Nullable;

public class DefaultInlineKVReader implements InlineKVReader {
  private static final long UNINITIALIZED_HANDLE = -1;
  private final ClientOptions _clientOptions;
  private final Common.IKVStoreConfig _clientServerMergedConfig;
  private volatile long _handle;
  private volatile IKVClientJNI _ikvClientJni;

  public DefaultInlineKVReader(
      ClientOptions options, Common.IKVStoreConfig clientServerMergedConfig) {
    _handle = UNINITIALIZED_HANDLE;
    _ikvClientJni = null;
    _clientOptions = Objects.requireNonNull(options);
    _clientServerMergedConfig = clientServerMergedConfig;
  }

  // ok to call even if already open
  @Override
  public void startupReader() throws RuntimeException {
    if (_handle != UNINITIALIZED_HANDLE || _ikvClientJni != null) {
      return;
    }

    String mountDirectory =
        Preconditions.checkNotNull(
            _clientOptions.mountDirectory().orElse(null),
            "mountDirectory is a required client option");

    try {
      _ikvClientJni = IKVClientJNI.createNew(mountDirectory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // can throw (reader startup sequence)
    _handle = _ikvClientJni.open(_clientServerMergedConfig.toByteArray());
  }

  // ok to call even if already closed
  @Override
  public void shutdownReader() throws RuntimeException {
    if (_handle == UNINITIALIZED_HANDLE || _ikvClientJni == null) {
      return;
    }

    // can throw
    _ikvClientJni.close(_handle);
    _ikvClientJni = null;
    _handle = UNINITIALIZED_HANDLE;
  }

  @VisibleForTesting
  long handle() {
    return _handle;
  }

  @VisibleForTesting
  IKVClientJNI ikvClientJNI() {
    return _ikvClientJni;
  }

  @Nullable
  @Override
  public byte[] getBytesValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    return _ikvClientJni.readField(
        _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
  }

  @Nullable
  @Override
  public String getStringValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        _ikvClientJni.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null ? null : new String(result, StandardCharsets.UTF_8);
  }

  @Nullable
  @Override
  public Integer getIntValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        _ikvClientJni.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null ? null : ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).getInt();
  }

  @Nullable
  @Override
  public Float getFloatValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        _ikvClientJni.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null
        ? null
        : ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).getFloat();
  }

  @Nullable
  @Override
  public Long getLongValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        _ikvClientJni.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null ? null : ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).getLong();
  }

  @Nullable
  @Override
  public Double getDoubleValue(Object primaryKey, String fieldName) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    @Nullable
    byte[] result =
        _ikvClientJni.readField(
            _handle, serializePrimaryKey(primaryKey, _clientOptions.primaryKeyType()), fieldName);
    return result == null
        ? null
        : ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).getDouble();
  }

  // Returns nullable byte[] in the form of an iterator
  @Override
  public Iterator<byte[]> multiGetBytesValues(List<Object> primaryKeys, List<String> fieldNames) {
    return multiGetRawByteValues(primaryKeys, fieldNames);
  }

  private Iterator<byte[]> multiGetRawByteValues(
      List<Object> primaryKeys, List<String> fieldNames) {
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      return Collections.emptyIterator();
    }

    byte[] sizePrefixedPrimaryKeys =
        sizePrefixedSerializedPrimaryKeys(primaryKeys, _clientOptions.primaryKeyType());
    byte[] sizePrefixedFieldNames = sizePrefixedFieldNames(fieldNames);

    byte[] result =
        _ikvClientJni.batchReadFields(_handle, sizePrefixedPrimaryKeys, sizePrefixedFieldNames);
    return new RawByteValuesIterator(result);
  }

  private static final class RawByteValuesIterator implements Iterator<byte[]> {
    private final ByteBuffer _result;

    public RawByteValuesIterator(byte[] result) {
      _result = ByteBuffer.wrap(Objects.requireNonNull(result)).order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public boolean hasNext() {
      return _result.hasRemaining();
    }

    @Override
    @Nullable
    public byte[] next() {
      int size = _result.getInt();

      if (size == 0) {
        return new byte[0];
      }

      if (size == -1) {
        return null;
      }

      byte[] value = new byte[size];
      _result.get(value);
      return value;
    }
  }

  /**
   * Concatenates serialized bytes of primary-keys, by prefixing their size as a lower-endian 32-bit
   * signed integer.
   */
  private static byte[] sizePrefixedSerializedPrimaryKeys(
      List<Object> primaryKeys, Common.FieldType fieldType) {
    int capacity = 0;
    int i = 0;
    byte[][] serializedPrimaryKeys = new byte[primaryKeys.size()][];
    for (Object primaryKey : primaryKeys) {
      byte[] serializedPrimaryKey = serializePrimaryKey(primaryKey, fieldType);
      capacity += 4 + serializedPrimaryKey.length;
      serializedPrimaryKeys[i] = serializedPrimaryKey;
      i++;
    }

    ByteBuffer bb = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    for (byte[] serializedPrimaryKey : serializedPrimaryKeys) {
      bb.putInt(serializedPrimaryKey.length);
      bb.put(serializedPrimaryKey);
    }

    return bb.array();
  }

  private static byte[] serializePrimaryKey(Object primaryKey, Common.FieldType fieldType) {
    switch (fieldType) {
      case STRING -> {
        return primaryKey.toString().getBytes(StandardCharsets.UTF_8);
      }
      case BYTES -> {
        // can throw ClassCastException - ok
        return (byte[]) primaryKey;
      }
      default -> throw new UnsupportedOperationException();
    }
  }

  private static byte[] sizePrefixedFieldNames(List<String> fieldNames) {
    int capacity = 0;

    int i = 0;
    byte[][] serializedFieldNames = new byte[fieldNames.size()][];
    for (String fieldName : fieldNames) {
      byte[] serializedFieldName = fieldName.getBytes(StandardCharsets.UTF_8);
      capacity += 4 + serializedFieldName.length;
      serializedFieldNames[i] = serializedFieldName;
      i++;
    }

    ByteBuffer bb = ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    for (byte[] serializedFieldName : serializedFieldNames) {
      bb.putInt(serializedFieldName.length);
      bb.put(serializedFieldName);
    }

    return bb.array();
  }
}
