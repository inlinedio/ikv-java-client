package io.inlined.clients;

import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.Streaming;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** RPC based writer instance. */
public class DefaultInlineKVWriter implements InlineKVWriter {
  private static final long UNINITIALIZED_HANDLE = -1;

  private final String _mountDirectory;
  private final String _primaryKeyFieldName;
  private final String _partitioningKeyFieldName;

  private final Common.IKVStoreConfig _clientServerMergedConfig;
  private volatile long _handle;
  private volatile IKVClientJNI _ikvClientJni;

  public DefaultInlineKVWriter(
      ClientOptions options, Common.IKVStoreConfig clientServerMergedConfig) {
    _handle = UNINITIALIZED_HANDLE;
    _ikvClientJni = null;

    Objects.requireNonNull(options);
    _mountDirectory =
        Preconditions.checkNotNull(
            options.mountDirectory().orElse(null), "mountDirectory is a required client option");

    _clientServerMergedConfig = clientServerMergedConfig;
    _primaryKeyFieldName =
        clientServerMergedConfig.getStringConfigsOrThrow(IKVConstants.PRIMARY_KEY_FIELD_NAME);
    _partitioningKeyFieldName =
        clientServerMergedConfig.getStringConfigsOrThrow(IKVConstants.PARTITIONING_KEY_FIELD_NAME);
  }

  @Override
  public void startupWriter() {
    if (_handle != UNINITIALIZED_HANDLE || _ikvClientJni != null) {
      return;
    }

    try {
      _ikvClientJni = IKVClientJNI.createNew(_mountDirectory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // can throw (reader startup sequence)
    _handle = _ikvClientJni.openWriter(_clientServerMergedConfig.toByteArray());
  }

  @Override
  public void shutdownWriter() {
    if (_handle == UNINITIALIZED_HANDLE || _ikvClientJni == null) {
      return;
    }

    // can throw
    _ikvClientJni.closeWriter(_handle);
    _ikvClientJni = null;
    _handle = UNINITIALIZED_HANDLE;
  }

  @Override
  public void upsertFieldValues(IKVDocument document) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    // extract primary and partitioning keys
    Map<String, FieldValue> documentAsFieldValueMap = document.asNameToFieldValueMap();
    Objects.requireNonNull(extractPrimaryKeyValue(documentAsFieldValueMap));
    FieldValue partitioningKey =
        Objects.requireNonNull(extractPartitioningKeyValue(documentAsFieldValueMap));

    // construct IKVDataEvent
    IKVDocumentOnWire documentOnWire =
        IKVDocumentOnWire.newBuilder().putAllDocument(documentAsFieldValueMap).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();
    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setEventHeader(
                Streaming.EventHeader.newBuilder().setSourceTimestamp(timestamp).build())
            .setUpsertDocumentFieldsEvent(
                Streaming.UpsertDocumentFieldsEvent.newBuilder()
                    .setDocument(documentOnWire)
                    .build())
            .build();

    // send
    _ikvClientJni.singlePartitionWrite(_handle, partitioningKey.toByteArray(), event.toByteArray());
  }

  @Override
  public void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    if (fieldsToDelete.isEmpty()) {
      return;
    }

    // extract primary and partitioning keys
    Map<String, FieldValue> documentAsFieldValueMap = documentId.asNameToFieldValueMap();
    Objects.requireNonNull(extractPrimaryKeyValue(documentAsFieldValueMap));
    FieldValue partitioningKey =
        Objects.requireNonNull(extractPartitioningKeyValue(documentAsFieldValueMap));

    IKVDocumentOnWire docId =
        IKVDocumentOnWire.newBuilder().putAllDocument(documentAsFieldValueMap).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setEventHeader(
                Streaming.EventHeader.newBuilder().setSourceTimestamp(timestamp).build())
            .setDeleteDocumentFieldsEvent(
                Streaming.DeleteDocumentFieldsEvent.newBuilder()
                    .setDocumentId(docId)
                    .addAllFieldsToDelete(fieldsToDelete)
                    .build())
            .build();

    // send
    _ikvClientJni.singlePartitionWrite(_handle, partitioningKey.toByteArray(), event.toByteArray());
  }

  @Override
  public void deleteDocument(IKVDocument documentId) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);

    // extract primary and partitioning keys
    Map<String, FieldValue> documentAsFieldValueMap = documentId.asNameToFieldValueMap();
    Objects.requireNonNull(extractPrimaryKeyValue(documentAsFieldValueMap));
    FieldValue partitioningKey =
        Objects.requireNonNull(extractPartitioningKeyValue(documentAsFieldValueMap));

    IKVDocumentOnWire docId =
        IKVDocumentOnWire.newBuilder().putAllDocument(documentAsFieldValueMap).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setEventHeader(
                Streaming.EventHeader.newBuilder().setSourceTimestamp(timestamp).build())
            .setDeleteDocumentEvent(
                Streaming.DeleteDocumentEvent.newBuilder().setDocumentId(docId).build())
            .build();

    // send
    _ikvClientJni.singlePartitionWrite(_handle, partitioningKey.toByteArray(), event.toByteArray());
  }

  @Override
  public void dropFieldsByName(List<String> fieldNames) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    Streaming.DropFieldEvent dropFieldEvent =
        Streaming.DropFieldEvent.newBuilder()
            .addAllFieldNames(fieldNames)
            .setDropAll(false)
            .build();
    sendDropFieldEvent(dropFieldEvent);
  }

  @Override
  public void dropFieldsByNamePrefix(List<String> fieldNamePrefixes) {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    Streaming.DropFieldEvent dropFieldEvent =
        Streaming.DropFieldEvent.newBuilder()
            .addAllFieldNamePrefixes(fieldNamePrefixes)
            .setDropAll(false)
            .build();
    sendDropFieldEvent(dropFieldEvent);
  }

  @Override
  public void dropAllDocuments() {
    Preconditions.checkState(_handle != UNINITIALIZED_HANDLE);
    Streaming.DropFieldEvent dropFieldEvent =
        Streaming.DropFieldEvent.newBuilder().setDropAll(true).build();
    sendDropFieldEvent(dropFieldEvent);
  }

  private void sendDropFieldEvent(Streaming.DropFieldEvent dropFieldEvent) {
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    Streaming.IKVDataEvent event =
        Streaming.IKVDataEvent.newBuilder()
            .setEventHeader(
                Streaming.EventHeader.newBuilder().setSourceTimestamp(timestamp).build())
            .setDropFieldEvent(dropFieldEvent)
            .build();

    _ikvClientJni.broadcastWrite(_handle, event.toByteArray());
  }

  private FieldValue extractPrimaryKeyValue(Map<String, FieldValue> document)
      throws IllegalArgumentException {
    FieldValue value = document.get(_primaryKeyFieldName);
    Preconditions.checkArgument(value != null, "primaryKey missing");
    return value;
  }

  private FieldValue extractPartitioningKeyValue(Map<String, FieldValue> document)
      throws IllegalArgumentException {
    FieldValue value = document.get(_partitioningKeyFieldName);
    Preconditions.checkArgument(value != null, "partitioningKey missing");
    return value;
  }
}
