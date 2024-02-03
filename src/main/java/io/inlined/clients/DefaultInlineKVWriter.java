package io.inlined.clients;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import com.inlineio.schemas.Common.*;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

/** RPC based writer instance. */
public class DefaultInlineKVWriter implements InlineKVWriter {
  private volatile InlineKVWriteServiceGrpc.InlineKVWriteServiceBlockingStub _stub;
  private final UserStoreContextInitializer _userStoreCtxInitializer;

  public DefaultInlineKVWriter(ClientOptions clientOptions) {
    Objects.requireNonNull(clientOptions);
    _userStoreCtxInitializer = clientOptions.createUserStoreContextInitializer();
  }

  @Override
  public void startupWriter() {
    ManagedChannelBuilder<?> channelBuilder =
        ManagedChannelBuilder.forAddress(
                IKVConstants.IKV_GATEWAY_GRPC_URL, IKVConstants.IKV_GATEWAY_GRPC_PORT)
            .overrideAuthority("www.inlined.io");
    ManagedChannel channel = channelBuilder.build();
    _stub = InlineKVWriteServiceGrpc.newBlockingStub(channel);
  }

  @Override
  public void shutdownWriter() {
    _stub = null;
  }

  @Override
  public void upsertFieldValues(IKVDocument document) {
    Preconditions.checkState(
        _stub != null, "client cannot be used before finishing startup() or after shutdown()");
    Preconditions.checkArgument(
        document.asNameToFieldValueMap().size() >= 1, "empty document not allowed");

    IKVDocumentOnWire documentOnWire =
        IKVDocumentOnWire.newBuilder().putAllDocument(document.asNameToFieldValueMap()).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    UpsertFieldValuesRequest request =
        UpsertFieldValuesRequest.newBuilder()
            .setDocument(documentOnWire)
            .setTimestamp(timestamp)
            .setUserStoreContextInitializer(_userStoreCtxInitializer)
            .build();

    StatusRuntimeException maybeException = null;
    for (int retry = 0; retry < 3; retry++) {
      try {
        // make grpc call
        Status ignored = _stub.upsertFieldValues(request);
        return;
      } catch (StatusRuntimeException e) {
        maybeException = e;

        // retry only when servers are unavailable
        if (e.getStatus().getCode() != io.grpc.Status.Code.UNAVAILABLE) {
          break;
        }
      }
    }

    throw new RuntimeException(
        "upsertFieldValues failed with error: "
            + MoreObjects.firstNonNull(maybeException.getMessage(), "unknown"));
  }

  @Override
  public void deleteFieldValues(IKVDocument documentId, Collection<String> fieldsToDelete) {
    Preconditions.checkState(
        _stub != null, "client cannot be used before finishing startup() or after shutdown()");
    Preconditions.checkArgument(
        documentId.asNameToFieldValueMap().size() >= 1, "need document-identifiers");
    if (fieldsToDelete.isEmpty()) {
      return;
    }

    IKVDocumentOnWire docId =
        IKVDocumentOnWire.newBuilder().putAllDocument(documentId.asNameToFieldValueMap()).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    DeleteFieldValueRequest request =
        DeleteFieldValueRequest.newBuilder()
            .setDocumentId(docId)
            .addAllFieldNames(fieldsToDelete)
            .setTimestamp(timestamp)
            .setUserStoreContextInitializer(_userStoreCtxInitializer)
            .build();

    StatusRuntimeException maybeException = null;
    for (int retry = 0; retry < 3; retry++) {
      try {
        // make grpc call
        Status ignored = _stub.deleteFieldValues(request);
        return;
      } catch (StatusRuntimeException e) {
        maybeException = e;

        // retry only when servers are unavailable
        if (e.getStatus().getCode() != io.grpc.Status.Code.UNAVAILABLE) {
          break;
        }
      }
    }

    throw new RuntimeException(
        "deleteFieldValues failed with error: "
            + MoreObjects.firstNonNull(maybeException.getMessage(), "unknown"));
  }

  @Override
  public void deleteDocument(IKVDocument documentId) {
    Preconditions.checkState(
        _stub != null, "client cannot be used before finishing startup() or after shutdown()");
    Preconditions.checkArgument(
        documentId.asNameToFieldValueMap().size() >= 1, "need document-identifiers");

    IKVDocumentOnWire docId =
        IKVDocumentOnWire.newBuilder().putAllDocument(documentId.asNameToFieldValueMap()).build();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build();

    DeleteDocumentRequest request =
        DeleteDocumentRequest.newBuilder()
            .setDocumentId(docId)
            .setTimestamp(timestamp)
            .setUserStoreContextInitializer(_userStoreCtxInitializer)
            .build();

    StatusRuntimeException maybeException = null;
    for (int retry = 0; retry < 3; retry++) {
      try {
        // make grpc call
        Status ignored = _stub.deleteDocument(request);
        return;
      } catch (StatusRuntimeException e) {
        maybeException = e;

        // retry only when servers are unavailable
        if (e.getStatus().getCode() != io.grpc.Status.Code.UNAVAILABLE) {
          break;
        }
      }
    }

    throw new RuntimeException(
        "deleteDocument failed with error: "
            + MoreObjects.firstNonNull(maybeException.getMessage(), "unknown"));
  }
}
