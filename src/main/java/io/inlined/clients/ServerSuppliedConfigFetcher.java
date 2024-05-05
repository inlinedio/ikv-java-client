package io.inlined.clients;

import com.google.common.base.MoreObjects;
import com.inlineio.schemas.Common;
import com.inlineio.schemas.InlineKVWriteServiceGrpc;
import com.inlineio.schemas.Services;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.protobuf.StatusProto;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class ServerSuppliedConfigFetcher {
  private final ManagedChannel _channel;
  private final InlineKVWriteServiceGrpc.InlineKVWriteServiceBlockingStub _stub;
  private final Services.UserStoreContextInitializer _userStoreContextInitializer;

  public ServerSuppliedConfigFetcher(ClientOptions clientOptions) {
    // TODO: stub creation- use dns
    ManagedChannelBuilder<?> channelBuilder =
        ManagedChannelBuilder.forAddress(
                IKVConstants.IKV_GATEWAY_GRPC_URL, IKVConstants.IKV_GATEWAY_GRPC_PORT)
            .overrideAuthority("www.inlined.io");
    _channel = channelBuilder.build();
    _stub = InlineKVWriteServiceGrpc.newBlockingStub(_channel);
    _userStoreContextInitializer =
        Objects.requireNonNull(clientOptions.createUserStoreContextInitializer());
  }

  public void close() {
    _channel.shutdown();
    try {
      _channel.awaitTermination(100, TimeUnit.MICROSECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  // TODO: add retries with grpc service configs
  public Common.IKVStoreConfig fetchServerConfig() {
    Services.GetUserStoreConfigRequest request =
        Services.GetUserStoreConfigRequest.newBuilder()
            .setUserStoreContextInitializer(_userStoreContextInitializer)
            .build();

    Services.GetUserStoreConfigResponse response;
    try {
      // make grpc call
      response = _stub.getUserStoreConfig(request);
      return response.getGlobalConfig();
    } catch (Throwable thrown) {
      // propagate errors
      com.google.rpc.Status errorStatus = StatusProto.fromThrowable(thrown);
      if (errorStatus != null) {
        throw new RuntimeException(
            "Cannot fetch server supplied configs,  error: "
                + MoreObjects.firstNonNull(errorStatus.getMessage(), "unknown"));
      }
      throw new RuntimeException("Cannot fetch server supplied configs");
    }
  }
}
