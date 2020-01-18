package dev.vidlicka.hbase.filtermapreduce.reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerRequest;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerResponse;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerService;

public class ReducerClient {

  private ReducerClient() {}

  public static <A, B> B call(Table table, Scan scan, A initialReduceValue,
      SerializableBiFunction<A, List<Cell>, A> reducer, B initialMergeValue,
      SerializableBiFunction<B, A, B> merger) throws Throwable {
    ReducerRequest request =
        ReducerRequest.newBuilder().setSerializedScan(ProtobufUtil.toScan(scan).toByteString())
            .setInitialValue(ByteString.copyFrom(SerdeUtil.serialize(initialReduceValue)))
            .setReducer(ByteString.copyFrom(SerdeUtil.serialize(reducer))).build();

    Map<byte[], A> results = table.coprocessorService(ReducerService.class, scan.getStartRow(),
        scan.getStopRow(), new ReducerCall<A>(request));

    B accumulator = initialMergeValue;
    for (A value : results.values()) {
      accumulator = merger.apply(accumulator, value);
    }
    return accumulator;
  }

  public static <A> SerializableBiFunction<A, List<Cell>, A> createRowReducer(
      SerializableBiFunction<A, Cell, A> cellReducer) {
    return (accumulator, row) -> {
      for (Cell cell : row) {
        accumulator = cellReducer.apply(accumulator, cell);
      }
      return accumulator;
    };
  }

  private static class ReducerCall<A> implements Batch.Call<ReducerService, A> {

    ReducerRequest request;

    ReducerCall(ReducerRequest request) {
      this.request = request;
    }

    @Override
    public A call(ReducerService reducerService) throws IOException {
      ServerRpcController controller = new ServerRpcController();
      BlockingRpcCallback<ReducerResponse> rpcCallback = new BlockingRpcCallback<ReducerResponse>();
      reducerService.reduce(controller, request, rpcCallback);
      ReducerResponse response = rpcCallback.get();
      if (controller.failedOnException()) {
        throw controller.getFailedOn();
      }

      A result;

      try {
        result = SerdeUtil.deserialize(response.getResult().toByteArray());
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      return result;
    }
  }
}
