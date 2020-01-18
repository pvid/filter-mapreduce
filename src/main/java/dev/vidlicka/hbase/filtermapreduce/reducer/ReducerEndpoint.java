package dev.vidlicka.hbase.filtermapreduce.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import dev.vidlicka.hbase.filtermapreduce.SerdeUtil;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerRequest;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerResponse;
import dev.vidlicka.hbase.filtermapreduce.reducer.ProtoService.ReducerService;

public class ReducerEndpoint extends ReducerService implements Coprocessor, CoprocessorService {
  private RegionCoprocessorEnvironment env;

  @Override
  public void reduce(RpcController controller, ReducerRequest request,
      RpcCallback<ReducerResponse> done) {
    Object initialValue = null;
    BiFunction<Object, List<Cell>, Object> reducer = null;
    Scan scan = null;

    // deserialize values
    try {
      initialValue = SerdeUtil.deserialize(request.getInitialValue().toByteArray());
      reducer = SerdeUtil.deserialize(request.getReducer().toByteArray());
      scan = ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(request.getSerializedScan()));
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      return;
    } catch (ClassNotFoundException e) {
      ResponseConverter.setControllerException(controller,
          new IOException("Class of the deserialized value not found", e));
    }
    Object accumulator = initialValue;
    List<Cell> row = new ArrayList<Cell>();
    boolean hasMoreRows;
    RegionScanner scanner = null;

    try {
      scanner = env.getRegion().getScanner(scan);
      do {
        hasMoreRows = scanner.next(row);
        if (!row.isEmpty())
          accumulator = reducer.apply(accumulator, row);
        // clear to list to prepare for the next row
        row.clear();
      } while (hasMoreRows);
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {
        }
      }
    }

    ReducerResponse response;

    try {
      response = ReducerResponse.newBuilder()
          .setResult(ByteString.copyFrom(SerdeUtil.serialize(accumulator))).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
      return;
    }
    done.run(response);
  }

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {}
}
