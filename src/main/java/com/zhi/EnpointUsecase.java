package com.zhi;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.zhi.protos.Sum;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;

public class EnpointUsecase extends Sum.SumService implements Coprocessor, CoprocessorService {
    private RegionCoprocessorEnvironment env;

    @Override
    public void getSum(RpcController controller, Sum.SumRequest request, RpcCallback<Sum.SumResponse> done) {

    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (coprocessorEnvironment instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) coprocessorEnvironment;
        }else {
            throw new CoprocessorException("must be loaded on a table region");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {

    }

    @Override
    public Service getService() {
        return this;
    }
}
