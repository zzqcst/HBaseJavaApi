package com.zhi;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 以下Observer协处理器可防止在“用户”表的“获取”或“扫描”中返回用户admin的详细信息。
 * 1. 实现RegionServer接口，重写perGetOp()方法，用来检查客户端请求的rowkey为admin,若是，返回空结果
 * 2. 打包为jar文件
 * 3. 将jar文件放到HDFS或者其它HBase类路径中
 * 4. 加载协处理器
 * 5. 测试
 */
public class RegionObserverExample extends BaseRegionObserver {
    private static final byte[] ADMIN = Bytes.toBytes("admin");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("details");
    private static final byte[] COLUMN = Bytes.toBytes("Admin_det");
    private static final byte[] VALUE = Bytes.toBytes("You can't see Admin details");
    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        super.preGetOp(e, get, results);
        if (Bytes.equals(get.getRow(), ADMIN)) {
            Cell cell = CellUtil.createCell(ADMIN, COLUMN_FAMILY, COLUMN, System.currentTimeMillis(), (byte)4, VALUE);
            results.add(cell);
            e.bypass();
        }
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,new BinaryComparator(ADMIN));
        scan.setFilter(filter);
        return super.preScannerOpen(e, scan, s);
    }
}
