package com.zhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CoprosessorConfig {
    public static void main(String[] args) {
        loadCoprocessor();
    }

    private static void unloadCoprocessor(){
        TableName tableName = TableName.valueOf("users");
        Configuration conf = HBaseConfiguration.create();
        try (Connection conn = ConnectionFactory.createConnection(conf);) {
            Admin admin = conn.getAdmin();
            admin.disableTable(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("personalDet"));
            HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(Bytes.toBytes("salaryDet"));
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("users"));
            tableDescriptor.addFamily(columnDescriptor);
            tableDescriptor.addFamily(columnDescriptor2);
            admin.modifyTable(TableName.valueOf("users"),tableDescriptor);
            admin.enableTable(tableName);
        } catch (IOException e) {

            e.printStackTrace();
        }
    }

    private static void loadCoprocessor() {
        TableName tableName = TableName.valueOf("users");
        Configuration conf = HBaseConfiguration.create();
        try (Connection conn = ConnectionFactory.createConnection(conf);) {
            Path path = new Path("hdfs://localhost:9000/hbase/hbase1.2.9-1.0.jar");
            Admin admin = conn.getAdmin();
            admin.disableTable(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("personalDet"));
            HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(Bytes.toBytes("salaryDet"));
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("users"));
            tableDescriptor.addFamily(columnDescriptor);
            tableDescriptor.addFamily(columnDescriptor2);
            tableDescriptor.addCoprocessor(RegionObserverExample.class.getCanonicalName(), path, Coprocessor.PRIORITY_USER, null);
            admin.modifyTable(TableName.valueOf("users"),tableDescriptor);
            admin.enableTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
