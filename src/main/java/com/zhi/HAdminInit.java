package com.zhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HAdminInit {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try(Connection conn=ConnectionFactory.createConnection()) {
            Admin admin = conn.getAdmin();
            if (admin.tableExists(TableName.valueOf("users"))&&admin.isTableDisabled(TableName.valueOf("users"))) {
                admin.enableTable(TableName.valueOf("users"));
            }
            if (admin.isTableAvailable(TableName.valueOf("users"))) {//表存在，删除重建
                admin.disableTable(TableName.valueOf("users"));
                admin.deleteTable(TableName.valueOf("users"));
                System.out.println("表存在，删除");

                createTable(admin);
            } else {
                createTable(admin);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void createTable(Admin admin) throws IOException {
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Bytes.toBytes("personalDet"));
        HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(Bytes.toBytes("salaryDet"));
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("users"));
        tableDescriptor.addFamily(columnDescriptor);
        tableDescriptor.addFamily(columnDescriptor2);
        admin.createTable(tableDescriptor);
        System.out.println("表创建成功");
        initTable();
    }
    private static void initTable() {
        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("users"))) {
            Put put = new Put(Bytes.toBytes("admin"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("name"), Bytes.toBytes("Admin"));
            Put put2 = new Put(Bytes.toBytes("admin"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("lastname"), Bytes.toBytes("Admin"));
            Put put3 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("name"), Bytes.toBytes("Charles"));
            Put put14 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("lastname"), Bytes.toBytes("Dickens"));
            Put put4 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("dob"), Bytes.toBytes("02/07/1812"));
            Put put5 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("gross"), Bytes.toBytes("10000"));
            Put put6 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("net"), Bytes.toBytes("8000"));
            Put put7 = new Put(Bytes.toBytes("cdickens"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("allowances"), Bytes.toBytes("2000"));
            Put put8 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("name"), Bytes.toBytes("Jules"));
            Put put9 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("lastname"), Bytes.toBytes("Verne"));
            Put put10 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("personalDet"), Bytes.toBytes("dob"), Bytes.toBytes("02/08/1828"));

            Put put11 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("gross"), Bytes.toBytes("12000"));
            Put put12 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("net"), Bytes.toBytes("9000"));
            Put put13 = new Put(Bytes.toBytes("jverne"))
                    .addColumn(Bytes.toBytes("salaryDet"), Bytes.toBytes("allowances"), Bytes.toBytes("3000"));
            List<Put> list = new ArrayList<>();
            list.add(put);
            list.add(put2);
            list.add(put3);
            list.add(put4);
            list.add(put5);
            list.add(put6);
            list.add(put7);
            list.add(put8);
            list.add(put9);
            list.add(put10);
            list.add(put11);
            list.add(put12);
            list.add(put13);
            list.add(put14);
            table.put(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
