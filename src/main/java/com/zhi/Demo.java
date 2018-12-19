package com.zhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class Demo {
    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf("users"))) {
//            Put put = new Put(Bytes.toBytes("row3"));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"), Bytes.toBytes("沈阳"));
//            table.put(put);//添加和更新数据
//            Get get = new Get(Bytes.toBytes("row3"));
//            Result result = table.get(get);
//            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("addr"));
//            System.out.println(Bytes.toString(value));//查询数据
//            Delete del = new Delete(Bytes.toBytes("row3"));
//            del.addColumn(Bytes.toBytes("info"), Bytes.toBytes("addr"));
//            table.delete(del);//删除数据
//            table.checkAndMutate(Bytes.toBytes("row3"), Bytes.toBytes("info")).qualifier(Bytes.toBytes("addr")).ifNotExists().thenPut(put);
//            Scan scan = new Scan();
//            ResultScanner scanner = table.getScanner(scan);
//            for (Result result = scanner.next(); result != null; result = scanner.next()) {
//                System.out.println("Found row : " + result.toString());
//            }
//            scanner.close();//扫描数据
//            List<Row> actions = new ArrayList<>();
//            Put put = new Put(Bytes.toBytes("row3"));
//            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("李白"));
//            actions.add(put);
//            Put put1 = new Put(Bytes.toBytes("row3"));
//            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(20));
//            actions.add(put1);
//            Put put2 = new Put(Bytes.toBytes("row4"));
//            put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("21"));
//            actions.add(put2);
//            Delete delete = new Delete(Bytes.toBytes("row3"));
//            actions.add(delete);
//            Object[] results = new Object[actions.size()];
//            table.batch(actions, results);
//            for (int i = 0; i < results.length; i++) {
//                System.out.println("result " + i + ":" + results[i]);
//            }//批操作

//            //行过滤器
//            Scan scan = new Scan();
//            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//            Filter filter1 = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row2")));
//            scan.setFilter(filter1);
//            ResultScanner scanner = table.getScanner(scan);
//            System.out.println("filter 1 .................");
//            for (Result result = scanner.next(); result != null; result = scanner.next()) {
//                System.out.println("Found row : " + result);
//            }
//
//            Filter filter2 = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(".*4"));
//            scan.setFilter(filter2);
//            ResultScanner scanner1 = table.getScanner(scan);
//            System.out.println("filter 2 .................");
//            for (Result result : scanner1) {
//                System.out.println("Found row : " + result);
//            }
//
//            Filter filter3 = new RowFilter(CompareOperator.EQUAL, new SubstringComparator("1"));
//            scan.setFilter(filter3);
//            ResultScanner scanner2 = table.getScanner(scan);
//            System.out.println("filter 3 .................");
//            for (Result result : scanner2) {
//                System.out.println("Found row : " + result);
//            }
//
//            scanner.close();

//            Scan scan = new Scan();
//            Filter filter = new FamilyFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes("i")));
//            Filter filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row2")));
//            Filter filter = new QualifierFilter(CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("name")));
//            Filter filter = new DependentColumnFilter(Bytes.toBytes("info"),Bytes.toBytes("name"));
//            Filter filter = new ValueFilter(CompareOperator.EQUAL,new SubstringComparator("1"));
//            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("name"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes("libai")));
//            filter.setFilterIfMissing(true);
//            Filter filter = new PrefixFilter(Bytes.toBytes("info"));

//            final byte[] POSTFIX = new byte[] { 0x00 };
//            Filter filter = new PageFilter(2);
//            int totalRows = 0;
//            byte[] lastRow = null;//上次分页结果的最后一行
//            while (true) {
//                Scan scan = new Scan();
//                scan.setFilter(filter);
//                if (lastRow != null) {
//                    byte[] startRow = Bytes.add(lastRow, POSTFIX);
//                    System.out.println("start row: " +
//                            Bytes.toStringBinary(startRow));
//                    scan.withStartRow(startRow);
//                }
//                ResultScanner scanner = table.getScanner(scan);
//                int localRows = 0;//一次分页结果的第几行
//                Result result;
//                while ((result = scanner.next()) != null) {
//                    System.out.println(localRows++ + ": " + result);
//                    totalRows++;
//                    lastRow = result.getRow();//最后一行行键
//                    System.out.println("lastRow:"+Bytes.toString(lastRow));
//                }
//                scanner.close();
//                if (localRows == 0) break;//某次分页结果为0行，scan结束
//            }
//            System.out.println("total rows: " + totalRows);

            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,new BinaryComparator(Bytes.toBytes("admin")));
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(result);
            }
            scanner.close();
            //计数器
//            long l = table.incrementColumnValue(Bytes.toBytes("row1"), Bytes.toBytes("info"), Bytes.toBytes("counter"), 1);
//            Increment increment = new Increment(Bytes.toBytes("row1"));
//            increment.addColumn(Bytes.toBytes("info"), Bytes.toBytes("counter"), 1);
//            increment.addColumn(Bytes.toBytes("info"), Bytes.toBytes("counter1"), 1);
//            increment.addColumn(Bytes.toBytes("info"), Bytes.toBytes("counter2"), 1);
//            table.increment(increment);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
