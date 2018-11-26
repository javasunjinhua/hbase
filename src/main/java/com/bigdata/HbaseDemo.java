package com.bigdata;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Administrator
 * @date 2018/11/23 11:44
 * @description
 */
public class HbaseDemo {

    public static  Configuration  conf;

   static{
       //获取configuration 对象
       conf = HBaseConfiguration.create();
       conf.set("hbase.zookeeper.quorum","192.168.1.102");
       conf.set("hbase.zookeeper.property.clientPort","2181");

   }

    public static void main(String[] args) throws  Exception{
     //   tableExists();
     //   createTable();
      //  deleteTable();
      //  putData();
        //deleteDate();
    //deleteMultiRow("1002","1001");
       // deleteMultiRow("student","1002","1001");
       // scanData();
        //scanDataByCF();
    }


    /*//删除多行数据
    public static void deleteMultiRow(String... rows) throws IOException {
     // /获取连接对象
       Connection connection =ConnectionFactory.createConnection(conf);
       //通过连接获取hbase客户端对象
       HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
      //获取表对象
       HTable student = (HTable) connection.getTable(TableName.valueOf("student"));
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        student.delete(deleteList);
        student.close();
    }*/

    //删除多行数据
    public static void deleteMultiRow(String tableName,String... rows) throws  Exception{

        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteArrayList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteArrayList.add(delete);

        }
        hTable.delete(deleteArrayList);
        hTable.close();
        System.out.println("刪除成功");

    }








   //查詢所有
   public static void scanData() throws  Exception{
      // /获取连接对象
       Connection connection =ConnectionFactory.createConnection(conf);
       //通过连接获取hbase客户端对象
       HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
      //获取表对象
       HTable student = (HTable) connection.getTable(TableName.valueOf("student"));
       //获取一个scan 对象
       Scan scan = new Scan();
       ResultScanner scanner = student.getScanner(scan);
       //遍历scanner
       for (Result result : scanner) {
       //获取到一行
           Cell[] cells = result.rawCells();
           for (Cell cell : cells) {
               //通过cell 获取rowkey,cf,column ,value
               String cf  = Bytes.toString(CellUtil.cloneFamily(cell));
               String column = Bytes.toString(CellUtil.cloneQualifier(cell));
               String value = Bytes.toString(CellUtil.cloneValue(cell));
               String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
               System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);

           }
       }
     student.close();
   }

//查询某一个列族数据
    public static void  scanDataByCF() throws  Exception{
        // /获取连接对象
        Connection connection =ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //获取表对象
        HTable student = (HTable) connection.getTable(TableName.valueOf("student"));
       //创建查询的get对象
        Get get = new Get(Bytes.toBytes("1001"));
        //指定列族信息
         get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));
//执行查询
        Result result = student.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            //通过cell获取rowkey,cf,column,value
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            String rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            System.out.println(rowkey + "----" + cf + "---" + column + "---" + value);
        }
        student.close();
    }



//删除一行
    public  static void deleteDate() throws  Exception{
        //获取连接对象
        Connection connection =ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //获取表对象
        Table  t = connection.getTable(TableName.valueOf("student"));
        //创建一个delete 对象
        Delete delete = new Delete(Bytes.toBytes("1005"));
        t.delete(delete);
        System.out.println("删除指定数据成功");
    }










//向表中插入一条数据
    public static void putData() throws Exception {

       //获取连接对象
        Connection connection =ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //获取表对象
        Table  t = connection.getTable(TableName.valueOf("student"));
        //设定rowkey
        Put put = new Put(Bytes.toBytes("1005"));
        //设定列族，列，value
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("addr"),Bytes.toBytes("beijing"));
     //执行插入
        t.put(put);
        //关闭table 对象
        t.close();
        System.out.println("插入成功了");
    }














    //删除表
    public static void deleteTable() throws Exception{
        //获取连接对象
        Connection connection =ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //先设置表为不可用，disable
        admin.disableTable("student1");
        //删除表

        admin.deleteTable("student1");
        System.out.println("studnet表被删除了");
    }



    //创建表
    public static void  createTable() throws Exception{
        //获取连接对象
        Connection connection =ConnectionFactory.createConnection(conf);
        //通过连接获取hbase客户端对象
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        //创建表描述器
        HTableDescriptor student1 = new HTableDescriptor(TableName.valueOf("student1"));
       //设置列族描述器
          student1.addFamily(new HColumnDescriptor("info"));
       //执行创建操作
        admin.createTable(student1);
        System.out.println("student1表创建成功");


    }






   //判断表是否存在
   public static void  tableExists() throws Exception{
       //获取连接对象
       Connection connection =ConnectionFactory.createConnection(conf);
       //通过连接获取hbase客户端对象
       HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
       //通过admin 操作hbase
       boolean res = admin.tableExists(TableName.valueOf("student"));
       System.out.println("studnet表是否存在"+res);

   }




}
