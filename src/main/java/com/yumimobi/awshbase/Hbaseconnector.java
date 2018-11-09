/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yumimobi.awshbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.io.Text;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;



/**
 *
 * @author Yuruidong
 */
public class Hbaseconnector {

    public static void main(String[] args) throws IOException {

        
        if(args.length < 1){
            System.err.println("java -cp xxx MainClass <dns> <port>");
            System.exit(-1);
        }
        
        String dns = args[0]; // AWS DNS
        Integer port = Integer.parseInt(args[1]); //rest api port
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.client.pause", "50"); 
        conf.set("hbase.client.retries.number", "3"); 
        conf.set("hbase.rpc.timeout", "2000"); 
        conf.set("hbase.client.operation.timeout", "3000"); 
        conf.set("hbase.client.scanner.timeout.period", "10000");
        
        Cluster cluster = new Cluster();
        cluster.add(dns, port); 
        
        Client client = new Client(cluster); 
        RemoteAdmin admin = new RemoteAdmin(client,conf);
        
        String tableName = "my_table";
        String [] families = {"cf1", "cf2", "cf3"};
        if (admin.isTableAvailable(tableName)) {
                System.out.println("table already exists!");
                return;
        } else {
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
                for (int i = 0; i < families.length; i++) {
                        tableDesc.addFamily(new HColumnDescriptor(families[i]));
                }
                admin.createTable(tableDesc);
                System.out.println("create table " + tableName + " ok.");
        } 
        

        RemoteHTable table = new RemoteHTable(client, conf, tableName); 
        
        Put put = new Put(Bytes.toBytes("www.test2.com"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("price"), Bytes.toBytes("888"));
        put.addColumn(Bytes.toBytes("cf3"), Bytes.toBytes("function"), Bytes.toBytes("print"));
        table.put(put);
        table.flushCommits();
        
        
        Get get=new Get(Bytes.toBytes("www.test1.com"));
        get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("price"));

        Result result = null; 
        result = table.get(get);
        String cf1_price = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("price"))) ;
        System.out.println("Scan row[" + cf1_price + "]: ");
        
        
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("price"));
        ResultScanner scanner = table.getScanner(scan); 

        for (Result result2 : scanner) {
          System.out.println("Scan row[" + Bytes.toString(result2.getRow()) +
            "]: " + result2);
        }
        
        
        table.close();
        
        

   }


}