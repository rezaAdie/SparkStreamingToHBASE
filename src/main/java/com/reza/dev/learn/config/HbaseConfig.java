package com.reza.dev.learn.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class HbaseConfig {

    private String outputTable;
    private String hbaseMaster;
    private String zookeeperQuorum;
    private String hbaseZkPort;

    /**
     * Set variable in constructor
     * @param outputTable -> String output table name
     * @param hbaseMaster -> HBase Master host
     * @param zookeeperQuorum -> Zookeeper host
     */
    public HbaseConfig(String outputTable, String hbaseMaster, String zookeeperQuorum, String hbaseZkPort) {
        this.outputTable = outputTable;
        this.hbaseMaster = hbaseMaster;
        this.zookeeperQuorum = zookeeperQuorum;
        this.hbaseZkPort = hbaseZkPort;
    }

    /**
     * Set HBase configuration
     * @return HBase configuration
     */
    private Configuration hbaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", hbaseMaster);
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        configuration.setInt("timeout", 120000);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.client.keyvalue.maxsize", "0");
        configuration.set("hbase.client.scanner.timeout.period", "100000");
        configuration.set("hbase.rpc.timeout", "100000");
        configuration.set("mapred.output.dir", "/tmp");
        configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");
        configuration.set("hbase.zookeeper.property.clientPort", hbaseZkPort);

        return configuration;
    }

    /**
     * Set HBase job
     * @return HBase job
     * @throws IOException -> Exception
     */
    public Job hbaseJob() throws IOException {
        Job job = Job.getInstance(hbaseConfiguration());
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, outputTable);
        job.setOutputFormatClass(TableOutputFormat.class);

        return job;
    }
}
