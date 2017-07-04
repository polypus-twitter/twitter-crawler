/*
    Polypus: a Big Data Self-Deployable Architecture for Microblogging 
    Text Extraction and Real-Time Sentiment Analysis

    Copyright (C) 2017 Rodrigo Martínez (brunneis) <dev@brunneis.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.brunneis.polypus.polypus4t.dao;

import com.brunneis.polypus.polypus4t.vo.DigitalPost;
import java.util.ArrayList;
import java.util.HashMap;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.brunneis.polypus.polypus4t.conf.AerospikeConf;
import com.brunneis.polypus.polypus4t.conf.Conf;
import static com.brunneis.polypus.polypus4t.conf.Conf.DB_PERSISTENCE;
import com.brunneis.polypus.polypus4t.conf.HBaseConf;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author brunneis
 */
public final class DigitalPostSingletonHBaseAerospikeDAO implements DigitalPostDAO {

    private static final DigitalPostSingletonHBaseAerospikeDAO INSTANCE
            = new DigitalPostSingletonHBaseAerospikeDAO();

    private Logger logger;

    // HBASE
    private Table hbaseTable;
    private Connection hbaseConn;
    private final Configuration hbaseConf;
    private final String hbasePrimaryFamily;
    private final String hbaseSecondaryFamily;
    private final String hbaseTableName;

    // AEROSPIKE
    private AerospikeClient aerospikeClient;

    private DigitalPostSingletonHBaseAerospikeDAO() {
        logger = Logger.getLogger(DigitalPostSingletonHBaseAerospikeDAO.class.getName());
        logger.setLevel(Conf.LOGGER_LEVEL.value());

        this.hbaseConf = HBaseConfiguration.create();

        this.hbaseConf.set(
                "hbase.zookeeper.quorum",
                ((HBaseConf) DB_PERSISTENCE.value()).hbaseZookeeperQuorum.value()
        );
        this.hbaseConf.set(
                "hbase.zookeeper.property.clientPort",
                ((HBaseConf) DB_PERSISTENCE.value()).hbaseZookeeperPort.value()
        );

        this.hbasePrimaryFamily
                = ((HBaseConf) DB_PERSISTENCE.value()).hbasePrimaryFamily.value();
        this.hbaseSecondaryFamily
                = ((HBaseConf) DB_PERSISTENCE.value()).hbaseSecondaryFamily.value();
        this.hbaseTableName
                = DB_PERSISTENCE.value().NAME.value();

        // Start clients
        this.connect();
    }

    public static DigitalPostDAO getInstance() {
        return INSTANCE;
    }

    private void connectAerospike() {
        String host = ((AerospikeConf) Conf.DB_BUFFER.value()).host.value();
        Integer port = ((AerospikeConf) Conf.DB_BUFFER.value()).port.value();
        // Single Seed Node
        this.aerospikeClient = new AerospikeClient(host, port);
    }

    private void disconnectAerospike() {
        this.aerospikeClient.close();
    }

    private void reconnectAerospike() {
        this.disconnectAerospike();
        this.connectAerospike();
    }

    private void connectHbase() {
        try {
            this.hbaseConn = ConnectionFactory.createConnection(hbaseConf);
            this.hbaseTable = hbaseConn
                    .getTable(TableName.valueOf(this.hbaseTableName));

        } catch (IOException ex) {
            logger.log(Level.SEVERE, "Error accessing HBase table.", ex);
            System.exit(1);
        }
    }

    private void disconnectHbase() {
        try {
            // Calling close on the HTable instance will invoke flushCommits
            this.hbaseTable.close();
            this.hbaseConn.close();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    private void reconnectHbase() {
        this.disconnectHbase();
        this.connectHbase();
    }

    @Override
    public synchronized void connect() {
        this.connectAerospike();
        this.connectHbase();
    }

    @Override
    public synchronized void disconnect() {
        this.disconnectAerospike();
        this.disconnectHbase();
    }

    @Override
    public synchronized void dumpBuffer(HashMap<String, DigitalPost> buffer) {
        // HBase puts to be written
        List<Put> puts = new ArrayList<>();

        WritePolicy wpolicyOutputBuffer = new WritePolicy();
        // The record never expires
        wpolicyOutputBuffer.expiration = -1;
        // Write only if record does not exist
        wpolicyOutputBuffer.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        wpolicyOutputBuffer.timeout = 1000;
        wpolicyOutputBuffer.retryOnTimeout = true;
        wpolicyOutputBuffer.maxRetries = 5;
        wpolicyOutputBuffer.sleepBetweenRetries = 50;

        WritePolicy wpolicyProcessedPosts = new WritePolicy();
        // The record is removed after 2 days (seconds)
        wpolicyProcessedPosts.expiration = 172800;
        // Write if record does not exist, otherwise replace it
        wpolicyProcessedPosts.recordExistsAction = RecordExistsAction.REPLACE;
        wpolicyProcessedPosts.timeout = 1000;
        wpolicyProcessedPosts.retryOnTimeout = true;
        wpolicyProcessedPosts.maxRetries = 5;
        wpolicyProcessedPosts.sleepBetweenRetries = 50;

        // Buffer filter
        buffer.keySet().forEach((key) -> {
            Key keyToHash = new Key(
                    // Source as namespace (twttr)
                    "polypus_" + buffer.get(key).getSource(),
                    "ids", // set
                    key
            );
            boolean postExists = false;
            try {
                postExists = aerospikeClient.exists(null, keyToHash);
            } catch (AerospikeException ex) {
                logger.log(Level.SEVERE, null, ex);
                this.reconnectAerospike();
            }
            // Check new posts
            if (!postExists) {
                try {
                    this.aerospikeClient.put(
                            wpolicyProcessedPosts,
                            keyToHash,
                            new Bin("processed", true)
                    );
                } catch (AerospikeException ex) {
                    logger.log(Level.SEVERE, "ids put", ex);

                    this.reconnectAerospike();
                }

                // Write to Aerospike output buffer
                // Polypus identifier
                Bin browkey = new Bin("rowkey", buffer.get(key).getId());
                // Post content
                Bin bcontent = new Bin("content", buffer.get(key).getContent());
                // Post language
                Bin blanguage = new Bin("language", buffer.get(key).getLanguage());
                // External buffer of posts for the next stage
                keyToHash = new Key(
                        "polypus_classifier",
                        "input_buffer",
                        buffer.get(key).getId()
                );

                try {
                    this.aerospikeClient.put(
                            wpolicyOutputBuffer,
                            keyToHash,
                            browkey,
                            bcontent,
                            blanguage
                    );
                } catch (AerospikeException ex) {
                    logger.log(Level.SEVERE, "input_buffer put", ex);
                    this.reconnectAerospike();
                }

                // Keep the post for batch insert in HBase
                Put put = new Put(Bytes.toBytes(buffer.get(key).getId()));

                put.addColumn(Bytes.toBytes(this.hbasePrimaryFamily),
                        Bytes.toBytes("content"),
                        Bytes.toBytes(buffer.get(key).getContent()));
                put.addColumn(Bytes.toBytes(this.hbasePrimaryFamily),
                        Bytes.toBytes("language"),
                        Bytes.toBytes(buffer.get(key).getLanguage()));
                put.addColumn(Bytes.toBytes(this.hbasePrimaryFamily),
                        Bytes.toBytes("post_timestamp"),
                        Bytes.toBytes(buffer.get(key).getPublicationTimestamp()));
                put.addColumn(Bytes.toBytes(this.hbaseSecondaryFamily),
                        Bytes.toBytes("post_id"),
                        Bytes.toBytes(buffer.get(key).getPostId()));
                put.addColumn(Bytes.toBytes(this.hbaseSecondaryFamily),
                        Bytes.toBytes("author_name"),
                        Bytes.toBytes(buffer.get(key).getAuthorName()));
                put.addColumn(Bytes.toBytes(this.hbasePrimaryFamily),
                        Bytes.toBytes("author_nick"),
                        Bytes.toBytes(buffer.get(key).getAuthorNickname()));
                put.addColumn(Bytes.toBytes(this.hbaseSecondaryFamily),
                        Bytes.toBytes("author_id"),
                        Bytes.toBytes(buffer.get(key).getAuthorId()));
                puts.add(put);
            }
        });

        // Writing stored puts to HBase
        try {
            this.hbaseTable.put(puts);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            this.reconnectHbase();
        }
    }
}
