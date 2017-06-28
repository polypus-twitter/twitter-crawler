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
package com.brunneis.polypus.conf;

import com.brunneis.locker.Locker;
import com.brunneis.sg.exceptions.FileParsingException;
import com.brunneis.sg.io.FileHandler;
import com.brunneis.sg.vo.Document;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public class Conf {
    
    public final static int SG = 001;
    public final static int HBASE = 002;
    public final static int AEROSPIKE = 003;
    public final static int HBASE_AEROSPIKE = 101;
    
    public final static Locker<String> CONF_FILE = new Locker<>();
    public final static Locker<Level> LOGGER_LEVEL = new Locker<>();
    public final static Locker<DBConf> DB_PERSISTENCE = new Locker<>(); // HBase
    public final static Locker<DBConf> DB_BUFFER = new Locker<>(); // Aerospike
    public final static Locker<Integer> STORE_MODE = new Locker<>();
    public final static Locker<String[]> TT_FILES = new Locker<>();
    public final static Locker<ArrayList<String>> LANGUAGES = new Locker<>();
    public final static Locker<HashMap<String, String[]>> TERMS = new Locker<>();
    public final static Locker<Integer> THREADS = new Locker<>();
    public final static Locker<Integer> STREAMING = new Locker<>();
    public final static Locker<Integer> SLEEP = new Locker<>();
    public final static Locker<Integer> MINS = new Locker<>();
    public final static Locker<Integer> BUFFER = new Locker<>();
    public final static Locker<Integer> INCREMENT = new Locker<>();
    public final static Locker<String> TWITTER_CK = new Locker<>();
    public final static Locker<String> TWITTER_CS = new Locker<>();
    public final static Locker<String> TWITTER_AT = new Locker<>();
    public final static Locker<String> TWITTER_ATS = new Locker<>();
    
    public static void loadConf() throws ConfLoadException {
        if (!CONF_FILE.isLocked()) {
            CONF_FILE.set("polypus4t.conf");
        }
        
        if (!LOGGER_LEVEL.isLocked()) {
            LOGGER_LEVEL.set(Level.INFO);
        }

        // Force config file presence
        File file = new File(CONF_FILE.value());
        if (!file.exists()) {
            throw new ConfLoadException();
        }
        
        Properties properties = new Properties();
        InputStream input;
        try {
            input = new FileInputStream(CONF_FILE.value());
            properties.load(input);
            
            if (properties.getProperty("DB_PERSISTENCE") != null) {
                // New instance of DBConf
                switch (properties.getProperty("DB_PERSISTENCE").toUpperCase()) {
                    case "HBASE":
                        DB_PERSISTENCE.set(new HBaseConf());
                        DB_PERSISTENCE.value().CURRENT.set(HBASE);
                        DB_PERSISTENCE.value().NAME.set(properties.getProperty("DB_PERSISTENCE_NAME"));
                        ((HBaseConf) DB_PERSISTENCE.value()).hbasePrimaryFamily.set(properties.getProperty("HBASE_PRIMARY_FAMILY"));
                        ((HBaseConf) DB_PERSISTENCE.value()).hbaseSecondaryFamily.set(properties.getProperty("HBASE_SECONDARY_FAMILY"));
                        ((HBaseConf) DB_PERSISTENCE.value()).hbaseZookeeperQuorum.set(properties.getProperty("HBASE_ZOOKEEPER_QUORUM"));
                        ((HBaseConf) DB_PERSISTENCE.value()).hbaseZookeeperPort.set(properties.getProperty("HBASE_ZOOKEEPER_PORT"));

                        // Optional secondary storage
                        if (properties.getProperty("DB_BUFFER") != null) {
                            STORE_MODE.set(HBASE_AEROSPIKE);
                            switch (properties.getProperty("DB_BUFFER").toUpperCase()) {
                                case "AEROSPIKE":
                                    DB_BUFFER.set(new AerospikeConf());
                                    DB_BUFFER.value().CURRENT.set(AEROSPIKE);

                                    // Aerospike host = localhost, una instancia en cada nodo
                                    ((AerospikeConf) DB_BUFFER.value()).host.set(properties.getProperty("AEROSPIKE_HOST"));
                                    ((AerospikeConf) DB_BUFFER.value()).port.set(Integer.parseInt(properties.getProperty("AEROSPIKE_PORT")));
                                    break;
                                default:
                                    throw new ConfLoadException();
                            }
                        } else {
                            throw new ConfLoadException();
                        }
                        
                        break;
                    default:
                        throw new ConfLoadException();
                }
            } else {
                STORE_MODE.set(SG);
                DB_PERSISTENCE.set(new DBConf());
                DB_PERSISTENCE.value().CURRENT.set(SG);
            }
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if (properties.getProperty("FILES") != null) {
            TT_FILES.set(properties.getProperty("FILES").replaceAll("\\s*,\\s*", ",").split(","));
        } else {
            throw new ConfLoadException("Error loading document.");
        }
        
        Document document;
        TERMS.set(new HashMap<>());
        for (String tt : TT_FILES.value()) {
            try {
                document = FileHandler.loadFile(tt);
                
                List<String> lterms = document.getGroup("terms").getValues();
                String[] aterms = new String[lterms.size()];
                aterms = lterms.toArray(aterms);
                TERMS.value().put(document.getAttribute("LANG").toLowerCase(), aterms);
            } catch (FileParsingException | IOException ex) {
                throw new ConfLoadException("Error loading document.");
            }
        }

        // The languages of the target-terms files are set
        LANGUAGES.set(new ArrayList<>());
        Conf.TERMS.value().keySet().forEach((language) -> {
            LANGUAGES.value().add(language);
        });
        
        try {
            if (properties.getProperty("STREAMING") != null) {
                if (Integer.parseInt(properties.getProperty("STREAMING")) == 0) {
                    STREAMING.set(0);
                } else {
                    STREAMING.set(1);
                }
            } else {
                STREAMING.set(1);
            }
            
            if (properties.getProperty("THREADS") != null) {
                if (Integer.parseInt(properties.getProperty("THREADS")) == 0) {
                    THREADS.set(1);
                } else {
                    THREADS.set(Integer.parseInt(properties.getProperty("THREADS")));
                }
            } else {
                THREADS.set(1);
            }
            
            if (properties.getProperty("SLEEP") != null) {
                SLEEP.set(Integer.parseInt(properties.getProperty("SLEEP")));
            } else {
                SLEEP.set(5);
            }
            
            if (properties.getProperty("MINS") != null) {
                MINS.set(Integer.parseInt(properties.getProperty("MINS")));
            } else {
                MINS.set(0);
            }
            
            if (properties.getProperty("BUFFER") != null) {
                BUFFER.set(Integer.parseInt(properties.getProperty("BUFFER")));
            } else {
                BUFFER.set(10000);
            }
            
            if (properties.getProperty("STEP") != null) {
                INCREMENT.set(Integer.parseInt(properties.getProperty("STEP")));
            } else {
                INCREMENT.set(500);
            }
            
            if (properties.getProperty("TWITTER_CK") != null) {
                TWITTER_CK.set(properties.getProperty("TWITTER_CK"));
            } else {
                TWITTER_CK.set(null);
            }
            
            if (properties.getProperty("TWITTER_CS") != null) {
                TWITTER_CS.set(properties.getProperty("TWITTER_CS"));
            } else {
                TWITTER_CS.set(null);
            }
            
            if (properties.getProperty("TWITTER_AT") != null) {
                TWITTER_AT.set(properties.getProperty("TWITTER_AT"));
            } else {
                TWITTER_AT.set(null);
            }
            
            if (properties.getProperty("TWITTER_ATS") != null) {
                TWITTER_ATS.set(properties.getProperty("TWITTER_ATS"));
            } else {
                TWITTER_ATS.set(null);
            }
            
        } catch (NumberFormatException ex) {
            throw new ConfLoadException();
        }
        
    }
    
}
