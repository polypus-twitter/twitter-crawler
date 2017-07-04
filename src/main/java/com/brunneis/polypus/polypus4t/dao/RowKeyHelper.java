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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author brunneis
 */
public class RowKeyHelper {

    private static final Long MAX_LONG = 9223372036854775807L;

    private static final String[] PREFIXES;
    private static final int BUCKETS_NUMBER = 32;
    private static final int SALT_LENGTH = Integer.toString(BUCKETS_NUMBER).length();

    private static String previousKey = "";
    private static int counter = 0;

    private static Random random;
    private static String hostId = "XXXX";

    static {
        PREFIXES = new String[BUCKETS_NUMBER];
        for (byte i = 0; i < BUCKETS_NUMBER; i++) {
            PREFIXES[i] = "";
            int iLength = Integer.toString(i).length();
            if (iLength < SALT_LENGTH) {
                int diff = SALT_LENGTH - iLength;
                for (int j = 0; j < diff; j++) {
                    PREFIXES[i] += "0";
                }
            }
            PREFIXES[i] += i;
        }

        // PID
        String[] pidHost = getPidHost();
        random = new Random(Long.parseLong(pidHost[0]));

        // Hostname sum
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(Bytes.toBytes(pidHost[1]));
            byte[] digest = md.digest();
            hostId = Base64.getEncoder().encodeToString(digest).substring(0, 4);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(RowKeyHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public synchronized static final String generateId(String source) {

        // Inverted timestamp
        String reverseTimestamp = (MAX_LONG - Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis()) + "";

        // Zero padding
        Integer padding = MAX_LONG.toString().length() - reverseTimestamp.length();
        for (int i = 0; i < padding; i++) {
            reverseTimestamp = 0 + reverseTimestamp;
        }

        // Source size check
        if (source.length() != 5) {
            throw new IllegalArgumentException("La longitud de la fuente debe ser de 5 caracteres.");
        }

        String originalKey;
        do {
            // 4-digit random number (PID as seed)
            Integer randomInt = RowKeyHelper.random.nextInt(90) + 10;
            // Identifier build
            originalKey = reverseTimestamp + source + hostId + randomInt;
        } while (originalKey.equals(previousKey));

        previousKey = originalKey;

        return RowKeyHelper.getSaltedKey(originalKey);
    }

    public static String[] getPidHost() {
        String info = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        if (info != null && info.length() > 0) {
            try {
                return info.split("@");
            } catch (Exception ex) {
                return new String[]{"0", "None"};
            }
        }
        return new String[]{"0", "None"};
    }

    public static String getSourceFromKey(String key) {
        int offset = SALT_LENGTH + 19;
        return key.substring(offset, offset + 5);
    }

    public static String getOriginalKey(String saltedKey) {
        return saltedKey.substring(SALT_LENGTH);
    }

    public synchronized static String getSaltedKey(String originalKey) {
        String saltedKey = PREFIXES[counter] + originalKey;
        counter = (counter + 1) % BUCKETS_NUMBER;
        return saltedKey;
    }

}
