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
package com.brunneis.polypus.polypus4t.threads;

import com.brunneis.polypus.polypus4t.conf.Conf;
import com.brunneis.polypus.polypus4t.vo.DigitalPost;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public final class StreamingMiner extends Thread {

    private final HashMap<String, DigitalPost> buffer;
    private final TwitterStream twitterStream;
    private long startTime;
    private final ThreadHelper th;
    static int mcounter = 0;

    private Logger logger;

    public StreamingMiner(String name) {
        logger = Logger.getLogger(StreamingMiner.class.getName());
        logger.setLevel(Conf.LOGGER_LEVEL.value());

        super.setName(name);
        this.buffer = new HashMap<>();
        this.th = new ThreadHelper();

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false).setOAuthConsumerKey(Conf.TWITTER_CK.value())
                .setOAuthConsumerSecret(Conf.TWITTER_CS.value())
                .setOAuthAccessToken(Conf.TWITTER_AT.value())
                .setOAuthAccessTokenSecret(Conf.TWITTER_ATS.value());
        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        this.twitterStream = tf.getInstance();
    }

    @Override
    public void run() {
        logger.log(Level.INFO, "thread {0} | starting job...", getName());

        // Initial date in ms
        this.startTime = new Date().getTime();

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {

                // Every matching tweet is saved in the HashMap buffer
                if (Conf.LANGUAGES.value().contains(status.getLang())) {
                    if (buffer.size() % 50 == 0) {
                        logger.log(
                                Level.FINE,
                                "thread {0} | counter: {1}",
                                new Object[]{getName(), buffer.size()}
                        );
                    }
                    String content
                            = status.getText();
                    String language
                            = status.getLang();
                    String postId
                            = String.valueOf(status.getId());
                    String publicationTimestamp
                            = String.valueOf(status.getCreatedAt().getTime());
                    String authorNickname
                            = status.getUser().getScreenName();
                    String authorId
                            = String.valueOf(status.getUser().getId());
                    String authorName
                            = String.valueOf(status.getUser().getName());

                    DigitalPost tweet = new DigitalPost(
                            "twttr",
                            language,
                            content,
                            authorName,
                            authorNickname,
                            authorId,
                            postId,
                            publicationTimestamp,
                            // getRelevance(authorNickname)
                            null
                    );

                    synchronized (buffer) {
                        // The tweet is added to the buffer if it is not
                        // already there
                        if (!buffer.containsKey(tweet.getPostId())) {
                            buffer.put(tweet.getPostId(), tweet);
                        }
                    }
                }

                if (!th.inTime(startTime)) {
                    // Sync only if not in time
                    synchronized (buffer) {
                        logger.log(Level.INFO,
                                "thread {0} | mins_left: {1} | dumping...",
                                new Object[]{getName(),
                                    th.getMinsLeft(startTime)}
                        );
                        th.dumpBuffer(buffer);
                        logger.log(Level.INFO,
                                "thread {0} | job finished.", getName());
                        twitterStream.shutdown();
                        twitterStream.removeListener(this);
                        return;
                    }
                }

                if (buffer.size() >= Conf.BUFFER.value()) {
                    // Sync only if not in time
                    synchronized (buffer) {
                        logger.log(Level.INFO,
                                "thread {0} | mins_left: {1} | dumping...",
                                new Object[]{getName(),
                                    th.getMinsLeft(startTime)}
                        );
                        th.dumpBuffer(buffer);
                    }
                }

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice
            ) {
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses
            ) {
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId
            ) {
            }

            @Override
            public void onStallWarning(StallWarning warning
            ) {
            }

            @Override
            public void onException(Exception ex
            ) {
            }

        };

        twitterStream.addListener(listener);
        twitterStream.sample();
    }
}
