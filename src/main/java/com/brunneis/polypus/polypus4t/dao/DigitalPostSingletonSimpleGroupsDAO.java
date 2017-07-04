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
import com.brunneis.sg.exceptions.BadItemException;
import com.brunneis.sg.exceptions.DuplicateNameException;
import com.brunneis.sg.exceptions.FileParsingException;
import com.brunneis.sg.io.FileHandler;
import com.brunneis.sg.vo.Document;
import com.brunneis.sg.vo.SimpleGroup;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public class DigitalPostSingletonSimpleGroupsDAO implements DigitalPostDAO {

    private static final DigitalPostSingletonSimpleGroupsDAO INSTANCE = new DigitalPostSingletonSimpleGroupsDAO();

    private DigitalPostSingletonSimpleGroupsDAO() {
        super();
    }

    public static DigitalPostDAO getInstance() {
        return INSTANCE;
    }

    @Override
    public synchronized void dumpBuffer(HashMap<String, DigitalPost> buffer) {

        Document document;
        try {
            document = FileHandler.loadFile("twitter_crawler_output.sg");
        } catch (FileNotFoundException ex) {
            document = new Document();
        } catch (IOException | FileParsingException ex) {
            Logger.getLogger(DigitalPostSingletonSimpleGroupsDAO.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        SimpleGroup group = document.getGroup("dp");
        if (group == null) {
            group = new SimpleGroup();
            group.setName("dp");
            document = new Document();
            try {
                document.addAttribute("TITLE", "twitter_crawler_output");
            } catch (BadItemException ex) {
                Logger.getLogger(DigitalPostSingletonSimpleGroupsDAO.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
            try {
                document.addGroup(group);
            } catch (DuplicateNameException ex) {
                Logger.getLogger(DigitalPostSingletonSimpleGroupsDAO.class.getName()).log(Level.SEVERE, null, ex);
                return;
            }
        }

        for (String id : buffer.keySet()) {
            try {
                group.addItem(id,
                        buffer.get(id).getContent().replaceAll("[\\n\\r]+", "") + "|"
                        + buffer.get(id).getLanguage() + "|"
                        + buffer.get(id).getPublicationTimestamp() + "|"
                        + buffer.get(id).getPostId() + "|"
                        + buffer.get(id).getAuthorName() + "|"
                        + buffer.get(id).getAuthorNickname() + "|"
                        + buffer.get(id).getAuthorId() + "|"
                );
            } catch (BadItemException ex) {
                Logger.getLogger(DigitalPostSingletonSimpleGroupsDAO.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        try {
            FileHandler.writeFile(document);
        } catch (IOException ex) {
            Logger.getLogger(DigitalPostSingletonSimpleGroupsDAO.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        buffer.clear();
    }

    @Override
    public void connect() {
    }

    @Override
    public void disconnect() {
    }

}
