package com.cwc.corm;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Objects;

public class Corm {

    private ConnectionPool pool;

    static Logger cormLogger = LoggerFactory.getLogger(Corm.class);

    /**
     * only need to initiate once for a db in an application
     * @param url jdbcURL
     * @param username
     * @param password
     */
    public Corm(String url, String username, String password) {
        if (Objects.isNull(url) || Objects.isNull(username) || Objects.isNull(password)) {
            throw new NullPointerException("parameters can't be null");
        } else {
            ConnectionPool.connectionString = url;
            ConnectionPool.username = username;
            ConnectionPool.password = password;
            ConnectionPool.poolMinSize = 5;
            ConnectionPool.poolMaxSize = 20;
            pool = ConnectionPool.getInstance();
        }
    }

    /**
     * provide the support to configure the log4j configuration file
     * @param xmlPath
     */
    public void setLogger(String xmlPath) {
        DOMConfigurator.configure(xmlPath);
        cormLogger = LoggerFactory.getLogger(Corm.class);
    }

    public static Logger getCormLogger() {
        return cormLogger;
    }


    /**
     * return null if can't get a new session
     * @return
     */
    public Csession getNewSession(){
        Connection connection = pool.getConn();
        if (Objects.isNull(connection)) {
            return null;
        }
        return new Csession(connection);
    }
}
