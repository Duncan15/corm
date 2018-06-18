package corm;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class Corm {
    private ConnectionPool pool;
    static Logger cormLogger= LoggerFactory.getLogger("corm");
    /*
    only need to initiate once for a db in an application
     */
    public Corm(String url,String username,String password){
        if(url==null||username==null||password==null)throw new NullPointerException("parameters can't be null");
        else {
            ConnectionPool.connectionString=url;
            ConnectionPool.username=username;
            ConnectionPool.password=password;
            pool=ConnectionPool.getInstance();
        }
    }
    public void setLogger(String xmlPath){
        DOMConfigurator.configure(xmlPath);
        cormLogger=LoggerFactory.getLogger("corm");
    }

    public static Logger getCormLogger() {
        return cormLogger;
    }

    /*
        return null if can't get a new session
         */
    public Csession getNewSession(){
        Connection connection=pool.getConn();
        if(connection==null){
            return null;
        }
        return new Csession(connection);
    }
}
