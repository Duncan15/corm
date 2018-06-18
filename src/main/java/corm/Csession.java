package corm;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Csession {
    private Connection conn;
    private PreparedStatement statement;
    public Csession(Connection connection){
        this.conn=connection;
    }
    public void exit(){
        ConnectionPool.returnConn(conn);
    }
    public Csession sql(String sql,Object... param)throws SQLException{
        /*
        if can't return keys,this param will be ignore,don't worry about this
         */
        statement=conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        setParam(param);
        statement.execute();
        return this;
    }
    public<T> List<T> find(Class<T> cl){
        ResultSet resultSet=null;
        List<T> ans=new ArrayList<>();
        try{
            resultSet=statement.getResultSet();
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when getting result set",e);
            return ans;
        }
        try{
            while(resultSet.next()){
                T row=inject(cl,resultSet);
                if(row!=null){
                    ans.add(row);
                }
            }
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when iterate the result",e);
        }finally {
            closeResultSet(resultSet);
        }
        return ans;
    }
    public<T> T get(Class<T> cl){
        ResultSet resultSet=null;
        T ans=null;
        try{
            resultSet=statement.getResultSet();
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when getting result set",e);
            return ans;
        }
        try{
            if(resultSet.first()) ans=inject(cl,resultSet);
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when iterate the result",e);
        }finally {
            closeResultSet(resultSet);
        }
        return ans;
    }
    /*
    inner method: responsible for inject data into struct
    syntax: all the field is public, and if is extend field ,set the name sufix as _ext ,like bellow
    class sample{
        public User sample1_ext;
        public Account sample2_ext;
    }

    if is original field, set it's name the same with the one from the sql result
     */
    private<T> T inject(Class<T> cl,ResultSet resultSet){
        T ans=null;
        try{
            ans=cl.newInstance();
        }catch (InstantiationException e){
            Corm.cormLogger.error("error happen when create orm object",e);
            return ans;
        }catch (IllegalAccessException e){
            Corm.cormLogger.error("error happen when create orm object",e);
            return ans;
        }

        Field[] fields=cl.getDeclaredFields();
        for(Field e:fields){
            if(e.getName().endsWith("_ext")){
                try{
                    e.set(ans,inject(e.getType(),resultSet));
                }catch (IllegalAccessException err){
                    Corm.cormLogger.error("error happen when access the field"+e.getName(),err);
                }
            }else {
                try{
                    Object value=resultSet.getObject(e.getName());
                    e.set(ans,value);
                }catch (SQLException err){
                    Corm.cormLogger.error("error happen when get the field from result set:"+e.getName(),err);
                }catch (IllegalAccessException err){
                    Corm.cormLogger.error("error happen when access the field"+e.getName(),err);
                }
            }
        }
        return ans;
    }
    private void closeResultSet(ResultSet r){
        try{
            r.close();
        }catch (SQLException e){
            Corm.cormLogger.error("error happen when close result set",e);
        }
    }
    /*
	param here should fit the order in the sql
	 */
    private void setParam(Object... param) throws SQLException {
        for(int i=1;i<=param.length;i++){
            int index=i-1;
            Object cur=param[index];
            if(cur instanceof Long){//long
                statement.setLong(i,(Long)cur);
            }else if(cur instanceof Integer) {//int
                statement.setInt(i,(Integer)cur);
            }else if(cur instanceof Byte){//byte
                statement.setByte(i,(Byte)cur);
            }else if(cur instanceof Float) {//float
                statement.setFloat(i,(Float)cur);
            }else if(cur instanceof Double){//double
                statement.setDouble(i,(Double)cur);
            }else if(cur instanceof Short){//short
                statement.setShort(i,(Short)cur);
            }else if(cur instanceof String){//string
                statement.setString(i,(String)cur);
            }else if(cur instanceof byte[]){//byte[]
                statement.setBytes(i,(byte[])cur);
            }else if(cur instanceof Timestamp){//timestamp
                statement.setTimestamp(i,(Timestamp) cur);
            }else if(cur==null){
                statement.setNull(i,Types.INTEGER);
            }
        }
    }

}
