package compare;

import corm.ConnectionPool;
import corm.Corm;
import corm.Csession;
import mapper.ArticleInfoMapper;
import mapper.RowMapper;
import obj.Article;
import obj.ArticleInfo;
import org.junit.Test;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CompareTest {
    String connectionURL = "jdbc:mysql://127.0.0.1:3306/ypzj?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String userName = "root";
    String password = "1215287416";
    int testNum = 10000;

    @Test
    public void testCompare() {
        String sql = "select * from article_info_table;";
        Corm orm = new Corm(connectionURL, userName, password);
        Csession csession = orm.getNewSession();
        List<ArticleInfo> list = null;
        try {
            csession = csession.sql(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Long startTime = System.currentTimeMillis();
        for (int i = 0; i < testNum; i++) {
            try{
                csession.resultSet.beforeFirst();
            }catch (SQLException e){
                e.printStackTrace();
            }

            list = csession.find(ArticleInfo.class);

        }
        System.out.println("corm cost is " + (System.currentTimeMillis() - startTime) + "ms");
        csession.exit();


        Connection connection = ConnectionPool.getInstance().getConn();
        PreparedStatement statement = null;
        ResultSet resultSet=null;
        try {
            statement = connection.prepareStatement(sql);
            resultSet=statement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        startTime = System.currentTimeMillis();
        for (int i = 0; i < testNum; i++) {
            try {
                resultSet.beforeFirst();
                list = findAll(new ArticleInfoMapper(), resultSet);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        System.out.println("tradition cost is " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private <E> List<E> findAll(RowMapper<E> rowMapper, ResultSet resultSet) {
        ArrayList<E> ans = new ArrayList();
        try {
            while (resultSet.next()) {
                ans.add(rowMapper.map(resultSet));
            }
            return ans;
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            System.err.println("error in find all");
            return ans;
        }
    }
}
