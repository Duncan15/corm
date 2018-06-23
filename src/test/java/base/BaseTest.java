package base;

import base.obj.Article;
import corm.Corm;
import corm.Csession;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class BaseTest {
    String connectionURL="jdbc:mysql://127.0.0.1:3306/ypzj?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String userName="root";
    String password="1215287416";
    @Test
    public void testFind(){
        Corm orm=new Corm(connectionURL,userName,password);
        Csession csession=orm.getNewSession();
        List<Article> list=null;
        try{
            list=csession.sql("select * from article_info_table as a inner join article_content_table as b on a.id=b.id;").find(Article.class);
        }catch (SQLException e){
            e.printStackTrace();
        }

        for(Article each:list){
            System.out.print(each.articleContent_ext.content);
            System.out.println(each.articleInfo_ext.status);
        }
    }
}
