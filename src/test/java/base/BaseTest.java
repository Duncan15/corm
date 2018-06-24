package base;

import obj.Article;
import corm.Corm;
import corm.Csession;
import obj.ArticleInfo;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

public class BaseTest {
    String connectionURL="jdbc:mysql://127.0.0.1:3306/ypzj?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    String userName="root";
    String password="1215287416";
    int roundNum=10000;
    @Test
    public void testFind(){
        Corm orm=new Corm(connectionURL,userName,password);
        Csession csession=orm.getNewSession();
        List<Article> list=null;
        try{
            list=csession.sql("select * from article_info_table as a inner join article_content_table as b using(id);").find(Article.class);
        }catch (SQLException e){
            e.printStackTrace();
        }

        for(Article each:list){
            System.out.print(each.articleContent_ext.content);
            System.out.println(each.articleInfo_ext.status);

        }
        csession.exit();
    }
    @Test
    public void testReflect(){
        Long startTime=System.currentTimeMillis();
        for(int i=0;i<roundNum;i++){
            reflectInject(ArticleInfo.class);
        }
        System.out.println("reflect time is "+(System.currentTimeMillis()-startTime)+" ms");
        startTime=System.currentTimeMillis();
        for(int i=0;i<roundNum;i++){
            ArticleInfo articleInfo=new ArticleInfo();
            articleInfo.avatar_id="";
            articleInfo.author_id=0l;
            articleInfo.top_label_id=0l;
            articleInfo.supported_time=0l;
            articleInfo.created_time=0l;
            articleInfo.article_name="";
            articleInfo.status=0;
            articleInfo.id=0l;
        }
        System.out.println("traditional time is "+(System.currentTimeMillis()-startTime)+" ms");
    }
    private<T> T reflectInject(Class<T> tClass){
        T ans=null;
        try {
            ans=tClass.newInstance();
            Field[] fields=tClass.getDeclaredFields();
            for(Field e:fields){
                Object value=new Integer(1);
                if(value==null){
                    if(e.getType().isPrimitive()){
                        e.set(ans,0);
                    }else {
                        e.set(ans,null);
                    }
                }else {
                    value=e.getType().getConstructor(String.class).newInstance(value.toString());

                    e.set(ans,value);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return ans;
    }
}
