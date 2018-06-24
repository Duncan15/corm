package mapper;

import obj.ArticleInfo;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ArticleInfoMapper extends RowMapper {

    @Override
    public ArticleInfo map(ResultSet resultSet) throws SQLException {
        if (resultSet==null)return new ArticleInfo();
        // TODO Auto-generated method stub
        long id=resultSet.getLong("id");
        String articleName=resultSet.getString("article_name");
        long createdTime=resultSet.getLong("created_time");
        long supportedTime=resultSet.getLong("supported_time");
        long topLabelId=resultSet.getLong("top_label_id");
        Long authorId=resultSet.getLong("author_id");
        String avatarId=resultSet.getString("avatar_id");
        Byte status=resultSet.getByte("status");
        return new ArticleInfo(id, articleName, createdTime, supportedTime, topLabelId, authorId,avatarId,status);
    }

}
