package obj;

public class ArticleInfo {
    public Long id;
    public String article_name;
    public Long supported_time;
    public Long top_label_id;
    public String avatar_id;
    public Long author_id;
    public Long created_time;
    public Byte status;
    public ArticleInfo(){}
    public ArticleInfo(long id,String articleName,long createdTime,long supportedTime,long topLabelId,Long authorId,String avatarId,Byte status)
    {
        this.id=id;
        this.article_name=articleName;
        this.created_time=createdTime;
        this.supported_time=supportedTime;
        this.top_label_id=topLabelId;
        this.author_id=authorId;
        this.avatar_id=avatarId;
        this.status=status;
    }
}
