# milvus2milvus

milvus 数据导入导出工具, 支持 2.5.x 等版本

## (1) Requirements
- jdk8

## (2) Get Started

```shell

# 导入所有库和集合，迁移完成后，需在targer端milvus对集合进行加载后，才可以进行查询
java -cp milvus2milvus-1.0.0.jar Milvus2Milvus --uri <SRC_MILVUS_URI> --token <SRC_MILVUS_TOKEN> --t_uri <TARGET_MILVUS_URI> --t_token <TARGET_MILVUS_TOKEN> --collections '*' 

# 其余参数说明
--collections 'default.*,test.milvus1,test2.milvus2'  # 指定需要迁移的集合，多个用逗号进行分隔
--batchsize 1000  # 每次读取和写入的milvus数据大小
--skip 'default.milvus3' # 黑名单，跳过需要迁移的集合
--skip_index   # 不创建索引，target端需要在查询前创建索引并加载集合
--skip_schema  # 不创建schema，target端需要预先创建schema
```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
