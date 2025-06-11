# milvus2milvus

milvus 数据导入导出工具, 支持 2.5.x 等版本

## (1) Requirements
- jdk8

## (2) Get Started

```shell

# 导入所有库和集合，迁移完成后，需在targer端milvus对集合进行加载后，才可以进行查询
# 如果表结构主键为autoid自动生成的话，则跳过该字段数据的迁移，在target端会自动生成
java -cp milvus2milvus-1.0.0.jar Milvus2Milvus \
--uri '<SRC_MILVUS_URI>' \
--token '<SRC_MILVUS_TOKEN>' \
--t_uri '<TARGET_MILVUS_URI>' \
--t_token '<TARGET_MILVUS_TOKEN>' \
--collections '*'

# 参数详情
--uri <args>   src端的uri信息
--token <args> src端的token信息，token形式为："用户名:密码"
--t_uri <args> target端的uri信息
--t_token <args> target端的token信息，token形式为："用户名:密码"
--collections <args> 待迁移的库表，如 'default.*,test.milvus1,test2.milvus2'，多个用逗号连接，默认迁移所有库表
--skip <args> 待迁移的库表黑名单,迁移时将跳过，如 'test.milvus1'，多个用逗号连接
--skip_schema 跳过target端schema和索引的创建，若跳过则需要手动预先创建，确保target和src的scheme字段结构和数据类型一致
--skip_index  跳过target端索引创建，若跳过则在迁移完成后，需要在target端创建索引才能进行查询

```
## (3) Thanks

如果这个项目对你有帮助，欢迎扫码打赏！

<img src="images/coffee.png" alt="coffee" width="200" height="200">

感谢你的慷慨解囊，你的支持是我前进的动力！
