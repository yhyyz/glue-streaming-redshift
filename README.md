#### 一、背景
* 客户需要从MSK消费数据，实时写入到Redshift.并且要求数据可以按需自动存储为Super或者自动展平嵌套结构数据。
* kafka offset提交，用于消费速度监控。
* 可以自定义字段处理逻辑，比如敏感字段删除或者脱敏
* 自定义数据外层结构

#### 二、代码实现
##### 2.1. offfset committer
Spark3.x offset完全由checkpoint接管，offset不会向kafka提交，下面这个plugin可以再每次checkpoint向kafka提交offset，主要目的是为了方便监控kafka的offset的生产消费速度
```shell
# glue 4.0下载下面的jar放到S3, Dependent JARs path中配置S3 路径
https://dxs9dnjebzm6y.cloudfront.net/tmp/spark3.3-sql-kafka-offset-committer-1.0.jar
```

##### 2.2 glue code
* 需要Gule 4.0, Spark3.3 版本
* [kafka-redshift.py](kafka-redshift.py)作业参数一部分放到了外边，通过参数传递，redshift的信息直接写到了代码里，上生产可以使用SM管理密码。
```python
# 用户明密码可以用secret manager管理，如下是例子
def get_secret():
    secret_name = "${secret_name}"
    region_name = "${aws_region}"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret


secret_dict = json.loads(get_secret())
```

##### 2.3 glue job conf
```properties
--aws_region  xxx,eg: us-east-1
--checkpoint_interval  30   # 单位是秒,表示多长时间做一次checkpoint, 一般建议30~60秒
--checkpoint_location s3://xxxx/glue-spark-checkpoint/  #spark checkpoint目录
--conf spark.sql.streaming.streamingQueryListeners=net.heartsavior.spark.KafkaOffsetCommitterListener
--consumer_group  cg-01 #消费者组
--kafka_broker  xxxx:9092,xxx9092
--startingOffsets  latest or earliest or timestamp
--topic  test_data # 多个topic逗号分割
```

