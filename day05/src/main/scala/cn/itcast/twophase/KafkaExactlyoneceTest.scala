package cn.itcast.twophase

import java.util.Properties

//测试flink+kafka 一致性语义效果
// 使用flink消费kafka中数据
object KafkaExactlyoneceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var topic = "test"
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    //设置消费者的隔离级别，默认是读取未提交数据 read_uncommitted
    prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
    val kafkaDs: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      prop
    ))
    //打印
    kafkaDs.print("测试kafka 一致性结果数据>>")
    //启动
    env.execute()

  }
}
