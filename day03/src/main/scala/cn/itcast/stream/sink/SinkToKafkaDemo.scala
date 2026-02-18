package cn.itcast.stream.sink

import java.util.Properties

/*
flink程序计算结果保存到kafka
 */
//定义student case class

case class Student(id: Int, name: String, age: Int)

object SinkToKafkaDemo {
  def main(args: Array[String]): Unit = {
    /*
    flink读取数据然后把数据写入kafka中
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载source
    val stuDs: DataStream[Student] = env.fromElements(Student(0, "tony", 18))
    // 3 直接使用flinkkafkaproducer来生产数据到kafka
    //3.1 准备一个flinkkafkaproducer对象
    //写入kafka的数据类型
    //param1
    var topic="test"
    //param2
    val keyedSerializationWrapper: KeyedSerializationSchemaWrapper[String] =
      new KeyedSerializationSchemaWrapper(new SimpleStringSchema())
   //param3
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node2:9092")
    val flinkKafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](
      topic,keyedSerializationWrapper,prop)
    // 4 sink 操作
    stuDs.map(_.toString).addSink(flinkKafkaProducer)
    // 5 执行
    env.execute()
  }
}

