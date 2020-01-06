**1.4.0** (2020/01/03)
- 新增 user_unset 接口，支持删除用户属性
- BatchConsumer 性能优化：支持配置压缩模式；移除 Base64 编码
- DebugConsumer 优化: 在服务端对数据进行更完备准确地校验

**1.3.1** (2019/09/26)
- 去除 LoggerConsumer 默认文件大小 1G 上限. 用户可自行配置按天，小时，大小切分
- ExampleSDK 案例优化

**1.3.0** (2019/09/21)
- 去除 KafkaProduce, 避免过多的依赖

**v1.2.0** (2019/09/12)
- 新增 DebugConsumer, 便于调试接口 
- 优化 LoggerConsumer, 支持按小时切分日志文件
- 优化代码，提升稳定性

**v1.1.17** (2019/08/23)
- 优化数据上报异常时异常打印提示
- BatchConsumer 请求异常返回码提醒

**v1.1.16** (2019/05/30) 
- 解决 LoggerConsumer 多线程下会出现关闭不 flush 数据的 bug
- 解决 BatchConsumer 多线程下数据重复的情况

**v1.1.15** (2019/04/28)
- 修复 Java 1.7 兼容性 bug
- LoggerConsumer 不根据时间间隔落盘

**v1.1.14** (2019/04/25)
- 兼容 Java 1.7
- 优化了loggerConsumer的上报机制

**v1.1.13** (2019/04/11)
- 优化数据上报的性能及稳定性
- 调整了Consumer 的默认参数
