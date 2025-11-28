
# NextFS
主要由interceptor和daemon两个部分组成。
interceptor主要通过[syscall_intercept库](https://github.com/pmem/syscall_intercept.git)实现，负责拦截应用发起的系统调用并将被拦截的调用通过无所并发队列发送给daemon处理。

daemon主要由两个线程池构成，一个是用于处理来自本地intercptor的请求，一个适用于处理来自其它节点的rpc的请求，两个线程池的主要区别是生产者线程的不同：ipc侧的生产者由一个dispatch线程构成，该线程既作为与interceptor通信的无锁并发队列的消费者，同时也是daemon侧线程池任务队列的生产者。rpc侧的任务队列的生产者由rpc模块的cq poller构成，主机的每台网卡上都会有一个cq poller线程用于取出发给本机rdma rpc请求，并将其塞进rpc侧任务队列。
![](/Readme/2.png)
两个线程池的消费者都主要用于处理被拦截后的系统调用请求，区别在于ipc侧的是一手请求，rpc侧的都是二手的转发的请求，然而这种区别没有什么实质上的影响，之前提到的阻塞系统调用的问题，只需要让阻塞发生在interceptor侧就行了，daemon通过修改IpcState进行同步即可。因此两个线程池或许可以合并为一个线程池。显然下面的图比上面的图要简明不少。
![](/Readme/3.png)
## 环境安装
通过根目录下 deps_install.sh 脚本安装依赖，依赖库均存放在 nextfs_deps/ 下。
``` bash
. deps_install.sh
```

项目代码第一次编译使用脚本`build.sh`编译，之后通过以下命令重新编译：
```bash
. build.sh rebuild
```
## todo
- 大文件的 A table 散列
- fsync 如何实现 ?
- 近根热点缓存
- 大目录的 dents 分裂
- write 的数据从 ipc 直接进入 rpc，减少一次拷贝
