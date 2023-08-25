# 作业描述

## 架构图

![e385286d-6950-4e8f-acbc-560c77ff6808](https://raw.githubusercontent.com/mayrainLN/picGo/main/img/202308251725092.svg)

**动手实现一个拥有最基础的文件操作的分布式文件存储系统**

1. 支持文件创建(create、createDirectory)、查看属性信息(listStatus，getStatus)、删除(delete)           
2. 支持文件open
3. 支持文件的write
4. 支持文件read
5. 获取集群信息（主节点信息，从节点信息）
6. 可采用http 协议模拟以上文件操作功能
7. 要求按照文件元数据、内容数据的方式存储设计实现，元数据的定义为包含文件的基本信息如文件名、大小、创建、修改时间、内容数据的三副本索引等，内容数据为实际文件内所存储的内容信息
8. 元数据服务高可用，元数据的操作支持分布式的一致性
9. 文件内容数据，可支持3副本写（强一致写）、随机一个副本读（实现读写一致性）
10. 3副本分布可支持合理分区，消除热点
11. 对于一个稳定运行的系统，可以主动发现文件副本不足隐患，同时可以自我修复补足不足3副本的数据
12. 最后运用前几周学习过的的maven打包知识，及Linux下的程序部署所需的shell 脚本等知识，可以将程序部署到服务器上运行起来

## 考核要求

1. 服务可以通过启动脚本，一键启动
2. 集群规模最小为两个metaServer，一主一从（默认），如果是raft方式实现则最少3个节点，交作业时可特殊说明采用raft实现，四个dataserver，保证三副本写。最终所有程序会部署在一个机器上，通过不同端口号来模拟多节点的部署方式，可选端口范围8000~9999
3. SDK可暴露出来集群的metaServer地址及dataServer地址
4. 系统内相关心跳超时时间一律默认小于30s，如果实现了fsck+recovery，则扫描、恢复周期需要小于等于120s
5. 程序中用到的第三方组件zookeeper，要求可以通过启动参数来指定 如-Dzookeeper.addr=xxxxx:2181,aaaa:2181（**验收环境zk地址：10.0.0.201:2181**）
6. 最终业务需要编译完成后放入统一目录下，目录名称workpublish，下面的文件目录包括
   1.  bin，一键启动所有服务的启动脚本，名称为start.sh
   2.  metaServer，元数据服务，内部可包含所有服务的lib及主服务jar、日志目录等
   3.  dataServer，内容数据服务，内部可包含所有服务的lib及主服务jar、日志目录等
   4.  easyClient，内部包含client的SDK的jar文件easyClient-1.0.jar

# 个人实现

## MetaServer

由于老师提到了使用Raft是进行实现是加分项，我就以为用ZK是为了用它的Zab算法实现元数据同步，和Raft的作业一样。
所以将ZK作为元数据的存储层。

直到交作业的最后第三天，我才意识到最开始思路就不太对。

后来问了老师，对话如下：

> 老师我有个疑问： 用SOFARaft做元数据的同步，就是借助Raft实现元数据的状态机 用ZK存储元数据，不也是借助Zab去实现元数据的状态机吗？只不过日志的提交和复制zk帮我们实现好了 有同学觉得相比用ZK存元数据，这样是加分的： 用MetaServer去存储元数据，用那种主从同步的方式去实现元数据的一致，ZK只是做服务发现用。 他们的想法是对的吗？真的是加分的吗？我怎么感觉有种脱裤子放屁的感觉...花更大的力气做出来一个可用性和一致性更差的版本...
>
> 回答：
> 其实不是的，在考虑性能和大规模数据的场景下，直接存zk是不行的，因为zk只是一个能保证数据一致的注册中心，在分布式服务中主要担任的是个投票选举的角色，真正数据存储都是有ms和ds来完成，zk性能和存储数据大小都是有上限的，而且上限很低

正确构想如下：

ZK只是作为服务注册、选举的组件，不作为数据层存储任何数据。

因此设计MetaServer存储对象如下：

```java
/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/10 18:51
 * @description : 原本文件元信息都是存在ZK上。考虑到zk的不适合存储大量数据，迁移到metaServer中。补充树形结构信息
 */
@Deprecated
@Data
@Accessors(chain = true)
@NoArgsConstructor
public class FileTree extends StatInfo {
    // 文件/目录 名
    private String name;
    // 子文件/目录  key:文件(夹)名
    private ConcurrentHashMap<String, FileTree> children;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConcurrentHashMap<String, FileTree> getChildren() {
        return children;
    }

    public void setChildren(ConcurrentHashMap<String, FileTree> children) {
        this.children = children;
    }
}

```

对DB操作的封装

```java
/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/10 19:18
 * @description : 本类作为元信息的存储层
 * 问题是之前的结构是按照zk存储元信息写的，改动存储方式要改很多地方
 * 先确定能上线，有时间再改动吧
 */
@Deprecated
@Slf4j
public class Database {
    public static FileTree fileTree;

    // 创建文件，如果目录不存在，需要创建目录

    /**
     * @param path 文件逻辑路径
     * @param realPathMap ds实例->文件实际路径
     * @return
     */
    public static boolean create(String path, Map<DataServerInstance, String> realPathMap){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        StringBuilder currFullPath = new StringBuilder(PrefixConstants.DB_BASE_PATH_META_DATA);
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            FileTree curr = parent.getChildren().get(currName);
            currFullPath = currFullPath.append("/").append(currName);
            if(i == partPath.length - 1){
                // 创建文件
                curr = FileTree.initFile(currName);
                curr.setPath(currFullPath.toString());
                List<ReplicaData> ReplicaDatas= new ArrayList<>();
                for (Map.Entry<DataServerInstance, String> entry : realPathMap.entrySet()) {
                    ReplicaData replicaData = new ReplicaData();
                    replicaData.setId(UUID.randomUUID().toString());
                    String dsAddr = entry.getKey().getHost()+":"+entry.getKey().getPort();
                    replicaData.setDsNode(dsAddr);
                    replicaData.setPath(entry.getValue());
                    ReplicaDatas.add(replicaData);
                }
                curr.setReplicaData(ReplicaDatas);
                parent.getChildren().put(currName,curr);
                return true;
            }

            if(curr == null){
                // 父目录不存在，创建父目录
                curr = FileTree.initDir(currName);
                curr.setPath(currFullPath.toString());
                parent.getChildren().put(currName,curr);
                parent = curr;
            }else{
                parent = curr;
            }
        }
        return false;
    }
}
...........
```

需要给MetaServer增加持久化的能力。

可以采取定时快照+WAL的方案

### 流程

两个MetaServer启动时通过ZK来选举出来Master（临时顺序节点，序号小的为主）。
本地内存有一个是否为主的变量，根据监听到的ZK中的信息来确定谁是主、什么时候切换到主等...

```java
@Slf4j
@Deprecated
/**
 * 本类用于MetaServer选举，以及主从切换
 * 目前的项目结构暂时不不适合用MetaServer存储元信息，感觉改不完了，后续有时间再采取这种方式
 */
public class ZkUtil {
    private static CuratorFramework client;
    public static final String ZK_PATH_META_SERVER_INFO = "/MinFS/metaServerInfo";

    public static volatile boolean isMaster = false;

    public static volatile String masterAddr ;

    public static volatile String slaveAddr ;

    static {
        int heartbeatTimeoutMs = 30000; // 30秒
        initZkClient(heartbeatTimeoutMs);
        init();
        addListener();
    }

    public static CuratorFramework client() {
        return client;
    }

    /**
     * 1. 初始化主从路由表、主从标志位。根据目录下的节点数量即可确定当前是主还是从
     * 2. 如果本机是从节点，添加监听器，监听主节点的健康情况，主节点挂到后自动完成切换
     * @return
     */
    @SneakyThrows
    private static void init() {
        List<String> nodeNameList = client.getChildren().forPath(ZK_PATH_META_SERVER_INFO);
        Collections.sort(nodeNameList);
        String masterNode = null, slaveNode = null;
        if(nodeNameList.isEmpty()){
            log.error("无可用MetaServer");
            throw new RuntimeException("无可用MetaServer");
        }
        if(nodeNameList.size() == 1){
            log.info("当前MetaServer Master为：" + nodeNameList.get(0));
            masterNode = nodeNameList.get(0);
            byte[] bytes = client.getData().forPath(ZK_PATH_META_SERVER_INFO + "/" + masterNode);
            Map<String, String> masterMeta = JSONUtil.toBean(new String(bytes), HashMap.class);
            masterAddr = masterMeta.get("host")+":"+masterMeta.get("port");
            // 更新本机主从标志位
            isMaster = true;
        }
        if(nodeNameList.size() == 2){
            log.info("当前MetaServer Slave为：" + nodeNameList.get(1));
            String slaveNodeName = nodeNameList.get(1);
            byte[] bytes = client.getData().forPath(ZK_PATH_META_SERVER_INFO + "/" + slaveNodeName);
            Map<String, String> slaveMeta = JSONUtil.toBean(new String(bytes), HashMap.class);
            masterAddr = slaveMeta.get("host")+":"+slaveMeta.get("port");
        }
    }

    private static void initZkClient(int heartbeatTimeoutMs) {
        // 连接zookeeper
        String zkAddr = System.getProperty("zookeeper.addr");
        client = CuratorFrameworkFactory.newClient(
                zkAddr,
                heartbeatTimeoutMs, // 设置心跳超时时间
                heartbeatTimeoutMs, // 设置连接超时时间
                new ExponentialBackoffRetry(1000, 3)
        );
        client.start();
    }

    @SneakyThrows
    private static void addListener(){
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, ZK_PATH_META_SERVER_INFO, true);
        pathChildrenCache.start();
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        log.info("新增从节点");
                        HashMap hashMap = JSONUtil.toBean(new String(event.getData().getData()), HashMap.class);
                        slaveAddr = hashMap.get("host")+":"+hashMap.get("port");
                        break;
                    case CHILD_REMOVED:
                        if(!isMaster) {
                            log.info("从节点监听到：主节点下线");
                            log.info("开始故障转移");
                            isMaster = true;
                            masterAddr = slaveAddr;
                            slaveAddr = null;
                            log.info("本节点切换为主节点，开始对外提供服务");
                        }else{
                            // 当前是主节点，从节点下线
                            log.info("主节点监听到：从节点下线");
                            slaveAddr = null;
                        }
                        break;
                    default:
                        break;
                }
            }
        });

        // 阻塞，保持监听状态
        Thread.sleep(Long.MAX_VALUE);
    }
}
```

以创建、写入、读取文件为例。

客户端请求MetaServer创建文件，此时在MetaServer中创建元信息。主节点收到信息后，修改本地数据层，并同步请求从节点修改元数据。

为了保持数据一致，这个请求是同步请求，在从节点ack之前，不会响应客户端。超时就响应客户端失败或者重试。

选出三个剩余容量最大的Dataserver后，填入元信息中。
此时文件还没有commit，所以需要在元信息中设置一个标志位进行标识。

元数据创建成功后，返回客户端List<Dataserver>。后续客户端直接读写操作Dataserver。

最终客户端Close输出流的时候，向MetaServer发起commit请求。最终才算写入文件成功。

由于写请求不及格MetaServer，所以需要自行维护size等信息，在commit的时候传入给MetaServer。



由于时间有限来不及修改，因此还是使用的最开始的版本：所有请求都通过MetaServer中转，MetaServer作为一个无状态的服务，也就不需要主从同步，只需要在客户端做选举的逻辑即可。

## DataServer

由于需要本机启动四个服务，所以在base目录下加了一层隔离：

---localhost-9000
	---default
    	---dir1
		    file1.txt
		---dir2
---localhost-9001
---localhost-9002
---localhost-9003

DataServer启动时，会检查此目录，重新注册文件数量、已用容量等信息。

注册使用临时节点。

可以优化的点：每次负载均衡都去请求了一下ZK。其实可以借鉴选举的那种监听思路，设置监听器，发生时间时更新本地ds列表。

## Client SDK

对输入、输出流的调用其实都是代理成Http调用。

对于FileSystem，在客户端统一拼接成逻辑全路径（默认以default开头），减少Dataserver和MetaServer对路径的处理。

对http调用封装：

```java
public class HttpClientUtil {
    private static HttpClient httpClient;
    public static HttpClient createHttpClient(HttpClientConfig config) {

        int socketSendBufferSizeHint = config.getSocketSendBufferSizeHint();
        int socketReceiveBufferSizeHint = config.getSocketReceiveBufferSizeHint();
        int buffersize = 0;
        if (socketSendBufferSizeHint > 0 || socketReceiveBufferSizeHint > 0) {
            buffersize = Math.max(socketSendBufferSizeHint, socketReceiveBufferSizeHint);
        }
        SocketConfig soConfig = SocketConfig.custom()
                .setTcpNoDelay(true).setSndBufSize(buffersize)
                .setSoTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                .build();
        ConnectionConfig coConfig = ConnectionConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .build();
        RequestConfig reConfig;
        RequestConfig.Builder builder= RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectionTimeOut()))
                .setResponseTimeout(Timeout.ofMilliseconds(config.getSocketTimeOut()))
                ;
        reConfig=builder.build();
        PlainConnectionSocketFactory sf = PlainConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> r = RegistryBuilder.<ConnectionSocketFactory> create().register("http", sf).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(r);
        connectionManager.setMaxTotal(config.getMaxConnections());
        connectionManager.setDefaultMaxPerRoute(connectionManager.getMaxTotal());
        connectionManager.setDefaultConnectionConfig(coConfig);
        connectionManager.setDefaultSocketConfig(soConfig);


        httpClient = HttpClients.custom().setConnectionManager(connectionManager).setRetryStrategy(new DefaultHttpRequestRetryStrategy(config.getMaxRetry(), TimeValue.ZERO_MILLISECONDS))
                .setDefaultRequestConfig(reConfig)
                .build();
        return httpClient;
    }

    public static HttpClient defaultClient(){
        return createHttpClient(HttpClientConfig.defaultConfig());
    }

    /**
     *
     * @param url 不包含http前缀、host和端口
     * @param formDatas 其中的path应当是完整的逻辑路径，包含fileSystem，所以不用设置FileSystem了
     * @return
     */
    @SneakyThrows
    public static ClassicHttpResponse sendPostToMetaServer(String url, Map<String,Object> formDatas){
        String metaServerMasterAddr = ZkUtil.getMetaServerMasterAddr();
        String fullUrl = "http://" + metaServerMasterAddr + url;
        return sendPot(fullUrl,formDatas);
    }

    /**
     *
     * @param fullUrl 不包含http前缀、host和端口
     * @param formDatas 其中的path应当是完整的逻辑路径，包含fileSystem，所以不用设置FileSystem了
     * @return
     */
    @SneakyThrows
    public static ClassicHttpResponse sendPot(String fullUrl, Map<String,Object> formDatas){
        HttpClient httpClient = HttpClientUtil.defaultClient();
        HttpPost httpPost = new HttpPost(fullUrl);
        httpPost.setHeader("Content-Type", "multipart/form-data");
        // 构建 MultipartEntityBuilder，用于创建 formdata 格式的请求体
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
        Object path = formDatas.get("path");
        if(path!= null){
            entityBuilder.addTextBody("path", (String) path, ContentType.TEXT_PLAIN);
        }
        Object file = formDatas.get("file");
        if(file!= null){
            ByteArrayBody byteArrayBody = new ByteArrayBody((byte[]) file, ContentType.APPLICATION_OCTET_STREAM, "tempFileName");
            entityBuilder.addPart("file", byteArrayBody);
        }

        Object offset = formDatas.get("offset");
        if(offset!= null){
            entityBuilder.addTextBody("offset", offset+"", ContentType.TEXT_PLAIN);
        }
        Object length = formDatas.get("length");
        if(length!= null){
            entityBuilder.addTextBody("length", length+"", ContentType.TEXT_PLAIN);
        }

        entityBuilder.setBoundary(HttpClientConfig.HTTP_FORMDATA_BOUNDARY);
        // 构建请求实体
        HttpEntity httpEntity = entityBuilder.build();
        httpPost.setEntity(httpEntity);
        httpPost.setHeader("Content-Type", "multipart/form-data; boundary="+HttpClientConfig.HTTP_FORMDATA_BOUNDARY);
        return (ClassicHttpResponse) httpClient.execute(httpPost);
    }
}

```



## 各模块说明

bin：项目一键启动脚本，用于编译完成后，上传至服务器上，可以将minFS服务整体启动起来

dataServer:主要提供数据内容存储服务能力，单节点无状态设计，可以横向扩容

metaServer:主要提供文件系统全局元数据管理，管理dataserver的负载均衡，主备模式运行

easyClient:一个功能逻辑简单的SDK，用来与metaServer、dataServer通信，完成文件数据操作

