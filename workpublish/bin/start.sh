SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 相对于当前脚本目录的项目目录
JAR_DIRS=("../metaServer" "../dataServer")
JAR_NAMES=("metaServer-1.0.jar" "dataServer-1.0.jar")

DATA_DIR="${SCRIPT_DIR}/../dataServer"
META_DIR="${SCRIPT_DIR}/../metaServer"

echo $DATA_DIR
echo $META_DIR

# 对应的端口和ZooKeeper地址
DATA_SERVER_PORTS=("9000" "9001" "9002" "9003")
META_SERVER_PORTS=("8000" "8001")
ZK_ADDR="localhost:2181"

# 循环启动项目
for i in "${!META_SERVER_PORTS[@]}"; do
    PORT=${META_SERVER_PORTS[$i]}

    # 切换到项目目录
    cd "$META_DIR"

    # 设置启动参数并启动项目
    java -jar -Dserver.port="$PORT" -Dzookeeper.addr="$ZK_ADDR" metaServer-1.0.jar &
done

for i in "${!DATA_SERVER_PORTS[@]}"; do
    PORT=${DATA_SERVER_PORTS[$i]}

    # 切换到项目目录
    cd "$DATA_DIR"

    # 设置启动参数并启动项目
    java -jar -Dserver.port="$PORT" -Dzookeeper.addr="$ZK_ADDR" dataServer-1.0.jar &
done


echo "All projects started."
