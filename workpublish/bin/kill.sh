KEEP_PIDS="23453 23432"

# 获取所有与 Java 相关的进程号
JAVA_PIDS=$(pgrep -fl java | awk '{print $1}')

# 循环遍历所有 Java 进程号
for pid in $JAVA_PIDS; do
    # 检查是否在保留列表中
    if [[ " $KEEP_PIDS " =~ " $pid " ]]; then
        echo "Keeping Java process: $pid"
    else
        # 不在保留列表中的 Java 进程，执行杀死命令
        echo "Killing Java process: $pid"
        kill $pid
    fi
done

echo "Done."

