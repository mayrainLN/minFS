package dto;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/5 16:36
 * @description :
 */
@Data
@Accessors(chain = true)
@Builder
public class DataServerInstance {
    private String host;
    private int port;
    private int capacity; // 总容量，单位:Byte
    private int fileTotal; // 已存文件总数
    private int useCapacity; // 已用容量，单位:Byte
    private String rack;    // 机架
    private String zone;    // 机房


    @Override
    public String toString() {
        return "DataServerInstance{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", capacity=" + capacity +
                ", useCapacity=" + useCapacity +
                ", fileTotal=" + fileTotal +
                ", rack='" + rack + '\'' +
                ", zone='" + zone + '\'' +
                '}';
    }
}
