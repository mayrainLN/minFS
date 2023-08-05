package dto;

import lombok.Builder;
import lombok.Data;
import lombok.Setter;
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
    private String ip;
    private int port;
    // 单位：字节
    private long capacity;
    private String rack;
    private String zone;

    @Override
    public String toString() {
        return "DataServerInstance{" + '\n' +
                "   ip='" + ip +'\'' + '\n' +
                "   port=" + port + '\n' +
                "   capacity=" + capacity + '\n' +
                "   rack='" + rack +'\'' + '\n' +
                "   zone='" + zone +'\'' + '\n' +
                '}';
    }
}
