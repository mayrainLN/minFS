package dto;

import lombok.Data;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/6 21:15
 * @description :
 */
@Data
public class WriteFileRequest {
    private String path;
    private String data;
}
