package dto;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/3 18:19
 * @description :
 */
@Data
@Accessors(chain = true)
public class RestResult{
    private String msg;
    private int code;
    private Object data;
    public static RestResult success(){
        return new RestResult().setCode(200).setMsg("success");
    }
    public static RestResult fail(){
        return new RestResult().setCode(500).setMsg("fail");
    }
    public RestResult data(Object data){
        this.data = data;
        return this;
    }
    public RestResult msg(String msg){
        this.msg = msg;
        return this;
    }
}

