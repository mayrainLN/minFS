package com.ksyun.campus.dataserver.services;

import cn.hutool.json.JSONUtil;
import com.ksyun.campus.dataserver.util.DataServerInfoUtil;
import dto.DataServerInstance;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
@Slf4j
public class DataService {

    @Resource
    DataServerInfoUtil dataServerInfoUtil;

    @Resource
    CuratorFramework client;

    /**
     * 返回文件存储的真实路径
     * @param fileLogicPath
     * @return
     */
    public ResponseEntity createFile(String fileLogicPath) {
        Path filePath = Paths.get(dataServerInfoUtil.getRealBasePath()+fileLogicPath);  // 文件全路径
        if (filePath.toFile().exists()) {
            log.error("文件已存在");
            return ResponseEntity.badRequest().body("文件已存在");
        }
        try {
            if (!filePath.getParent().toFile().exists()){
                FileUtils.forceMkdir(filePath.getParent().toFile());
            }
            Files.createFile(filePath);
            return ResponseEntity.ok().body(filePath.toFile().getAbsolutePath().replace('\\', '/'));
        } catch (IOException e) {

            log.error("文件创建失败", e);
            return ResponseEntity.badRequest().body("文件创建失败");
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystem fileSystem，相当于namespace，来模拟多个volume，相当与C盘D盘
     * @param path
     * @param offset
     * @param length
     * @return
     */
    public ResponseEntity readFile(String fileSystem, String path, int offset, int length) {
        // base目录已经自带了'/'
        if(path.startsWith(File.separator)){
            path = path.substring(1);
        }
        if (fileSystem == null){
            fileSystem = "";
        }
        String fullRealFilePath = dataServerInfoUtil.getRealBasePath() + fileSystem + "/" + path;
        byte[] data = read(fullRealFilePath, offset, length);

        InputStream inputStream = new ByteArrayInputStream(data);
        InputStreamResource resource = new InputStreamResource(inputStream);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        return new ResponseEntity<>(resource, headers, HttpStatus.OK);
    }



    /**
     * 追加写入。
     * 由metaServer来控制 创建、覆盖写 的逻辑。
     * 请求能打到DataServer就默认文件已经创建，但还没有commit
     * // getRealBasePath：/data/
     * // fileSystem：C/
     * // fileLogicPath：txt/test.txt
     *
     * @param data
     * @param fileLogicPath 文件逻辑路径 如文件名为test.txt，文件逻辑路径为txt/test.txt
     *                      不能以/开头
     * @return
     */
    public ResponseEntity appendLocalFile(String fileSystem, String fileLogicPath, byte[] data) {
        if(fileSystem==null){
            fileSystem = "";
        }
        Path filePath;  // 文件全路径
        Path dirPath; // 所属目录路径
        if (fileSystem == null || fileSystem.isEmpty()) {
            // 没有指定目录，写在base目录下
            filePath = Paths.get(dataServerInfoUtil.getRealBasePath(), fileLogicPath);
//            dirPath = filePath.getParent();
        } else {
            // 指定了目录，写在指定目录下
            filePath = Paths.get(dataServerInfoUtil.getRealBasePath(), fileSystem, fileLogicPath);
//            dirPath = filePath.getParent();
        }
        String absolutePath = filePath.toFile().getAbsolutePath();
        try {
            if(!Files.exists(filePath)){
                log.error("{}文件未创建，无法写入", absolutePath);
                return ResponseEntity.badRequest().body("文件未创建，无法写入");
            }

            File file = new File(absolutePath);
            FileUtils.writeByteArrayToFile(file, data, true);
            return ResponseEntity.ok(absolutePath);
        } catch (IOException e) {
            log.error("本地追加写入失败", e);
            return ResponseEntity.badRequest().body("本地追加写入失败");
        }
    }

    public ResponseEntity mkdir(String dirPathString) {

        boolean res = mkLocalDir(dirPathString);
        if(!res){
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，已达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
        return new ResponseEntity(HttpStatus.OK);
    }

    /**
     * @param dirPathString 最好不要以'/'开头，如果以'/'开头，会自动去掉
     * @return
     */
    private boolean mkLocalDir(String dirPathString) {
        // base目录已经自带了'/'
        if(dirPathString.startsWith(File.separator)){
            dirPathString = dirPathString.substring(1);
        }
        File folder = new File(dataServerInfoUtil.getRealBasePath()+dirPathString);
        if (folder.mkdirs()) {
            log.info("文件夹：{}创建成功", dirPathString);
            return true;
        }
        System.out.println("文件夹：{}创建失败" + dirPathString);
        return false;
    }

    /**
     * @param path 真实路径
     * @param offset
     * @param length
     * @return
     */
    public byte[] read(String path, int offset, int length) {

        Path filePath = Paths.get(path);
        try {
            byte[] fileData = Files.readAllBytes(filePath);
            int readLength = Math.min(length, fileData.length - offset);
            if (readLength < 0) {
                readLength = 0;
            }
            byte[] resultData = new byte[readLength];

            System.arraycopy(fileData, offset, resultData, 0, readLength);

            return resultData;
        } catch (IOException e) {
            log.error("本地读取失败", e);
            e.printStackTrace();
            return null;
        }
    }


    @SneakyThrows
    public void updateMetaData(String ZKPath,DataServerInstance dataServerInstance) {
        client.setData().forPath(ZKPath, JSONUtil.parse(dataServerInstance).toString().getBytes(StandardCharsets.UTF_8));
    }

    public ResponseEntity deleteLocalFile(String fileSystem, String path) {
        if(fileSystem==null){
            fileSystem = "";
        }
        Path filePath;  // 文件全路径
        Path dirPath; // 所属目录路径
        if (fileSystem == null || fileSystem.isEmpty()) {
            // 没有指定目录，写在base目录下
            filePath = Paths.get(dataServerInfoUtil.getRealBasePath(), path);
            dirPath = filePath.getParent();
        } else {
            // 指定了目录，写在指定目录下
            filePath = Paths.get(dataServerInfoUtil.getRealBasePath(), fileSystem,path);
            dirPath = filePath.getParent();
        }
        File file = filePath.toFile();

        // 这里保持幂等
        if(file.exists()){
            file.delete();
        }
        return ResponseEntity.ok().body("删除成功");
    }
}
