package com.ksyun.campus.metaserver.db;

import com.ksyun.campus.metaserver.domain.FileType;
import dto.PrefixConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.HashMap;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/10 19:18
 * @description :
 */
@Slf4j
public class Database {
    public static FileTree fileTree;
    private static void init(){
        fileTree = FileTree.initDir(PrefixConstants.DB_BASE_PATH_META_DATA);
    }

    static {
        init();
    }

    // 创建文件，如果目录不存在，需要创建目录
    public static boolean create(String path){
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

    // 创建文件，如果目录不存在，需要创建目录
    public static boolean commit(String path){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        FileTree curr = null;
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            curr = parent.getChildren().get(currName);
            if(curr == null){
                log.error("文件不存在");
                return false;
            }
            parent = curr;
        }
        curr.setCommitted(true);
        curr.setMtime(System.currentTimeMillis());
        return true;
    }

    // 创建文件，如果目录不存在，需要创建目录
    public static boolean write(String path,int size){
        path = PrefixConstants.DB_BASE_PATH_META_DATA + path;
        String[] partPath = path.split("/"); // 第一个是/metaData
        FileTree parent = fileTree;
        FileTree curr = null;
        for (int i = 1 ; i < partPath.length; i++) {
            String currName = partPath[i];
            curr = parent.getChildren().get(currName);
            if(curr == null){
                log.error("文件不存在");
                return false;
            }
            parent = curr;
        }
        curr.setSize(curr.getSize() + size);
        curr.setMtime(System.currentTimeMillis());
        return true;
    }
}
