package com.ksyun.campus.metaserver.db;

import com.ksyun.campus.metaserver.domain.FileType;
import com.ksyun.campus.metaserver.domain.StatInfo;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author :MayRain
 * @version :1.0
 * @date :2023/8/10 18:51
 * @description : 原本文件元信息都是存在ZK上。考虑到zk的不适合存储大量数据，迁移到metaServer中。补充树形结构信息
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
public class FileTree extends StatInfo {

    public static FileTree initDir(String name){
        FileTree dir = new FileTree();
        dir.setChildren(new ConcurrentHashMap<>());
        dir.setName(name);
        dir.setCommitted(true);
        dir.setMtime(System.currentTimeMillis());
        dir.setSize(0);
        dir.setType(FileType.Directory);
        return dir;
    }

    public static FileTree initFile(String name){
        FileTree file = new FileTree();
        file.setName(name);
        file.setCommitted(false);
        file.setSize(0);
        file.setType(FileType.File);
        return file;
    }

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
