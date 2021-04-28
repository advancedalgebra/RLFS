package org.apache.hadoop.fs.local;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.PathData;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class LocalCache implements Runnable{
    private ConcurrentHashMap<Path, LocalCacheInfo> conHashMap = new ConcurrentHashMap<Path, LocalCacheInfo>();
    private final PathData pathData;
    private LocalCacheInfo invalidInfo = new LocalCacheInfo();

    public LocalCache(PathData pathData) {
        this.pathData = pathData;
    }

    void clear() {
        conHashMap.clear();
    }

    public void invalidCache(Path path) {
        LocalCacheInfo newCacheInfo = new LocalCacheInfo();
        conHashMap.replace(path, newCacheInfo);
    }

    public String readCache(Path path) throws IOException {
        LocalCacheInfo info = conHashMap.get(path);
        if (info == null) {
            String tag = pathData.fs.getFileStatus(path).getTag();
            LocalCacheInfo newCacheInfo = new LocalCacheInfo(tag);
            conHashMap.putIfAbsent(path, newCacheInfo);
//            System.out.println("not shoot!");
            return tag;
        } else if (info.getExpiryTime() == -1L) {
            String tag = pathData.fs.getFileStatus(path).getTag();
            LocalCacheInfo newCacheInfo = new LocalCacheInfo(tag);
            conHashMap.replace(path, invalidInfo, newCacheInfo);
//            System.out.println("not shoot!");
            return tag;
        } else {
//            System.out.println("shoot!");
            return info.getTag();
        }
    }

    public void run() {
        Thread.currentThread().setName("CheckCacheValidOrNot");
        try {
            while (true) {
                Iterator<ConcurrentHashMap.Entry<Path, LocalCacheInfo>> entries = conHashMap.entrySet().iterator();
                while (entries.hasNext()) {
                    ConcurrentHashMap.Entry<Path, LocalCacheInfo> entry = entries.next();
                    long now = new Date().getTime();
                    LocalCacheInfo current = entry.getValue();
                    if (!current.getTag().equals("invalid") && (now - current.getExpiryTime() >= 1000)) {
                        LocalCacheInfo newCacheInfo = new LocalCacheInfo();
                        conHashMap.replace(entry.getKey(), newCacheInfo);
                    }
                }

            }
        } catch (Throwable t) {
            terminate(1, t);
        }
    }
}
