package org.apache.rocketmq.example.quickstart;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;

/**
 * @program: rocketmq-delaymsg
 * @description:
 * @author: huqi
 * @create: 2019-05-16 11:35
 */
public class WatchFile {

    public static void watchFile() {

        final Path path = Paths.get("D:\\Users\\qihu\\store");

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            //给path路径加上文件观察服务
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
            // start an infinite loop
            while (true) {
                final WatchKey key = watchService.take();

                for (WatchEvent<?> watchEvent : key.pollEvents()) {

                    final WatchEvent.Kind<?> kind = watchEvent.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }
                    //创建事件
                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    }
                    //修改事件
                    if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {

                    }
                    //删除事件
                    if (kind == StandardWatchEventKinds.ENTRY_DELETE) {

                    }
                    // get the filename for the event
                    final WatchEvent<Path> watchEventPath = (WatchEvent<Path>) watchEvent;
                    final Path filename = watchEventPath.context();
                    // print it out
                    System.out.println(kind + " -> " + filename);

                }
                // reset the keyf
                boolean valid = key.reset();
                // exit loop if the key is not valid (if the directory was
                // deleted,for
                if (!valid) {
                    break;
                }
            }

        } catch (IOException | InterruptedException ex) {
            System.err.println(ex);
        }

    }

    public static void main(String[] args) {
        watchFile();
    }


}

