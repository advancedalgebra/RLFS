package org.apache.hadoop.fs.shell;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.local.LocalCache;
import org.apache.hadoop.fs.local.LocalCacheInfo;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import sun.util.locale.LocaleObjectCache;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

@InterfaceAudience.Private
@InterfaceStability.Unstable

public class GetTag extends FsCommand{
    public static void registerCommands(CommandFactory factory) {
        factory.addClass(GetTag.class, "-gettag");
    }

    public static final String NAME = "gettag";
    public static final String USAGE = "[-c] [<path> ...]";
    public static final String DESCRIPTION =
            "Get tag for files that match the specified file pattern. If " +
                    "path is not specified, the contents of /user/<currentUser> " +
                    "will be listed.\n" +
                    "-c:  Do not use cache!";

    protected boolean useCache = true;
    protected String tag = null;

    @Override
    protected void processOptions(LinkedList<String> args)
            throws IOException {
        CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "c");
        cf.parse(args);
        if (cf.getOpt("R")) {
            useCache = false;
        };
        if (args.isEmpty()) args.add(Path.CUR_DIR);
    }

    @Override
    protected void processPathArgument(PathData item) throws IOException {
        super.processPathArgument(item);
    }

    @Override
    protected void processPaths(PathData parent, PathData ... items)
            throws IOException {
        super.processPaths(parent, items);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
        LocalCache localCache = new LocalCache(item);
        Thread check = new Thread(localCache);
        check.setDaemon(true);
        check.start();
        int x = 0;
        while (x < 500000) {
            System.out.println(localCache.readCache(item.path));
            x++;
        }
//        x = 0;
//        while (x < 5) {
//            System.out.println(localCache.readCache(new Path("hdfs://localhost:9000/user/input")));
//            x++;
//        }
    }
}
