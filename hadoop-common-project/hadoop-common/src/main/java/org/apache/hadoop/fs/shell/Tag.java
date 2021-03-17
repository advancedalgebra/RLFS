package org.apache.hadoop.fs.shell;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;

public class Tag extends FsCommand{
    public static void registerCommands(CommandFactory factory) {
        factory.addClass(Tag.class, "-tag");
    }

    public static final String NAME = "tag";
    public static final String USAGE = "[-R] [<path> ...]";
    public static final String DESCRIPTION =
            "Set tag for files that match the specified file pattern. If " +
                    "path is not specified, the contents of /user/<currentUser> " +
                    "will be listed.\n" +
                    "-R:  Recursively set tag for files of the directories.";

    protected boolean dirRecurse = true;

    @Override
    protected void processOptions(LinkedList<String> args)
            throws IOException {
        CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "R");
        cf.parse(args);
        setRecursive(cf.getOpt("R") && dirRecurse);
        if (args.isEmpty()) args.add(Path.CUR_DIR);
    }

    @Override
    protected void processPathArgument(PathData item) throws IOException {
        // implicitly recurse once for cmdline directories
        if (dirRecurse && item.stat.isDirectory()) {
            recursePath(item);
        } else {
            super.processPathArgument(item);
        }
    }

    @Override
    protected void processPaths(PathData parent, PathData ... items)
            throws IOException {
        if (parent != null && !isRecursive() && items.length != 0) {
            out.println("Found " + items.length + " items");
        }
        super.processPaths(parent, items);
    }

    @Override
    protected void processPath(PathData item) throws IOException {
        FileStatus stat = item.stat;
    }
}
