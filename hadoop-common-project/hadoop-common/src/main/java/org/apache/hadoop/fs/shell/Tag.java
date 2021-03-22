package org.apache.hadoop.fs.shell;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.LinkedList;

public class Tag extends FsCommand{
    public static void registerCommands(CommandFactory factory) {
        factory.addClass(Tag.class, "-tag");
    }

    public static final String NAME = "tag";
    public static final String USAGE = "[-R] [<path> ...] [tag]";
    public static final String DESCRIPTION =
            "Set tag for files that match the specified file pattern. If " +
                    "path is not specified, the contents of /user/<currentUser> " +
                    "will be listed.\n" +
                    "-R:  Recursively set tag for files of the directories.";

    protected boolean dirRecurse = true;
    protected String tag = null;

    @Override
    protected void processOptions(LinkedList<String> args)
            throws IOException {
        CommandFormat cf = new CommandFormat(0, Integer.MAX_VALUE, "R");
        cf.parse(args);
        setRecursive(cf.getOpt("R") && dirRecurse);
        if (args.size() == 0) {
            tag = "default";
        } else {
            tag = args.getLast();
            args.removeLast();
        }
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
        item.fs.setTag(item.path, this.tag);
    }
}
