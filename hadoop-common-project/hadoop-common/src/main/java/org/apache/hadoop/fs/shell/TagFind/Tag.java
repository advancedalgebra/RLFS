/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell.TagFind;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.TagFind.*;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Deque;

/**
 * Implements the -tag expression for the
 * {@link TagFind} command.
 */
final class Tag extends BaseExpression {
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory)
      throws IOException {
    factory.addClass(Tag.class, "-tag");
    factory.addClass(Itag.class, "-itag");
  }

  private static final String[] USAGE = { "-tag pattern", "-itag pattern" };
  private static final String[] HELP = {
      "Evaluates as true if the tag of the file matches the",
      "pattern using standard file system globbing.",
      "If -itag is used then the match is case insensitive." };
  private GlobPattern globPattern;
  private boolean caseSensitive = true;

  /** Creates a case sensitive tag expression. */
  public Tag() {
    this(true);
  }

  /**
   * Construct a Tag {@link Expression} with a specified case sensitivity.
   *
   * @param caseSensitive if true the comparisons are case sensitive.
   */
  private Tag(boolean caseSensitive) {
    super();
    setUsage(USAGE);
    setHelp(HELP);
    setCaseSensitive(caseSensitive);
  }

  private void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }

  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  @Override
  public void prepare() throws IOException {
    String argPattern = getArgument(1);
    if (!caseSensitive) {
      argPattern = StringUtils.toLowerCase(argPattern);
    }
    globPattern = new GlobPattern(argPattern);
  }

  @Override
  public Result apply(PathData item, int depth) throws IOException {
    String tag = loadTag(item.path.toUri());
    if (!caseSensitive) {
      tag = StringUtils.toLowerCase(tag);
    }
    if (globPattern.matches(tag)) {
      return Result.PASS;
    } else {
      return Result.FAIL;
    }
  }

  private String loadTag(URI uri) {
    IOException e = null;
    String [] cmd = new String[] {"getfattr", "-n", "user.tag"};
    try {
      String output = FileUtil.execCommand(new File(uri), cmd);
      return output.split("\n")[1].split("\"")[1];
    } catch (Shell.ExitCodeException ioe) {
      if (ioe.getExitCode() != 1) {
        e = ioe;
      } else {
        return "default";
      }
    } catch (IOException ioe) {
      e = ioe;
    } finally {
      if (e != null) {
        throw new RuntimeException("Error while running command to get " +
                "file tag : " + StringUtils.stringifyException(e));
      }
    }
    return null;
  }

  /** Case insensitive version of the -tag expression. */
  static class Itag extends FilterExpression {
    public Itag() {
      super(new Tag(false));
    }
  }
}
