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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributeProvider.AccessControlEnforcer;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestINodeAttributeProvider {
  private MiniDFSCluster miniDFS;
  private static final Set<String> CALLED = new HashSet<String>();

  public static class MyAuthorizationProvider extends INodeAttributeProvider {

    public static class MyAccessControlEnforcer implements AccessControlEnforcer {

      @Override
      public void checkPermission(String fsOwner, String supergroup,
          UserGroupInformation ugi, INodeAttributes[] inodeAttrs,
          INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
          int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
          FsAction parentAccess, FsAction access, FsAction subAccess,
          boolean ignoreEmptyDir) throws AccessControlException {
        CALLED.add("checkPermission|" + ancestorAccess + "|" + parentAccess + "|" + access);
      }
    }

    @Override
    public void start() {
      CALLED.add("start");
    }

    @Override
    public void stop() {
      CALLED.add("stop");
    }

    @Override
    public INodeAttributes getAttributes(String[] pathElements,
        final INodeAttributes inode) {
      CALLED.add("getAttributes");
      final boolean useDefault = useDefault(pathElements);
      return new INodeAttributes() {
        @Override
        public boolean isDirectory() {
          return inode.isDirectory();
        }

        @Override
        public byte[] getLocalNameBytes() {
          return inode.getLocalNameBytes();
        }

        @Override
        public String getUserName() {
          return (useDefault) ? inode.getUserName() : "foo";
        }

        @Override
        public String getGroupName() {
          return (useDefault) ? inode.getGroupName() : "bar";
        }

        @Override
        public FsPermission getFsPermission() {
          return (useDefault) ? inode.getFsPermission()
                              : new FsPermission(getFsPermissionShort());
        }

        @Override
        public short getFsPermissionShort() {
          return (useDefault) ? inode.getFsPermissionShort()
                              : (short) getPermissionLong();
        }

        @Override
        public long getPermissionLong() {
          return (useDefault) ? inode.getPermissionLong() : 0770;
        }

        @Override
        public AclFeature getAclFeature() {
          AclFeature f;
          if (useDefault) {
            f = inode.getAclFeature();
          } else {
            AclEntry acl = new AclEntry.Builder().setType(AclEntryType.GROUP).
                setPermission(FsAction.ALL).setName("xxx").build();
            f = new AclFeature(AclEntryStatusFormat.toInt(
                Lists.newArrayList(acl)));
          }
          return f;
        }

        @Override
        public XAttrFeature getXAttrFeature() {
          return (useDefault) ? inode.getXAttrFeature() : null;
        }

        @Override
        public long getModificationTime() {
          return (useDefault) ? inode.getModificationTime() : 0;
        }

        @Override
        public String getTag() {
          return (useDefault) ? inode.getTag() : "default";
        }

        @Override
        public long getAccessTime() {
          return (useDefault) ? inode.getAccessTime() : 0;
        }
      };

    }

    @Override
    public AccessControlEnforcer getExternalAccessControlEnforcer(
        AccessControlEnforcer deafultEnforcer) {
      return new MyAccessControlEnforcer();
    }

    private boolean useDefault(String[] pathElements) {
      return (pathElements.length < 2) ||
          !(pathElements[0].equals("user") && pathElements[1].equals("authz"));
    }

  }

  @Before
  public void setUp() throws IOException {
    CALLED.clear();
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_INODE_ATTRIBUTES_PROVIDER_KEY,
        MyAuthorizationProvider.class.getName());
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    miniDFS = new MiniDFSCluster.Builder(conf).build();
  }

  @After
  public void cleanUp() throws IOException {
    CALLED.clear();
    if (miniDFS != null) {
      miniDFS.shutdown();
    }
    Assert.assertTrue(CALLED.contains("stop"));
  }

  @Test
  public void testDelegationToProvider() throws Exception {
    Assert.assertTrue(CALLED.contains("start"));
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    fs.mkdirs(new Path("/tmp"));
    fs.setPermission(new Path("/tmp"), new FsPermission((short) 0777));
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting("u1",
        new String[]{"g1"});
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
        CALLED.clear();
        fs.mkdirs(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(CALLED.contains("checkPermission|null|null|null"));
        Assert.assertTrue(CALLED.contains("checkPermission|WRITE|null|null"));
        CALLED.clear();
        fs.listStatus(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(
            CALLED.contains("checkPermission|null|null|READ_EXECUTE"));
        CALLED.clear();
        fs.getAclStatus(new Path("/tmp/foo"));
        Assert.assertTrue(CALLED.contains("getAttributes"));
        Assert.assertTrue(CALLED.contains("checkPermission|null|null|null"));
        return null;
      }
    });
  }

  @Test
  public void testCustomProvider() throws Exception {
    FileSystem fs = FileSystem.get(miniDFS.getConfiguration(0));
    fs.mkdirs(new Path("/user/xxx"));
    FileStatus status = fs.getFileStatus(new Path("/user/xxx"));
    Assert.assertEquals(System.getProperty("user.name"), status.getOwner());
    Assert.assertEquals("supergroup", status.getGroup());
    Assert.assertEquals(new FsPermission((short)0755), status.getPermission());
    fs.mkdirs(new Path("/user/authz"));
    status = fs.getFileStatus(new Path("/user/authz"));
    Assert.assertEquals("foo", status.getOwner());
    Assert.assertEquals("bar", status.getGroup());
    Assert.assertEquals(new FsPermission((short) 0770), status.getPermission());
  }

}
