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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.LoaderContext;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SaverContext;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FilesUnderConstructionSection.FileUnderConstructionEntry;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeDirectorySection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.AclFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrCompactProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.XAttrFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeEntryProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields.PermissionStatusFormat;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager.StringTable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.protobuf.ByteString;

@InterfaceAudience.Private
public final class FSImageFormatPBINode {
  public static final int ACL_ENTRY_NAME_MASK = (1 << 24) - 1;
  public static final int ACL_ENTRY_NAME_OFFSET = 6;
  public static final int ACL_ENTRY_TYPE_OFFSET = 3;
  public static final int ACL_ENTRY_SCOPE_OFFSET = 5;
  public static final int ACL_ENTRY_PERM_MASK = 7;
  
  public static final int XATTR_NAMESPACE_MASK = 3;
  public static final int XATTR_NAMESPACE_OFFSET = 30;
  public static final int XATTR_NAME_MASK = (1 << 24) - 1;
  public static final int XATTR_NAME_OFFSET = 6;

  /* See the comments in fsimage.proto for an explanation of the following. */
  public static final int XATTR_NAMESPACE_EXT_OFFSET = 5;
  public static final int XATTR_NAMESPACE_EXT_MASK = 1;

  private static final Logger LOG =
      LoggerFactory.getLogger(FSImageFormatPBINode.class);

  private static final int DIRECTORY_ENTRY_BATCH_SIZE = 1000;

  // the loader must decode all fields referencing serial number based fields
  // via to<Item> methods with the string table.
  public final static class Loader {
    public static PermissionStatus loadPermission(long id,
        final StringTable stringTable) {
      return PermissionStatusFormat.toPermissionStatus(id, stringTable);
    }

    public static ImmutableList<AclEntry> loadAclEntries(
        AclFeatureProto proto, final StringTable stringTable) {
      ImmutableList.Builder<AclEntry> b = ImmutableList.builder();
      for (int v : proto.getEntriesList()) {
        b.add(AclEntryStatusFormat.toAclEntry(v, stringTable));
      }
      return b.build();
    }
    
    public static List<XAttr> loadXAttrs(
        XAttrFeatureProto proto, final StringTable stringTable) {
      List<XAttr> b = new ArrayList<>();
      for (XAttrCompactProto xAttrCompactProto : proto.getXAttrsList()) {
        int v = xAttrCompactProto.getName();
        byte[] value = null;
        if (xAttrCompactProto.getValue() != null) {
          value = xAttrCompactProto.getValue().toByteArray();
        }
        b.add(XAttrFormat.toXAttr(v, value, stringTable));
      }
      
      return b;
    }

    public static ImmutableList<QuotaByStorageTypeEntry> loadQuotaByStorageTypeEntries(
      QuotaByStorageTypeFeatureProto proto) {
      ImmutableList.Builder<QuotaByStorageTypeEntry> b = ImmutableList.builder();
      for (QuotaByStorageTypeEntryProto quotaEntry : proto.getQuotasList()) {
        StorageType type = PBHelperClient.convertStorageType(quotaEntry.getStorageType());
        long quota = quotaEntry.getQuota();
        b.add(new QuotaByStorageTypeEntry.Builder().setStorageType(type)
            .setQuota(quota).build());
      }
      return b.build();
    }

    public static INodeDirectory loadINodeDirectory(INodeSection.INode n,
        LoaderContext state) {
      assert n.getType() == INodeSection.INode.Type.DIRECTORY;
      INodeSection.INodeDirectory d = n.getDirectory();

      final PermissionStatus permissions = loadPermission(d.getPermission(),
          state.getStringTable());
      final INodeDirectory dir = new INodeDirectory(n.getId(), n.getName()
          .toByteArray(), permissions, d.getModificationTime());
      final long nsQuota = d.getNsQuota(), dsQuota = d.getDsQuota();
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.Builder().
            nameSpaceQuota(nsQuota).storageSpaceQuota(dsQuota).build());
      }
      EnumCounters<StorageType> typeQuotas = null;
      if (d.hasTypeQuotas()) {
        ImmutableList<QuotaByStorageTypeEntry> qes =
            loadQuotaByStorageTypeEntries(d.getTypeQuotas());
        typeQuotas = new EnumCounters<StorageType>(StorageType.class,
            HdfsConstants.QUOTA_RESET);
        for (QuotaByStorageTypeEntry qe : qes) {
          if (qe.getQuota() >= 0 && qe.getStorageType() != null &&
              qe.getStorageType().supportTypeQuota()) {
            typeQuotas.set(qe.getStorageType(), qe.getQuota());
          }
        }

        if (typeQuotas.anyGreaterOrEqual(0)) {
          DirectoryWithQuotaFeature q = dir.getDirectoryWithQuotaFeature();
          if (q == null) {
            dir.addDirectoryWithQuotaFeature(new DirectoryWithQuotaFeature.
                Builder().typeQuotas(typeQuotas).build());
          } else {
            q.setQuota(typeQuotas);
          }
        }
      }

      if (d.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(
            d.getAcl(), state.getStringTable()));
        dir.addAclFeature(new AclFeature(entries));
      }
      if (d.hasXAttrs()) {
        dir.addXAttrFeature(new XAttrFeature(
            loadXAttrs(d.getXAttrs(), state.getStringTable())));
      }
      return dir;
    }

    public static void updateBlocksMap(INodeFile file, BlockManager bm) {
      // Add file->block mapping
      final BlockInfo[] blocks = file.getBlocks();
      if (blocks != null) {
        for (int i = 0; i < blocks.length; i++) {
          file.setBlock(i, bm.addBlockCollectionWithCheck(blocks[i], file));
        }
      }
    }

    private final FSDirectory dir;
    private final FSNamesystem fsn;
    private final FSImageFormatProtobuf.Loader parent;

    // Update blocks map by single thread asynchronously
    private ExecutorService blocksMapUpdateExecutor;
    // update name cache by single thread asynchronously.
    private ExecutorService nameCacheUpdateExecutor;

    Loader(FSNamesystem fsn, final FSImageFormatProtobuf.Loader parent) {
      this.fsn = fsn;
      this.dir = fsn.dir;
      this.parent = parent;
      // Note: these executors must be SingleThreadExecutor, as they
      // are used to modify structures which are not thread safe.
      blocksMapUpdateExecutor = Executors.newSingleThreadExecutor();
      nameCacheUpdateExecutor = Executors.newSingleThreadExecutor();
    }

    void loadINodeDirectorySectionInParallel(ExecutorService service,
        ArrayList<FileSummary.Section> sections, String compressionCodec)
        throws IOException {
      LOG.info("Loading the INodeDirectory section in parallel with {} sub-" +
              "sections", sections.size());
      CountDownLatch latch = new CountDownLatch(sections.size());
      final List<IOException> exceptions = Collections.synchronizedList(new ArrayList<>());
      for (FileSummary.Section s : sections) {
        service.submit(() -> {
          InputStream ins = null;
          try {
            ins = parent.getInputStreamForSection(s,
                compressionCodec);
            loadINodeDirectorySection(ins);
          } catch (Exception e) {
            LOG.error("An exception occurred loading INodeDirectories in parallel", e);
            exceptions.add(new IOException(e));
          } finally {
            latch.countDown();
            try {
              if (ins != null) {
                ins.close();
              }
            } catch (IOException ioe) {
              LOG.warn("Failed to close the input stream, ignoring", ioe);
            }
          }
        });
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        LOG.error("Interrupted waiting for countdown latch", e);
        throw new IOException(e);
      }
      if (exceptions.size() != 0) {
        LOG.error("{} exceptions occurred loading INodeDirectories",
            exceptions.size());
        throw exceptions.get(0);
      }
      LOG.info("Completed loading all INodeDirectory sub-sections");
    }

    void loadINodeDirectorySection(InputStream in) throws IOException {
      final List<INodeReference> refList = parent.getLoaderContext()
          .getRefList();
      while (true) {
        INodeDirectorySection.DirEntry e = INodeDirectorySection.DirEntry
            .parseDelimitedFrom(in);
        // note that in is a LimitedInputStream
        if (e == null) {
          break;
        }
        INodeDirectory p = dir.getInode(e.getParent()).asDirectory();
        for (long id : e.getChildrenList()) {
          INode child = dir.getInode(id);
          if (!addToParent(p, child)) {
            LOG.warn("Failed to add the inode {} to the directory {}",
                child.getId(), p.getId());
          }
        }

        for (int refId : e.getRefChildrenList()) {
          INodeReference ref = refList.get(refId);
          if (!addToParent(p, ref)) {
            LOG.warn("Failed to add the inode reference {} to the directory {}",
                ref.getId(), p.getId());
          }
        }
      }
    }

    private void fillUpInodeList(ArrayList<INode> inodeList, INode inode) {
      if (inode.isFile()) {
        inodeList.add(inode);
      }
      if (inodeList.size() >= DIRECTORY_ENTRY_BATCH_SIZE) {
        addToCacheAndBlockMap(inodeList);
        inodeList.clear();
      }
    }

    private void addToCacheAndBlockMap(final ArrayList<INode> inodeList) {
      final ArrayList<INode> inodes = new ArrayList<>(inodeList);
      nameCacheUpdateExecutor.submit(
          new Runnable() {
            @Override
            public void run() {
              addToCacheInternal(inodes);
            }
          });
      blocksMapUpdateExecutor.submit(
          new Runnable() {
            @Override
            public void run() {
              updateBlockMapInternal(inodes);
            }
          });
    }

    // update name cache with non-thread safe
    private void addToCacheInternal(ArrayList<INode> inodeList) {
      for (INode i : inodeList) {
        dir.cacheName(i);
      }
    }

     // update blocks map with non-thread safe
    private void updateBlockMapInternal(ArrayList<INode> inodeList) {
      for (INode i : inodeList) {
        updateBlocksMap(i.asFile(), fsn.getBlockManager());
      }
    }

    void waitBlocksMapAndNameCacheUpdateFinished() throws IOException {
      long start = System.currentTimeMillis();
      waitExecutorTerminated(blocksMapUpdateExecutor);
      waitExecutorTerminated(nameCacheUpdateExecutor);
      LOG.info("Completed update blocks map and name cache, total waiting "
          + "duration {}ms.", (System.currentTimeMillis() - start));
    }

    private void waitExecutorTerminated(ExecutorService executorService)
        throws IOException {
      executorService.shutdown();
      long start = System.currentTimeMillis();
      while (!executorService.isTerminated()) {
        try {
          executorService.awaitTermination(1, TimeUnit.SECONDS);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to executor service terminated duration {}ms.",
                (System.currentTimeMillis() - start));
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted waiting for executor terminated.", e);
          throw new IOException(e);
        }
      }
    }

    void loadINodeSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      loadINodeSectionHeader(in, prog, currentStep);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      int totalLoaded = loadINodesInSection(in, counter);
      LOG.info("Successfully loaded {} inodes", totalLoaded);
    }

    private int loadINodesInSection(InputStream in, Counter counter)
        throws IOException {
      // As the input stream is a LimitInputStream, the reading will stop when
      // EOF is encountered at the end of the stream.
      int cntr = 0;
      ArrayList<INode> inodeList = new ArrayList<>();
      while (true) {
        INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
        if (p == null) {
          break;
        }
        if (p.getId() == INodeId.ROOT_INODE_ID) {
          synchronized(this) {
            loadRootINode(p);
          }
        } else {
          INode n = loadINode(p);
          synchronized(this) {
            dir.addToInodeMap(n);
          }
          fillUpInodeList(inodeList, n);
        }
        cntr++;
        if (counter != null) {
          counter.increment();
        }
      }
      if (inodeList.size() > 0){
        addToCacheAndBlockMap(inodeList);
      }
      return cntr;
    }


    private long loadINodeSectionHeader(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      INodeSection s = INodeSection.parseDelimitedFrom(in);
      fsn.dir.resetLastInodeId(s.getLastInodeId());
      long numInodes = s.getNumInodes();
      LOG.info("Loading " + numInodes + " INodes.");
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numInodes);
      return numInodes;
    }

    void loadINodeSectionInParallel(ExecutorService service,
        ArrayList<FileSummary.Section> sections,
        String compressionCodec, StartupProgress prog,
        Step currentStep) throws IOException {
      LOG.info("Loading the INode section in parallel with {} sub-sections",
          sections.size());
      long expectedInodes = 0;
      CountDownLatch latch = new CountDownLatch(sections.size());
      AtomicInteger totalLoaded = new AtomicInteger(0);
      final List<IOException> exceptions = Collections.synchronizedList(new ArrayList<>());

      for (int i=0; i < sections.size(); i++) {
        FileSummary.Section s = sections.get(i);
        InputStream ins = parent.getInputStreamForSection(s, compressionCodec);
        if (i == 0) {
          // The first inode section has a header which must be processed first
          expectedInodes = loadINodeSectionHeader(ins, prog, currentStep);
        }
        service.submit(() -> {
          try {
            totalLoaded.addAndGet(loadINodesInSection(ins, null));
            prog.setCount(Phase.LOADING_FSIMAGE, currentStep,
                totalLoaded.get());
          } catch (Exception e) {
            LOG.error("An exception occurred loading INodes in parallel", e);
            exceptions.add(new IOException(e));
          } finally {
            latch.countDown();
            try {
              ins.close();
            } catch (IOException ioe) {
              LOG.warn("Failed to close the input stream, ignoring", ioe);
            }
          }
        });
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        LOG.info("Interrupted waiting for countdown latch");
      }
      if (exceptions.size() != 0) {
        LOG.error("{} exceptions occurred loading INodes", exceptions.size());
        throw exceptions.get(0);
      }
      if (totalLoaded.get() != expectedInodes) {
        throw new IOException("Expected to load "+expectedInodes+" in " +
            "parallel, but loaded "+totalLoaded.get()+". The image may " +
            "be corrupt.");
      }
      LOG.info("Completed loading all INode sections. Loaded {} inodes.",
          totalLoaded.get());
    }

    /**
     * Load the under-construction files section, and update the lease map
     */
    void loadFilesUnderConstructionSection(InputStream in) throws IOException {
      // Leases are added when the inode section is loaded. This section is
      // still read in for compatibility reasons.
      while (true) {
        FileUnderConstructionEntry entry = FileUnderConstructionEntry
            .parseDelimitedFrom(in);
        if (entry == null) {
          break;
        }
      }
    }

    private boolean addToParent(INodeDirectory parentDir, INode child) {
      if (parentDir == dir.rootDir && FSDirectory.isReservedName(child)) {
        throw new HadoopIllegalArgumentException("File name \""
            + child.getLocalName() + "\" is reserved. Please "
            + " change the name of the existing file or directory to another "
            + "name before upgrading to this release.");
      }
      // NOTE: This does not update space counts for parents
      if (!parentDir.addChildAtLoading(child)) {
        return false;
      }
      return true;
    }

    private INode loadINode(INodeSection.INode n) {
      switch (n.getType()) {
      case FILE:
        return loadINodeFile(n);
      case DIRECTORY:
        return loadINodeDirectory(n, parent.getLoaderContext());
      case SYMLINK:
        return loadINodeSymlink(n);
      default:
        break;
      }
      return null;
    }

    private INodeFile loadINodeFile(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.FILE;
      INodeSection.INodeFile f = n.getFile();
      List<BlockProto> bp = f.getBlocksList();
      BlockType blockType = PBHelperClient.convert(f.getBlockType());
      LoaderContext state = parent.getLoaderContext();
      boolean isStriped = f.hasErasureCodingPolicyID();
      assert ((!isStriped) || (isStriped && !f.hasReplication()));
      Short replication = (!isStriped ? (short) f.getReplication() : null);
      Byte ecPolicyID = (isStriped ?
          (byte) f.getErasureCodingPolicyID() : null);
      ErasureCodingPolicy ecPolicy = isStriped ?
          fsn.getErasureCodingPolicyManager().getByID(ecPolicyID) : null;

      BlockInfo[] blocks = new BlockInfo[bp.size()];
      for (int i = 0; i < bp.size(); ++i) {
        BlockProto b = bp.get(i);
        if (isStriped) {
          Preconditions.checkState(ecPolicy.getId() > 0,
              "File with ID " + n.getId() +
              " has an invalid erasure coding policy ID " + ecPolicy.getId());
          blocks[i] = new BlockInfoStriped(PBHelperClient.convert(b), ecPolicy);
        } else {
          blocks[i] = new BlockInfoContiguous(PBHelperClient.convert(b),
              replication);
        }
      }

      final PermissionStatus permissions = loadPermission(f.getPermission(),
          parent.getLoaderContext().getStringTable());

      final INodeFile file = new INodeFile(n.getId(),
          n.getName().toByteArray(), permissions, f.getModificationTime(),
          f.getAccessTime(), blocks, replication, ecPolicyID,
          f.getPreferredBlockSize(), (byte)f.getStoragePolicyID(), blockType);

      if (f.hasAcl()) {
        int[] entries = AclEntryStatusFormat.toInt(loadAclEntries(
            f.getAcl(), state.getStringTable()));
        file.addAclFeature(new AclFeature(entries));
      }

      if (f.hasXAttrs()) {
        file.addXAttrFeature(new XAttrFeature(
            loadXAttrs(f.getXAttrs(), state.getStringTable())));
      }

      // under-construction information
      if (f.hasFileUC()) {
        INodeSection.FileUnderConstructionFeature uc = f.getFileUC();
        file.toUnderConstruction(uc.getClientName(), uc.getClientMachine());
        // update the lease manager
        fsn.leaseManager.addLease(uc.getClientName(), file.getId());
        if (blocks.length > 0) {
          BlockInfo lastBlk = file.getLastBlock();
          // replace the last block of file
          final BlockInfo ucBlk;
          if (isStriped) {
            BlockInfoStriped striped = (BlockInfoStriped) lastBlk;
            ucBlk = new BlockInfoStriped(striped, ecPolicy);
          } else {
            ucBlk = new BlockInfoContiguous(lastBlk,
                replication);
          }
          ucBlk.convertToBlockUnderConstruction(
              HdfsServerConstants.BlockUCState.UNDER_CONSTRUCTION, null);
          file.setBlock(file.numBlocks() - 1, ucBlk);
        }
      }
      return file;
    }


    private INodeSymlink loadINodeSymlink(INodeSection.INode n) {
      assert n.getType() == INodeSection.INode.Type.SYMLINK;
      INodeSection.INodeSymlink s = n.getSymlink();
      final PermissionStatus permissions = loadPermission(s.getPermission(),
          parent.getLoaderContext().getStringTable());

      INodeSymlink sym = new INodeSymlink(n.getId(), n.getName().toByteArray(),
          permissions, s.getModificationTime(), s.getAccessTime(),
          s.getTarget().toStringUtf8());

      return sym;
    }

    private void loadRootINode(INodeSection.INode p) {
      INodeDirectory root = loadINodeDirectory(p, parent.getLoaderContext());
      final QuotaCounts q = root.getQuotaCounts();
      final long nsQuota = q.getNameSpace();
      final long dsQuota = q.getStorageSpace();
      if (nsQuota != -1 || dsQuota != -1) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(nsQuota, dsQuota);
      }
      final EnumCounters<StorageType> typeQuotas = q.getTypeSpaces();
      if (typeQuotas.anyGreaterOrEqual(0)) {
        dir.rootDir.getDirectoryWithQuotaFeature().setQuota(typeQuotas);
      }
      dir.rootDir.cloneModificationTime(root);
      dir.rootDir.clonePermissionStatus(root);
      final AclFeature af = root.getFeature(AclFeature.class);
      if (af != null) {
        dir.rootDir.addAclFeature(af);
      }
      // root dir supports having extended attributes according to POSIX
      final XAttrFeature f = root.getXAttrFeature();
      if (f != null) {
        dir.rootDir.addXAttrFeature(f);
      }
      dir.addRootDirToEncryptionZone(f);
    }
  }

  // the saver can directly write out fields referencing serial numbers.
  // the serial number maps will be compacted when loading.
  public final static class Saver {
    private long numImageErrors;//记录在序列化过程中检测到的非致命错误数量的属性
    //将 INodeAttributes 对象的权限信息转换为一个长整型值
    private static long buildPermissionStatus(INodeAttributes n) {
      return n.getPermissionLong();
    }
    //AclFeature f —— 存储 ACL（访问控制列表）信息的对象
    //将 AclFeature 对象中的所有 ACL 条目转换为 AclFeatureProto，即 Protobuf 格式的 ACL 条目集合。它通过遍历 f 的所有条目并将其添加到构建器
    private static AclFeatureProto.Builder buildAclEntries(AclFeature f) {
      AclFeatureProto.Builder b = AclFeatureProto.newBuilder();
      for (int pos = 0, e; pos < f.getEntriesSize(); pos++) {
        e = f.getEntryAt(pos);
        b.addEntries(e);
      }
      return b;
    }
    //XAttrFeature f —— 存储扩展属性（XAttrs）的对象
    //将 XAttrFeature 对象中的每个扩展属性转换为 XAttrCompactProto，并将其构建为 XAttrFeatureProto。扩展属性是文件的附加元数据，如用户自定义的属性
    private static XAttrFeatureProto.Builder buildXAttrs(XAttrFeature f) {
      XAttrFeatureProto.Builder b = XAttrFeatureProto.newBuilder();
      for (XAttr a : f.getXAttrs()) {
        XAttrCompactProto.Builder xAttrCompactBuilder = XAttrCompactProto.
            newBuilder();
        int v = XAttrFormat.toInt(a);
        xAttrCompactBuilder.setName(v);
        if (a.getValue() != null) {
          xAttrCompactBuilder.setValue(PBHelperClient.getByteString(a.getValue()));
        }
        b.addXAttrs(xAttrCompactBuilder.build());
      }
      
      return b;
    }
    //QuotaCounts q —— 存储配额信息的对象，包含不同存储类型的配额数据
    //将 QuotaCounts 对象中的每种存储类型的配额数据转换为 QuotaByStorageTypeFeatureProto。它会遍历所有支持配额的存储类型，并将每种类型的配额数据添加到构建器中
    private static QuotaByStorageTypeFeatureProto.Builder
        buildQuotaByStorageTypeEntries(QuotaCounts q) {
      QuotaByStorageTypeFeatureProto.Builder b =
          QuotaByStorageTypeFeatureProto.newBuilder();
      for (StorageType t: StorageType.getTypesSupportingQuota()) {
        if (q.getTypeSpace(t) >= 0) {
          QuotaByStorageTypeEntryProto.Builder eb =
              QuotaByStorageTypeEntryProto.newBuilder().
              setStorageType(PBHelperClient.convertStorageType(t)).
              setQuota(q.getTypeSpace(t));
          b.addQuotas(eb);
        }
      }
      return b;
    }
   //INodeFileAttributes file —— 文件的 inode 属性对象，包含文件的各种属性信息
    //将文件的属性转换为 INodeSection.INodeFile.Builder 对象，包括文件的访问时间、修改时间、权限、块大小等属性。如果文件是条带化的（使用了 Erasure Coding），还会保存相应的编码策略信息
    public static INodeSection.INodeFile.Builder buildINodeFile(
        INodeFileAttributes file, final SaverContext state) {
      INodeSection.INodeFile.Builder b = INodeSection.INodeFile.newBuilder()
          .setAccessTime(file.getAccessTime())
          .setModificationTime(file.getModificationTime())
          .setPermission(buildPermissionStatus(file))
          .setPreferredBlockSize(file.getPreferredBlockSize())
          .setStoragePolicyID(file.getLocalStoragePolicyID())
          .setBlockType(PBHelperClient.convert(file.getBlockType()));

      if (file.isStriped()) {
        b.setErasureCodingPolicyID(file.getErasureCodingPolicyID());
      } else {
        b.setReplication(file.getFileReplication());
      }

      AclFeature f = file.getAclFeature();
      if (f != null) {
        b.setAcl(buildAclEntries(f));
      }
      XAttrFeature xAttrFeature = file.getXAttrFeature();
      if (xAttrFeature != null) {
        b.setXAttrs(buildXAttrs(xAttrFeature));
      }
      return b;
    }
    //INodeDirectoryAttributes dir —— 目录的 inode 属性对象，包含目录的各种属性信息
    //将目录的属性转换为 INodeSection.INodeDirectory.Builder 对象，包括目录的修改时间、配额信息、权限等。如果目录有子目录或文件，方法还会将配额信息和 ACL 数据保存
    public static INodeSection.INodeDirectory.Builder buildINodeDirectory(
        INodeDirectoryAttributes dir, final SaverContext state) {
      QuotaCounts quota = dir.getQuotaCounts();
      INodeSection.INodeDirectory.Builder b = INodeSection.INodeDirectory
          .newBuilder().setModificationTime(dir.getModificationTime())
          .setNsQuota(quota.getNameSpace())
          .setDsQuota(quota.getStorageSpace())
          .setPermission(buildPermissionStatus(dir));

      if (quota.getTypeSpaces().anyGreaterOrEqual(0)) {
        b.setTypeQuotas(buildQuotaByStorageTypeEntries(quota));
      }

      AclFeature f = dir.getAclFeature();
      if (f != null) {
        b.setAcl(buildAclEntries(f));
      }
      XAttrFeature xAttrFeature = dir.getXAttrFeature();
      if (xAttrFeature != null) {
        b.setXAttrs(buildXAttrs(xAttrFeature));
      }
      return b;
    }
    //表示 HDFS 的文件系统名称系统,负责管理 HDFS 上的文件和目录结构，支持创建、删除、重命名文件/目录等操作
    private final FSNamesystem fsn;
    //用于构建并存储文件摘要信息
    private final FileSummary.Builder summary;
    //提供与当前保存文件系统镜像相关的上下文信息
    private final SaveNamespaceContext context;
    //表示父类保存器，负责处理文件系统镜像的更高层次的保存逻辑
    private final FSImageFormatProtobuf.Saver parent;

    Saver(FSImageFormatProtobuf.Saver parent, FileSummary.Builder summary) {
      this.parent = parent;
      this.summary = summary;
      this.context = parent.getContext();
      this.fsn = context.getSourceNamesystem();
      this.numImageErrors = 0;
    }
    //用于将 INode 目录部分的数据序列化到输出流中。它的目标是遍历并处理所有的目录节点，并将每个目录的子节点（即目录的子文件或子目录）信息保存到指定的输出流中
    void serializeINodeDirectorySection(OutputStream out) throws IOException {
      //获取 FSDirectory，它是 HDFS 文件系统目录的核心
      FSDirectory dir = fsn.getFSDirectory();
      //获取 INodeMap 的迭代器
      Iterator<INodeWithAdditionalFields> iter = dir.getINodeMap()
          .getMapIterator();
      //初始化参考列表
      final ArrayList<INodeReference> refList = parent.getSaverContext()
          .getRefList();
      int i = 0;
      int outputInodes = 0;
      //遍历 INodeMap 中的每个节点，如果当前节点是目录类型，则处理它
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        if (!n.isDirectory()) {
          continue;
        }
        //获取目录子节点
        ReadOnlyList<INode> children = n.asDirectory().getChildrenList(
            Snapshot.CURRENT_STATE_ID);
        if (children.size() > 0) {
          INodeDirectorySection.DirEntry.Builder b = INodeDirectorySection.
              DirEntry.newBuilder().setParent(n.getId());
          for (INode inode : children) {
            // Error if the child inode doesn't exist in inodeMap
            //如果子节点的 ID 在 INodeMap 中找不到对应的 INode，则记录错误，说明该子节点丢失
            if (dir.getInode(inode.getId()) == null) {
              FSImage.LOG.error(
                  "FSImageFormatPBINode#serializeINodeDirectorySection: " +
                      "Dangling child pointer found. Missing INode in " +
                      "inodeMap: id=" + inode.getId() +
                      "; path=" + inode.getFullPathName() +
                      "; parent=" + (inode.getParent() == null ? "null" :
                      inode.getParent().getFullPathName()));
              ++numImageErrors;
            }
            if (!inode.isReference()) {
              // Serialization must ensure that children are in order, related
              // to HDFS-13693
              b.addChildren(inode.getId());
            } else {
              refList.add(inode.asReference());
              b.addRefChildren(refList.size() - 1);
            }
            outputInodes++;
          }
          INodeDirectorySection.DirEntry e = b.build();
          e.writeDelimitedTo(out);
        }
        //每隔一定数量的节点检查取消状态
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
        //每处理一定数量的节点提交子部分
        if (outputInodes >= parent.getInodesPerSubSection()) {
          outputInodes = 0;
          parent.commitSubSection(summary,
              FSImageFormatProtobuf.SectionName.INODE_DIR_SUB);
        }
      }
      //序列化完成后提交整个部分
      parent.commitSectionAndSubSection(summary,
          FSImageFormatProtobuf.SectionName.INODE_DIR,
          FSImageFormatProtobuf.SectionName.INODE_DIR_SUB);
    }

    void serializeINodeSection(OutputStream out) throws IOException {
      INodeMap inodesMap = fsn.dir.getINodeMap();

      INodeSection.Builder b = INodeSection.newBuilder()
          .setLastInodeId(fsn.dir.getLastInodeId()).setNumInodes(inodesMap.size());
      INodeSection s = b.build();
      s.writeDelimitedTo(out);

      int i = 0;
      Iterator<INodeWithAdditionalFields> iter = inodesMap.getMapIterator();
      while (iter.hasNext()) {
        INodeWithAdditionalFields n = iter.next();
        save(out, n);
        ++i;
        if (i % FSImageFormatProtobuf.Saver.CHECK_CANCEL_INTERVAL == 0) {
          context.checkCancelled();
        }
        if (i % parent.getInodesPerSubSection() == 0) {
          parent.commitSubSection(summary,
              FSImageFormatProtobuf.SectionName.INODE_SUB);
        }
      }
      parent.commitSectionAndSubSection(summary,
          FSImageFormatProtobuf.SectionName.INODE,
          FSImageFormatProtobuf.SectionName.INODE_SUB);
    }

    void serializeFilesUCSection(OutputStream out) throws IOException {
      Collection<Long> filesWithUC = fsn.getLeaseManager()
              .getINodeIdWithLeases();
      for (Long id : filesWithUC) {
        INode inode = fsn.getFSDirectory().getInode(id);
        if (inode == null) {
          LOG.warn("Fail to find inode " + id + " when saving the leases.");
          continue;
        }
        INodeFile file = inode.asFile();
        if (!file.isUnderConstruction()) {
          LOG.warn("Fail to save the lease for inode id " + id
                       + " as the file is not under construction");
          continue;
        }
        String path = file.getFullPathName();
        FileUnderConstructionEntry.Builder b = FileUnderConstructionEntry
            .newBuilder().setInodeId(file.getId()).setFullPath(path);
        FileUnderConstructionEntry e = b.build();
        e.writeDelimitedTo(out);
      }
      parent.commitSection(summary,
          FSImageFormatProtobuf.SectionName.FILES_UNDERCONSTRUCTION);
    }

    private void save(OutputStream out, INode n) throws IOException {
      if (n.isDirectory()) {
        save(out, n.asDirectory());
      } else if (n.isFile()) {
        save(out, n.asFile());
      } else if (n.isSymlink()) {
        save(out, n.asSymlink());
      }
    }

    private void save(OutputStream out, INodeDirectory n) throws IOException {
      INodeSection.INodeDirectory.Builder b = buildINodeDirectory(n,
          parent.getSaverContext());
      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.DIRECTORY).setDirectory(b).build();
      r.writeDelimitedTo(out);
    }

    private void save(OutputStream out, INodeFile n) throws IOException {
      INodeSection.INodeFile.Builder b = buildINodeFile(n,
          parent.getSaverContext());
      BlockInfo[] blocks = n.getBlocks();

      if (blocks != null) {
        for (Block block : n.getBlocks()) {
          b.addBlocks(PBHelperClient.convert(block));
        }
      }

      FileUnderConstructionFeature uc = n.getFileUnderConstructionFeature();
      if (uc != null) {
        INodeSection.FileUnderConstructionFeature f =
            INodeSection.FileUnderConstructionFeature
            .newBuilder().setClientName(uc.getClientName())
            .setClientMachine(uc.getClientMachine()).build();
        b.setFileUC(f);
      }

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.FILE).setFile(b).build();
      r.writeDelimitedTo(out);
    }

    private void save(OutputStream out, INodeSymlink n) throws IOException {
      INodeSection.INodeSymlink.Builder b = INodeSection.INodeSymlink
          .newBuilder()
          .setPermission(buildPermissionStatus(n))
          .setTarget(ByteString.copyFrom(n.getSymlink()))
          .setModificationTime(n.getModificationTime())
          .setAccessTime(n.getAccessTime());

      INodeSection.INode r = buildINodeCommon(n)
          .setType(INodeSection.INode.Type.SYMLINK).setSymlink(b).build();
      r.writeDelimitedTo(out);
    }

    private INodeSection.INode.Builder buildINodeCommon(INode n) {
      return INodeSection.INode.newBuilder()
          .setId(n.getId())
          .setName(ByteString.copyFrom(n.getLocalNameBytes()));
    }

    /**
     * Number of non-fatal errors detected while writing the
     * INodeSection and INodeDirectorySection sections.
     * @return the number of non-fatal errors detected.
     */
    public long getNumImageErrors() {
      return numImageErrors;
    }
  }

  private FSImageFormatPBINode() {
  }
}
