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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
  private static final Logger LOG = LoggerFactory
      .getLogger(FSImageFormatProtobuf.class);

  private static volatile boolean enableParallelLoad = false;

  public static final class LoaderContext {
    private SerialNumberManager.StringTable stringTable;
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    public SerialNumberManager.StringTable getStringTable() {
      return stringTable;
    }

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class SaverContext {
    //内部静态类，用于实现去重功能，它通过将元素映射到唯一的 ID 来实现
    public static class DeduplicationMap<E> {
      //map 用于存储元素与其对应的 ID 映射关系。元素 E 作为键，ID 作为值，ID 是唯一的，用于去重
      private final Map<E, Integer> map = Maps.newHashMap();
      private DeduplicationMap() {}
      //返回一个新的 DeduplicationMap 实例
      static <T> DeduplicationMap<T> newMap() {
        return new DeduplicationMap<T>();
      }
      //value - 要去重的值
      int getId(E value) {
        if (value == null) {
          return 0;
        }
        Integer v = map.get(value);
        if (v == null) {//如果 value 不在 map 中，则为该值分配一个新的 ID（通过 map.size() + 1 来生成），并将该映射关系存入 map
          int nv = map.size() + 1;
          map.put(value, nv);
          return nv;
        }
        return v;//如果 value 已经存在于 map 中，返回其对应的 ID
      }

      int size() {
        return map.size();
      }

      Set<Entry<E, Integer>> entrySet() {
        return map.entrySet();
      }
    }
    //用于存储一些引用（INodeReference）
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class Loader implements FSImageFormat.AbstractLoader {
    static final int MINIMUM_FILE_LENGTH = 8;
    private final Configuration conf;
    private final FSNamesystem fsn;
    private final LoaderContext ctx;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /**
     * Whether the image's layout version must be the same with
     * {@link HdfsServerConstants#NAMENODE_LAYOUT_VERSION}. This is only set to true
     * when we're doing (rollingUpgrade rollback).
     */
    private final boolean requireSameLayoutVersion;

    private File filename;

    Loader(Configuration conf, FSNamesystem fsn,
        boolean requireSameLayoutVersion) {
      this.conf = conf;
      this.fsn = fsn;
      this.ctx = new LoaderContext();
      this.requireSameLayoutVersion = requireSameLayoutVersion;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      return imgTxId;
    }

    public LoaderContext getLoaderContext() {
      return ctx;
    }

    /**
     * Thread to compute the MD5 of a file as this can be in parallel while
     * loading the image without interfering much.
     */
    private static class DigestThread extends Thread {

      /**
       * Exception thrown when computing the digest if it cannot be calculated.
       */
      private volatile IOException ioe = null;

      /**
       * Calculated digest if there are no error.
       */
      private volatile MD5Hash digest = null;

      /**
       * FsImage file computed MD5.
       */
      private final File file;

      DigestThread(File inFile) {
        file = inFile;
        setName(inFile.getName() + " MD5 compute");
        setDaemon(true);
      }

      public MD5Hash getDigest() throws IOException {
        if (ioe != null) {
          throw ioe;
        }
        return digest;
      }

      public IOException getException() {
        return ioe;
      }

      @Override
      public void run() {
        try {
          digest = MD5FileUtils.computeMd5ForFile(file);
        } catch (IOException e) {
          ioe = e;
        } catch (Throwable t) {
          ioe = new IOException(t);
        }
      }

      @Override
      public String toString() {
        return "DigestThread{ ThreadName=" + getName() + ", digest=" + digest
            + ", file=" + file + '}';
      }
    }

    void load(File file) throws IOException {
      filename = file;
      long start = Time.monotonicNow();
      DigestThread dt = new DigestThread(file);
      dt.start();
      RandomAccessFile raFile = new RandomAccessFile(file, "r");
      FileInputStream fin = new FileInputStream(file);
      try {
        loadInternal(raFile, fin);
        try {
          dt.join();
          imgDigest = dt.getDigest();
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
        long end = Time.monotonicNow();
        LOG.info("Loaded FSImage in {} seconds.", (end - start) / 1000);
      } finally {
        fin.close();
        raFile.close();
      }
    }

    /**
     * Given a FSImage FileSummary.section, return a LimitInput stream set to
     * the starting position of the section and limited to the section length.
     * @param section The FileSummary.Section containing the offset and length
     * @param compressionCodec The compression codec in use, if any
     * @return An InputStream for the given section
     * @throws IOException
     */
    public InputStream getInputStreamForSection(FileSummary.Section section,
                                                String compressionCodec)
        throws IOException {
      FileInputStream fin = new FileInputStream(filename);
      try {

          FileChannel channel = fin.getChannel();
          channel.position(section.getOffset());
          InputStream in = new BufferedInputStream(new LimitInputStream(fin,
                  section.getLength()));

          in = FSImageUtil.wrapInputStreamForCompression(conf,
                  compressionCodec, in);
          return in;
      } catch (IOException e) {
          fin.close();
          throw e;
      }
    }

    /**
     * Takes an ArrayList of Section's and removes all Section's whose
     * name ends in _SUB, indicating they are sub-sections. The original
     * array list is modified and a new list of the removed Section's is
     * returned.
     * @param sections Array List containing all Sections and Sub Sections
     *                 in the image.
     * @return ArrayList of the sections removed, or an empty list if none are
     *         removed.
     */
    private ArrayList<FileSummary.Section> getAndRemoveSubSections(
        ArrayList<FileSummary.Section> sections) {
      ArrayList<FileSummary.Section> subSections = new ArrayList<>();
      Iterator<FileSummary.Section> iter = sections.iterator();
      while (iter.hasNext()) {
        FileSummary.Section s = iter.next();
        String name = s.getName();
        if (name.matches(".*_SUB$")) {
          subSections.add(s);
          iter.remove();
        }
      }
      return subSections;
    }

    /**
     * Given an ArrayList of Section's, return all Section's with the given
     * name, or an empty list if none are found.
     * @param sections ArrayList of the Section's to search though
     * @param name The name of the Sections to search for
     * @return ArrayList of the sections matching the given name
     */
    private ArrayList<FileSummary.Section> getSubSectionsOfName(
        ArrayList<FileSummary.Section> sections, SectionName name) {
      ArrayList<FileSummary.Section> subSec = new ArrayList<>();
      for (FileSummary.Section s : sections) {
        String n = s.getName();
        SectionName sectionName = SectionName.fromString(n);
        if (sectionName == name) {
          subSec.add(s);
        }
      }
      return subSec;
    }

    /**
     * Checks the number of threads configured for parallel loading and
     * return an ExecutorService with configured number of threads. If the
     * thread count is set to less than 1, it will be reset to the default
     * value
     * @return ExecutorServie with the correct number of threads
     */
    private ExecutorService getParallelExecutorService() {
      int threads = conf.getInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY,
          DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
      if (threads < 1) {
        LOG.warn("Parallel is enabled and {} is set to {}. Setting to the " +
            "default value {}", DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY,
            threads, DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT);
        threads = DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_DEFAULT;
      }
      ExecutorService executorService = Executors.newFixedThreadPool(
          threads);
      LOG.info("The fsimage will be loaded in parallel using {} threads",
          threads);
      return executorService;
    }

    private void loadInternal(RandomAccessFile raFile, FileInputStream fin)
        throws IOException {
      if (!FSImageUtil.checkFileFormat(raFile)) {
        throw new IOException("Unrecognized file format");
      }
      FileSummary summary = FSImageUtil.loadSummary(raFile);
      if (requireSameLayoutVersion && summary.getLayoutVersion() !=
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.getLayoutVersion() +
            " is not equal to the software version " +
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      }

      FileChannel channel = fin.getChannel();

      FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(
          fsn, this);
      FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(
          fsn, this);

      ArrayList<FileSummary.Section> sections = Lists.newArrayList(summary
          .getSectionsList());
      Collections.sort(sections, new Comparator<FileSummary.Section>() {
        @Override
        public int compare(FileSummary.Section s1, FileSummary.Section s2) {
          SectionName n1 = SectionName.fromString(s1.getName());
          SectionName n2 = SectionName.fromString(s2.getName());
          if (n1 == null) {
            return n2 == null ? 0 : -1;
          } else if (n2 == null) {
            return -1;
          } else {
            return n1.ordinal() - n2.ordinal();
          }
        }
      });

      StartupProgress prog = NameNode.getStartupProgress();
      /**
       * beginStep() and the endStep() calls do not match the boundary of the
       * sections. This is because that the current implementation only allows
       * a particular step to be started for once.
       */
      Step currentStep = null;
      boolean loadInParallel = enableParallelSaveAndLoad(conf);

      ExecutorService executorService = null;
      ArrayList<FileSummary.Section> subSections =
          getAndRemoveSubSections(sections);
      if (loadInParallel) {
        executorService = getParallelExecutorService();
      }

      for (FileSummary.Section s : sections) {
        channel.position(s.getOffset());
        InputStream in = new BufferedInputStream(new LimitInputStream(fin,
            s.getLength()));

        in = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), in);

        String n = s.getName();
        SectionName sectionName = SectionName.fromString(n);
        if (sectionName == null) {
          throw new IOException("Unrecognized section " + n);
        }

        ArrayList<FileSummary.Section> stageSubSections;
        switch (sectionName) {
        case NS_INFO:
          loadNameSystemSection(in);
          break;
        case STRING_TABLE:
          loadStringTableSection(in);
          break;
        case INODE: {
          currentStep = new Step(StepType.INODES);
          prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
          stageSubSections = getSubSectionsOfName(
              subSections, SectionName.INODE_SUB);
          if (loadInParallel && (stageSubSections.size() > 0)) {
            inodeLoader.loadINodeSectionInParallel(executorService,
                stageSubSections, summary.getCodec(), prog, currentStep);
          } else {
            inodeLoader.loadINodeSection(in, prog, currentStep);
          }
        }
          break;
        case INODE_REFERENCE:
          snapshotLoader.loadINodeReferenceSection(in);
          break;
        case INODE_DIR:
          stageSubSections = getSubSectionsOfName(
              subSections, SectionName.INODE_DIR_SUB);
          if (loadInParallel && stageSubSections.size() > 0) {
            inodeLoader.loadINodeDirectorySectionInParallel(executorService,
                stageSubSections, summary.getCodec());
          } else {
            inodeLoader.loadINodeDirectorySection(in);
          }
          inodeLoader.waitBlocksMapAndNameCacheUpdateFinished();
          break;
        case FILES_UNDERCONSTRUCTION:
          inodeLoader.loadFilesUnderConstructionSection(in);
          break;
        case SNAPSHOT:
          snapshotLoader.loadSnapshotSection(in);
          break;
        case SNAPSHOT_DIFF:
          snapshotLoader.loadSnapshotDiffSection(in);
          break;
        case SECRET_MANAGER: {
          prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
          Step step = new Step(StepType.DELEGATION_TOKENS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadSecretManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case CACHE_MANAGER: {
          Step step = new Step(StepType.CACHE_POOLS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadCacheManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case ERASURE_CODING:
          Step step = new Step(StepType.ERASURE_CODING_POLICIES);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadErasureCodingSection(in);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
          break;
        default:
          LOG.warn("Unrecognized section {}", n);
          break;
        }
      }
      if (executorService != null) {
        executorService.shutdown();
      }
    }

    private void loadNameSystemSection(InputStream in) throws IOException {
      NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
      BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();
      blockIdManager.setLegacyGenerationStamp(s.getGenstampV1());
      blockIdManager.setGenerationStamp(s.getGenstampV2());
      blockIdManager.setLegacyGenerationStampLimit(s.getGenstampV1Limit());
      blockIdManager.setLastAllocatedContiguousBlockId(s.getLastAllocatedBlockId());
      if (s.hasLastAllocatedStripedBlockId()) {
        blockIdManager.setLastAllocatedStripedBlockId(
            s.getLastAllocatedStripedBlockId());
      }
      imgTxId = s.getTransactionId();
      if (s.hasRollingUpgradeStartTime()
          && fsn.getFSImage().hasRollbackFSImage()) {
        // we set the rollingUpgradeInfo only when we make sure we have the
        // rollback image
        fsn.setRollingUpgradeInfo(true, s.getRollingUpgradeStartTime());
      }
    }

    private void loadStringTableSection(InputStream in) throws IOException {
      StringTableSection s = StringTableSection.parseDelimitedFrom(in);
      ctx.stringTable =
          SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
      for (int i = 0; i < s.getNumEntry(); ++i) {
        StringTableSection.Entry e = StringTableSection.Entry
            .parseDelimitedFrom(in);
        ctx.stringTable.put(e.getId(), e.getStr());
      }
    }

    private void loadSecretManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
      int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
      ArrayList<SecretManagerSection.DelegationKey> keys = Lists
          .newArrayListWithCapacity(numKeys);
      ArrayList<SecretManagerSection.PersistToken> tokens = Lists
          .newArrayListWithCapacity(numTokens);

      for (int i = 0; i < numKeys; ++i)
        keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numTokens; ++i) {
        tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));
      }

      fsn.loadSecretManagerState(s, keys, tokens, counter);
    }

    private void loadCacheManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
      int numPools = s.getNumPools();
      ArrayList<CachePoolInfoProto> pools = Lists
          .newArrayListWithCapacity(numPools);
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity(s.getNumDirectives());
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numPools; ++i) {
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
        counter.increment();
      }
      for (int i = 0; i < s.getNumDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(
          new CacheManager.PersistState(s, pools, directives));
    }

    private void loadErasureCodingSection(InputStream in)
        throws IOException {
      ErasureCodingSection s = ErasureCodingSection.parseDelimitedFrom(in);
      List<ErasureCodingPolicyInfo> ecPolicies = Lists
          .newArrayListWithCapacity(s.getPoliciesCount());
      for (int i = 0; i < s.getPoliciesCount(); ++i) {
        ecPolicies.add(PBHelperClient.convertErasureCodingPolicyInfo(
            s.getPolicies(i)));
      }
      fsn.getErasureCodingPolicyManager().loadPolicies(ecPolicies, conf);
    }
  }

  private static boolean enableParallelSaveAndLoad(Configuration conf) {
    boolean loadInParallel = enableParallelLoad;
    boolean compressionEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY,
        DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);

    if (loadInParallel) {
      if (compressionEnabled) {
        LOG.warn("Parallel Image loading and saving is not supported when {}" +
                " is set to true. Parallel will be disabled.",
            DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY);
        loadInParallel = false;
      }
    }
    return loadInParallel;
  }

  public static void initParallelLoad(Configuration conf) {
    enableParallelLoad =
        conf.getBoolean(DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY,
            DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_DEFAULT);
  }

  public static void refreshParallelSaveAndLoad(boolean enable) {
    enableParallelLoad = enable;
  }

  public static boolean getEnableParallelLoad() {
    return enableParallelLoad;
  }

  public static final class Saver {
    //一个常量，值为 4096，用于确定检查取消操作的间隔。
    public static final int CHECK_CANCEL_INTERVAL = 4096;
    //是否启用子部分（sub-section）写入，默认 false。
    private boolean writeSubSections = false;
    //每个子部分的 inode 数量，默认 Integer.MAX_VALUE
    private int inodesPerSubSection = Integer.MAX_VALUE;
    //提供保存 FSImage 过程中的上下文信息，如 FSNamesystem
    private final SaveNamespaceContext context;
    //Saver 相关的上下文信息。
    private final SaverContext saverContext;
    //记录当前文件偏移量，初始值为 FSImageUtil.MAGIC_HEADER.length
    private long currentOffset = FSImageUtil.MAGIC_HEADER.length;
    //记录子部分的起始偏移量，初始值等于 currentOffset。
    private long subSectionOffset = currentOffset;
    //保存 FSImage 文件的 MD5 哈希值，用于完整性校验。
    private MD5Hash savedDigest;
    //FileOutputStream 的通道，便于管理 FSImage 文件的写入。
    private FileChannel fileChannel;
    // OutputStream for the section data
    //负责写入 FSImage 各个部分的 OutputStream。
    private OutputStream sectionOutputStream;
    //压缩编解码器，用于可选的 FSImage 数据压缩。
    private CompressionCodec codec;
    //最底层的 OutputStream，可以是压缩流或普通流。
    private OutputStream underlyingOutputStream;
    private Configuration conf;

    Saver(SaveNamespaceContext context, Configuration conf) {
      this.context = context;
      this.saverContext = new SaverContext();
      this.conf = conf;
    }
    //MD5Hash，返回 savedDigest，即 FSImage 文件的哈希值，用于校验完整性
    public MD5Hash getSavedDigest() {
      return savedDigest;
    }

    public SaveNamespaceContext getContext() {
      return context;
    }

    public SaverContext getSaverContext() {
      return saverContext;
    }

    public int getInodesPerSubSection() {
      return inodesPerSubSection;
    }

    /**
     * Commit the length and offset of a fsimage section to the summary index,
     * including the sub section, which will be committed before the section is
     * committed.
     * @param summary The image summary object
     * @param name The name of the section to commit
     * @param subSectionName The name of the sub-section to commit
     * @throws IOException
     */
    public void commitSectionAndSubSection(FileSummary.Builder summary,
        SectionName name, SectionName subSectionName) throws IOException {
      commitSubSection(summary, subSectionName);
      commitSection(summary, name);
    }
    //提交一个部分的文件数据，记录该部分的名称、长度和偏移量到文件摘要中，并且更新文件的写入偏移量
    public void commitSection(FileSummary.Builder summary, SectionName name)
        throws IOException {
      //保存了当前文件的偏移量，currentOffset 是保存数据时的当前位置
      long oldOffset = currentOffset;
      //确保当前的数据流被写入磁盘
      flushSectionOutputStream();

      if (codec != null) {
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }
      //获取当前文件的写入位置（即文件的当前偏移量），减去保存的 oldOffset，得出当前部分数据的长度
      long length = fileChannel.position() - oldOffset;
      summary.addSections(FileSummary.Section.newBuilder().setName(name.name)
          .setLength(length).setOffset(currentOffset));
      currentOffset += length;
      subSectionOffset = currentOffset;
    }

    /**
     * Commit the length and offset of a fsimage sub-section to the summary
     * index.
     * @param summary The image summary object
     * @param name The name of the sub-section to commit
     * @throws IOException
     */
    //将 HDFS 中 fsimage 文件的子节（sub-section）的长度和偏移量（offset）记录到 FileSummary 对象中，
    // 方便后续解析 fsimage 文件时快速定位每个子节的位置
    //summary：类型为 FileSummary.Builder，用于构建 fsimage 文件的摘要信息，记录每个子节的名称、长度和偏移量
    //name：类型为 SectionName，表示需要提交的子节的名称
    public void commitSubSection(FileSummary.Builder summary, SectionName name)
        throws IOException {
      //writeSubSections 是一个布尔值，表示是否需要写入子节。当条件不满足时，跳过子节的记录操作
      if (!writeSubSections) {
        return;
      }

      LOG.debug("Saving a subsection for {}", name.toString());
      // The output stream must be flushed before the length is obtained
      // as the flush can move the length forward.
      //强制将缓冲区中的数据写入到 OutputStream，确保后续获取的位置和长度信息是准确的
      sectionOutputStream.flush();
      long length = fileChannel.position() - subSectionOffset;
      if (length == 0) {
        LOG.warn("The requested section for {} is empty. It will not be " +
            "output to the image", name.toString());
        return;
      }
      //向 summary 对象添加一个新的 Section，子节名称（name.name）、子节长度（length）、子节的起始偏移量（subSectionOffset）
      summary.addSections(FileSummary.Section.newBuilder().setName(name.name)
          .setLength(length).setOffset(subSectionOffset));
      subSectionOffset += length;
    }

    private void flushSectionOutputStream() throws IOException {
      if (codec != null) {
        ((CompressionOutputStream) sectionOutputStream).finish();
      }
      sectionOutputStream.flush();
    }

    /**
     * @return number of non-fatal errors detected while writing the image.
     * @throws IOException on fatal error.
     */
    //file (File): 要保存的目标文件。该文件是保存 FSImage 的文件，即文件系统镜像
    //compression (FSImageCompression): 镜像压缩方式，用于指定保存时使用的压缩方式
    //返回值 long: 返回保存镜像时检测到的非致命错误的数量。如果没有错误，则返回 0
    //用于保存文件系统镜像（FSImage）。它会根据配置决定是否启用子部分保存，并通过内部方法进行文件的实际保存操作
    long save(File file, FSImageCompression compression) throws IOException {
      //根据配置和条件决定是否启用子部分（subsections）。如果启用了子部分，后续的图像保存过程将会使用子部分进行保存
      enableSubSectionsIfRequired();
      FileOutputStream fout = new FileOutputStream(file);
      fileChannel = fout.getChannel();
      try {
        LOG.info("Saving image file {} using {}", file, compression);
        long startTime = monotonicNow();
        //调用 saveInternal 方法保存文件内容。保存过程返回检测到的非致命错误数量
        long numErrors = saveInternal(
            fout, compression, file.getAbsolutePath());
        LOG.info("Image file {} of size {} bytes saved in {} seconds {}.", file,
            file.length(), (monotonicNow() - startTime) / 1000,
            (numErrors > 0 ? (" with" + numErrors + " errors") : ""));
        return numErrors;
      } finally {
        fout.close();
      }
    }
    //决定是否启用子部分（subsections）。子部分的启用与配置参数和文件系统中的 inode 数量有关，
    // 主要用于优化大文件系统镜像的保存过程。启用子部分可以将镜像分割成多个较小的部分并行保存
    private void enableSubSectionsIfRequired() {
      //判断是否启用并行保存和加载
      boolean parallelEnabled = enableParallelSaveAndLoad(conf);
      //从配置中获取并行保存的 inode 阈值。只有当 inode 数量大于该阈值时，才会启用子部分保存
      int inodeThreshold = conf.getInt(
          DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY,
          DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT);
      //获取目标子部分的数量，表示将文件系统镜像分成多少个子部分
      int targetSections = conf.getInt(
          DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY,
          DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT);

      if (parallelEnabled) {
        if (targetSections <= 0) {
          LOG.warn("{} is set to {}. It must be greater than zero. Setting to" +
              " default of {}",
              DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY,
              targetSections,
              DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT);
          targetSections =
              DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_DEFAULT;
        }
        if (inodeThreshold <= 0) {
          LOG.warn("{} is set to {}. It must be greater than zero. Setting to" +
                  " default of {}",
              DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY,
              inodeThreshold,
              DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT);
          inodeThreshold =
              DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_DEFAULT;
        }
        //计算 inode 数量并决定是否启用子部分保存
        int inodeCount = context.getSourceNamesystem().dir.getInodeMapSize();
        // Only enable parallel sections if there are enough inodes
        if (inodeCount >= inodeThreshold) {//如果 inode 数量大于或等于 inodeThreshold（即满足启用子部分保存的条件），则启用子部分保存 (writeSubSections = true)
          writeSubSections = true;
          // Calculate the inodes per section rounded up to the nearest int
          inodesPerSubSection = (inodeCount + targetSections - 1) /
              targetSections;
        }
      } else {
        writeSubSections = false;
      }
    }

    private static void saveFileSummary(OutputStream out, FileSummary summary)
        throws IOException {
      summary.writeDelimitedTo(out);
      int length = getOndiskTrunkSize(summary);
      byte[] lengthBytes = new byte[4];
      ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
      out.write(lengthBytes);
    }
    //负责将文件系统中的所有 inode（索引节点）数据保存到镜像文件中。通过调用 FSImageFormatPBINode.Saver 的一系列方法，
    // 它将不同类型的 inode 数据（包括普通文件 inode、目录 inode、文件更新计数等）序列化并写入输出流
    private long saveInodes(FileSummary.Builder summary) throws IOException {
      //负责序列化 inode 数据
      FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this,
          summary);
      //将 inode 数据（文件和目录的 inode）序列化并写入到输出流中
      saver.serializeINodeSection(sectionOutputStream);
      //将 inode 目录数据序列化并写入到输出流中。这个部分包含 inode 树结构，表示文件系统的目录层次
      saver.serializeINodeDirectorySection(sectionOutputStream);
      //序列化文件更新计数（例如，文件操作的次数）并写入输出流
      saver.serializeFilesUCSection(sectionOutputStream);

      return saver.getNumImageErrors();
    }

    /**
     * @return number of non-fatal errors detected while saving the image.
     * @throws IOException on fatal error.
     */
    private long saveSnapshots(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(
          this, summary, context, context.getSourceNamesystem());

      snapshotSaver.serializeSnapshotSection(sectionOutputStream);
      // Skip snapshot-related sections when there is no snapshot.
      if (context.getSourceNamesystem().getSnapshotManager()
          .getNumSnapshots() > 0) {
        snapshotSaver.serializeSnapshotDiffSection(sectionOutputStream);
      }
      snapshotSaver.serializeINodeReferenceSection(sectionOutputStream);
      return snapshotSaver.getNumImageErrors();
    }

    /**
     * @return number of non-fatal errors detected while writing the FsImage.
     * @throws IOException on fatal error.
     */
    //负责将文件系统镜像（FsImage）保存到指定的文件中。它通过多个步骤将文件系统的不同部分（如命名空间、内存快照、委托令牌等）写入输出流，并在过程中计算 MD5 校验和

    private long saveInternal(FileOutputStream fout,
        FSImageCompression compression, String filePath) throws IOException {
      StartupProgress prog = NameNode.getStartupProgress();
      MessageDigest digester = MD5Hash.getDigester();
      //获取文件系统布局版本
      int layoutVersion =
          context.getSourceNamesystem().getEffectiveLayoutVersion();
      //初始化输出流，并写入文件头（MAGIC_HEADER）
      underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(
          fout), digester);
      underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);

      fileChannel = fout.getChannel();
      //初始化文件摘要构建器
      FileSummary.Builder b = FileSummary.newBuilder()
          .setOndiskVersion(FSImageUtil.FILE_VERSION)
          .setLayoutVersion(
              context.getSourceNamesystem().getEffectiveLayoutVersion());

      codec = compression.getImageCodec();
      if (codec != null) {
        b.setCodec(codec.getClass().getCanonicalName());
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }
      //保存命名空间部分
      saveNameSystemSection(b);
      // Check for cancellation right after serializing the name system section.
      // Some unit tests, such as TestSaveNamespace#testCancelSaveNameSpace
      // depends on this behavior.
      context.checkCancelled();

      Step step;

      // Erasure coding policies should be saved before inodes
      //如果布局版本支持 erasure coding 特性，则保存该部分
      if (NameNodeLayoutVersion.supports(
          NameNodeLayoutVersion.Feature.ERASURE_CODING, layoutVersion)) {
        step = new Step(StepType.ERASURE_CODING_POLICIES, filePath);
        prog.beginStep(Phase.SAVING_CHECKPOINT, step);
        saveErasureCodingSection(b);
        prog.endStep(Phase.SAVING_CHECKPOINT, step);
      }
      //保存 inode 和快照部分
      step = new Step(StepType.INODES, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      // Count number of non-fatal errors when saving inodes and snapshots.
      long numErrors = saveInodes(b);
      numErrors += saveSnapshots(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      //保存委托令牌部分
      step = new Step(StepType.DELEGATION_TOKENS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveSecretManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      //保存缓存池部分
      step = new Step(StepType.CACHE_POOLS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveCacheManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);
      //保存字符串表部分
      saveStringTableSection(b);

      // We use the underlyingOutputStream to write the header. Therefore flush
      // the buffered stream (which is potentially compressed) first.
      //刷新输出流： 在写入所有数据之后，刷新当前部分的输出流
      flushSectionOutputStream();
      //保存文件摘要并关闭流： 创建文件摘要，并将其保存到输出流中，然后关闭输出流
      FileSummary summary = b.build();
      saveFileSummary(underlyingOutputStream, summary);
      underlyingOutputStream.close();
      //计算并返回 MD5 校验和
      savedDigest = new MD5Hash(digester.digest());
      return numErrors;
    }

    private void saveSecretManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      DelegationTokenSecretManager.SecretManagerState state = fsn
          .saveSecretManagerState();
      state.section.writeDelimitedTo(sectionOutputStream);
      for (SecretManagerSection.DelegationKey k : state.keys)
        k.writeDelimitedTo(sectionOutputStream);

      for (SecretManagerSection.PersistToken t : state.tokens)
        t.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.SECRET_MANAGER);
    }

    private void saveCacheManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      CacheManager.PersistState state = fsn.getCacheManager().saveState();
      state.section.writeDelimitedTo(sectionOutputStream);

      for (CachePoolInfoProto p : state.pools)
        p.writeDelimitedTo(sectionOutputStream);

      for (CacheDirectiveInfoProto p : state.directives)
        p.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.CACHE_MANAGER);
    }
    //用于将 HDFS 中的纠删编码策略信息保存到文件镜像中。它从文件系统命名空间中获取已持久化的纠删编码策略，并将这些策略以合适的格式序列化到文件输出流中
    private void saveErasureCodingSection(
        FileSummary.Builder summary) throws IOException {
      //获取文件系统命名空间对象
      final FSNamesystem fsn = context.getSourceNamesystem();
      //获取已持久化的纠删编码策略
      ErasureCodingPolicyInfo[] ecPolicies =
          fsn.getErasureCodingPolicyManager().getPersistedPolicies();
      ArrayList<ErasureCodingPolicyProto> ecPolicyProtoes =
          new ArrayList<ErasureCodingPolicyProto>();
      //转换策略对象为协议缓冲格式
      for (ErasureCodingPolicyInfo p : ecPolicies) {
        ecPolicyProtoes.add(PBHelperClient.convertErasureCodingPolicy(p));
      }

      ErasureCodingSection section = ErasureCodingSection.newBuilder().
          addAllPolicies(ecPolicyProtoes).build();
      section.writeDelimitedTo(sectionOutputStream);
      commitSection(summary, SectionName.ERASURE_CODING);
    }
    //负责将文件系统命名空间信息（如命名空间 ID、块生成戳、事务 ID 等）保存到文件中。这是保存文件系统镜像（FsImage）时的一个关键部分，它把与文件系统命名空间相关的信息写入到输出流
    private void saveNameSystemSection(FileSummary.Builder summary)
        throws IOException {
      //获取文件系统命名空间（FSNamesystem）和块 ID 管理器
      final FSNamesystem fsn = context.getSourceNamesystem();
      OutputStream out = sectionOutputStream;
      BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();
      //构建 NameSystemSection 对象
      NameSystemSection.Builder b = NameSystemSection.newBuilder()
          .setGenstampV1(blockIdManager.getLegacyGenerationStamp())
          .setGenstampV1Limit(blockIdManager.getLegacyGenerationStampLimit())
          .setGenstampV2(blockIdManager.getGenerationStamp())
          .setLastAllocatedBlockId(blockIdManager.getLastAllocatedContiguousBlockId())
          .setLastAllocatedStripedBlockId(blockIdManager.getLastAllocatedStripedBlockId())
          .setTransactionId(context.getTxId());

      // We use the non-locked version of getNamespaceInfo here since
      // the coordinating thread of saveNamespace already has read-locked
      // the namespace for us. If we attempt to take another readlock
      // from the actual saver thread, there's a potential of a
      // fairness-related deadlock. See the comments on HDFS-2223.
      b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
      if (fsn.isRollingUpgrade()) {
        b.setRollingUpgradeStartTime(fsn.getRollingUpgradeInfo().getStartTime());
      }
      NameSystemSection s = b.build();
      //将命名空间部分对象写入输出流
      s.writeDelimitedTo(out);
      //提交命名空间部分信息，并更新文件摘要
      commitSection(summary, SectionName.NS_INFO);
    }

    private void saveStringTableSection(FileSummary.Builder summary)
        throws IOException {
      OutputStream out = sectionOutputStream;

      SerialNumberManager.StringTable stringTable =
          SerialNumberManager.getStringTable();
      StringTableSection.Builder b = StringTableSection.newBuilder()
          .setNumEntry(stringTable.size())
          .setMaskBits(stringTable.getMaskBits());
      b.build().writeDelimitedTo(out);
      for (Entry<Integer, String> e : stringTable) {
        StringTableSection.Entry.Builder eb = StringTableSection.Entry
            .newBuilder().setId(e.getKey()).setStr(e.getValue());
        eb.build().writeDelimitedTo(out);
      }
      commitSection(summary, SectionName.STRING_TABLE);
    }
  }

  /**
   * Supported section name. The order of the enum determines the order of
   * loading.
   */
  public enum SectionName {
    NS_INFO("NS_INFO"),
    STRING_TABLE("STRING_TABLE"),
    EXTENDED_ACL("EXTENDED_ACL"),
    ERASURE_CODING("ERASURE_CODING"),
    INODE("INODE"),
    INODE_SUB("INODE_SUB"),
    INODE_REFERENCE("INODE_REFERENCE"),
    INODE_REFERENCE_SUB("INODE_REFERENCE_SUB"),
    SNAPSHOT("SNAPSHOT"),
    INODE_DIR("INODE_DIR"),
    INODE_DIR_SUB("INODE_DIR_SUB"),
    FILES_UNDERCONSTRUCTION("FILES_UNDERCONSTRUCTION"),
    SNAPSHOT_DIFF("SNAPSHOT_DIFF"),
    SNAPSHOT_DIFF_SUB("SNAPSHOT_DIFF_SUB"),
    SECRET_MANAGER("SECRET_MANAGER"),
    CACHE_MANAGER("CACHE_MANAGER");

    private static final SectionName[] values = SectionName.values();

    public static SectionName fromString(String name) {
      for (SectionName n : values) {
        if (n.name.equals(name))
          return n;
      }
      return null;
    }

    private final String name;

    private SectionName(String name) {
      this.name = name;
    }
  }

  private static int getOndiskTrunkSize(
      org.apache.hadoop.thirdparty.protobuf.GeneratedMessageV3 s) {
    return CodedOutputStream.computeUInt32SizeNoTag(s.getSerializedSize())
        + s.getSerializedSize();
  }

  private FSImageFormatProtobuf() {
  }
}
