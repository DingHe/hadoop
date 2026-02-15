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

package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.HashMultiset;
import org.apache.hadoop.thirdparty.com.google.common.collect.Multiset;

/**
 * An abstract {@link InputFormat} that returns {@link CombineFileSplit}'s in 
 * {@link InputFormat#getSplits(JobContext)} method. 
 * 
 * Splits are constructed from the files under the input paths. 
 * A split cannot have files from different pools.
 * Each split returned may contain blocks from different files.
 * If a maxSplitSize is specified, then blocks on the same node are
 * combined to form a single split. Blocks that are left over are
 * then combined with other blocks in the same rack. 
 * If maxSplitSize is not specified, then blocks from the same rack
 * are combined in a single split; no attempt is made to create
 * node-local splits.
 * If the maxSplitSize is equal to the block size, then this class
 * is similar to the default splitting behavior in Hadoop: each
 * block is a locally processed split.
 * Subclasses implement 
 * {@link InputFormat#createRecordReader(InputSplit, TaskAttemptContext)}
 * to construct <code>RecordReader</code>'s for 
 * <code>CombineFileSplit</code>'s.
 * 
 * @see CombineFileSplit
 */
// CombineFileInputFormat 是 Hadoop MapReduce 中一个抽象的 InputFormat 基类，它解决的核心问题是 小文件过多 的效率低下问题
// 在标准的 FileInputFormat（如 TextInputFormat）中，每个 HDFS 块通常对应一个 InputSplit，进而启动一个 Map Task。如果集群中有大量小于 HDFS 块大小（通常 128MB）的小文件，就会产生大量 Map Task。启动和管理这些 Map Task 会带来巨大的开销，降低作业效率。
// CombineFileInputFormat 通过以下方式优化这一过程：
// 合并小文件： 它不像标准方式那样，让每个文件或文件块单独成为一个 Split，而是将多个小文件的逻辑块合并成一个更大的 InputSplit，称为 CombineFileSplit。
// 控制粒度： 它允许用户设置一个最大 Split 大小 (maxSplitSize)，将文件块进行聚合，从而有效减少 Map Task 的数量。
// 数据本地性优化： 在合并过程中，它会尝试最大化数据本地性 (Data Locality)，优先将位于同一 节点 (Node) 或同一 机架 (Rack) 上的文件块合并，以确保 Map Task 能够在数据所在地运行，减少网络传输。
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class CombineFileInputFormat<K, V>
  extends FileInputFormat<K, V> {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(CombineFileInputFormat.class);
  // 用于配置每个节点本地 Split 的最小字节大小。低于此大小的剩余块将被考虑合并到机架本地的 Split 中
  public static final String SPLIT_MINSIZE_PERNODE = 
    "mapreduce.input.fileinputformat.split.minsize.per.node";
  // 用于配置每个机架本地 Split 的最小字节大小。低于此大小的剩余块将被考虑与其他机架的块合并。
  public static final String SPLIT_MINSIZE_PERRACK = 
    "mapreduce.input.fileinputformat.split.minsize.per.rack";
  // ability to limit the size of a single split
  // 最大 Split 大小： 实例属性，用于限制单个 CombineFileSplit 的最大字节大小。这是控制 Map Task 数量和粒度的主要参数。
  private long maxSplitSize = 0;
  // 节点最小 Split 大小： 实例属性，与 SPLIT_MINSIZE_PERNODE 配置对应，控制节点本地 Split 的最小大小。
  private long minSplitSizeNode = 0;
  // 机架最小 Split 大小： 实例属性，与 SPLIT_MINSIZE_PERRACK 配置对应，控制机架本地 Split 的最小大小。
  private long minSplitSizeRack = 0;

  // A pool of input paths filters. A split cannot have blocks from files
  // across multiple pools.
  private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();

  // mapping from a rack name to the set of Nodes in the rack
  // 机架到节点映射
  // 存储集群中机架名称到该机架上所有主机名集合的映射。用于在计算 Split 本地性时快速查找主机。
  private HashMap<String, Set<String>> rackToNodes = 
                            new HashMap<String, Set<String>>();
  /**
   * Specify the maximum size (in bytes) of each split. Each split is
   * approximately equal to the specified size.
   */
  // 设置最大 Split 大小，单位为字节。此值优先于配置中设置的值
  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  /**
   * Specify the minimum size (in bytes) of each split per node.
   * This applies to data that is left over after combining data on a single
   * node into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeNode.
   */
  // 设置节点本地最小 Split 大小。此值优先于配置中设置的值。
  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  /**
   * Specify the minimum size (in bytes) of each split per rack.
   * This applies to data that is left over after combining data on a single
   * rack into splits that are of maximum size specified by maxSplitSize.
   * This leftover data will be combined into its own split if its size
   * exceeds minSplitSizeRack.
   */
  // 设置机架本地最小 Split 大小。此值优先于配置中设置的值。
  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  /**
   * Create a new pool and add the filters to it.
   * A split cannot have files from different pools.
   */
  // 创建路径过滤器池。 将一组 PathFilter 封装成一个 MultiPathFilter 并添加到 pools 列表中。确保同一个 Split 只包含能通过相同 MultiPathFilter 的文件块。
  protected void createPool(List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  /**
   * Create a new pool and add the filters to it. 
   * A pathname can satisfy any one of the specified filters.
   * A split cannot have files from different pools.
   */
  protected void createPool(PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f: filters) {
      multi.add(f);
    }
    pools.add(multi);
  }
  //重写判断文件是否可拆分。 逻辑与 TextInputFormat 相同，检查文件是否被压缩，以及是否使用了 SplittableCompressionCodec（可拆分的压缩格式）。
  // 只有可拆分的文件才会被进一步拆分成小于块大小的逻辑块（在 OneFileInfo 构造函数中实现）。
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * default constructor
   */
  public CombineFileInputFormat() {
  }
  // 生成所有 InputSplit 的主方法
  @Override
  public List<InputSplit> getSplits(JobContext job) 
    throws IOException {
    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;
    // 读取配置： 确定 maxSize、minSizeNode 和 minSizeRack 的最终值（优先使用 set 方法设置的值）
    Configuration conf = job.getConfiguration();

    // the values specified by setxxxSplitSize() takes precedence over the
    // values that might have been specified in the config
    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);
      // If maxSize is not configured, a single split will be generated per
      // node.
    }
    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
      throw new IOException("Minimum split size pernode " + minSizeNode +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
      throw new IOException("Minimum split size per rack " + minSizeRack +
                            " cannot be larger than maximum split size " +
                            maxSize);
    }
    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
      throw new IOException("Minimum split size per node " + minSizeNode +
                            " cannot be larger than minimum split " +
                            "size per rack " + minSizeRack);
    }

    // all the files in input set
    List<FileStatus> stats = listStatus(job);
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (stats.size() == 0) {
      return splits;    
    }

    // In one single iteration, process all the paths in a single pool.
    // Processing one pool at a time ensures that a split contains paths
    // from a single pool only.
    // 遍历路径池： 遍历 pools 列表，将匹配每个池的文件单独分组
    for (MultiPathFilter onepool : pools) {
      ArrayList<FileStatus> myPaths = new ArrayList<FileStatus>();
      
      // pick one input path. If it matches all the filters in a pool,
      // add it to the output set
      for (Iterator<FileStatus> iter = stats.iterator(); iter.hasNext();) {
        FileStatus p = iter.next();
        if (onepool.accept(p.getPath())) {
          myPaths.add(p); // add it to my output set
          iter.remove();
        }
      }
      // create splits for all files in this pool.
      // 生成 Split： 对每个文件分组调用 getMoreSplits 方法，生成实际的 CombineFileSplit 列表
      getMoreSplits(job, myPaths, maxSize, minSizeNode, minSizeRack, splits);
    }

    // create splits for all files that are not in any pool.
    getMoreSplits(job, stats, maxSize, minSizeNode, minSizeRack, splits);

    // free up rackToNodes map
    rackToNodes.clear();
    return splits;    
  }

  /**
   * Return all the splits in the specified set of paths
   */
  // 生成单个文件分组的 Split
  private void getMoreSplits(JobContext job, List<FileStatus> stats,
                             long maxSize, long minSizeNode, long minSizeRack,
                             List<InputSplit> splits)
    throws IOException {
    Configuration conf = job.getConfiguration();

    // all blocks for all the files in input set
    OneFileInfo[] files;
  
    // mapping from a rack name to the list of blocks it has
    HashMap<String, List<OneBlockInfo>> rackToBlocks = 
                              new HashMap<String, List<OneBlockInfo>>();

    // mapping from a block to the nodes on which it has replicas
    HashMap<OneBlockInfo, String[]> blockToNodes = 
                              new HashMap<OneBlockInfo, String[]>();

    // mapping from a node to the list of blocks that it contains
    HashMap<String, Set<OneBlockInfo>> nodeToBlocks = 
                              new HashMap<String, Set<OneBlockInfo>>();
    
    files = new OneFileInfo[stats.size()];
    if (stats.size() == 0) {
      return; 
    }

    // populate all the blocks for all files
    long totLength = 0;
    int i = 0;
    // 遍历文件列表，为每个文件创建 OneFileInfo 对象，收集该文件的所有块信息、节点位置和机架位置，
    // 并填充到 rackToBlocks、blockToNodes 和 nodeToBlocks 等映射中
    for (FileStatus stat : stats) {
      files[i] = new OneFileInfo(stat, conf, isSplitable(job, stat.getPath()),
                                 rackToBlocks, blockToNodes, nodeToBlocks,
                                 rackToNodes, maxSize);
      totLength += files[i].getLength();
    }
    createSplits(nodeToBlocks, blockToNodes, rackToBlocks, totLength, 
                 maxSize, minSizeNode, minSizeRack, splits);
  }

  /**
   * Process all the nodes and create splits that are local to a node.
   * Generate one split per node iteration, and walk over nodes multiple times
   * to distribute the splits across nodes.
   * <p>
   * Note: The order of processing the nodes is undetermined because the
   * implementation of nodeToBlocks is {@link java.util.HashMap} and its order
   * of the entries is undetermined.
   * @param nodeToBlocks Mapping from a node to the list of blocks that
   *                     it contains.
   * @param blockToNodes Mapping from a block to the nodes on which
   *                     it has replicas.
   * @param rackToBlocks Mapping from a rack name to the list of blocks it has.
   * @param totLength Total length of the input files.
   * @param maxSize Max size of each split.
   *                If set to 0, disable smoothing load.
   * @param minSizeNode Minimum split size per node.
   * @param minSizeRack Minimum split size per rack.
   * @param splits New splits created by this method are added to the list.
   */
  // CombineFileInputFormat 的核心，它实现了数据本地性优先的合并算法，将许多文件块 (OneBlockInfo) 聚合成较少但大小均衡的 InputSplit
  @VisibleForTesting
  void createSplits(Map<String, Set<OneBlockInfo>> nodeToBlocks,
                     Map<OneBlockInfo, String[]> blockToNodes,
                     Map<String, List<OneBlockInfo>> rackToBlocks,
                     long totLength,
                     long maxSize,
                     long minSizeNode,
                     long minSizeRack,
                     List<InputSplit> splits                     
                    ) {
    // 初始化一个动态列表，用于临时存放当前正在构建的 InputSplit 所包含的块 (OneBlockInfo)
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    // 初始化一个长整型变量，用于记录 validBlocks 中块的累计总大小，即当前正在构建的 Split 的大小。
    long curSplitSize = 0;
    // 获取集群中所有包含输入文件块的节点的总数量。
    int totalNodes = nodeToBlocks.size();
    //复制总输入数据长度（字节数），用于后续跟踪还剩多少数据未分配给 Split。
    long totalLength = totLength;
    // 用于记录每个节点已经创建了多少个 Split。用于实现负载均衡
    Multiset<String> splitsPerNode = HashMultiset.create();
    // 用于记录所有已处理完毕的节点。一旦节点上的所有本地块都已分配，该节点即被标记为完成。
    Set<String> completedNodes = new HashSet<String>();
    // 节点本地合并 (Node-Local Splitting)
    // 阶段的核心目标是最大化节点本地性。它通过循环遍历节点，将节点上的块尽可能合并成大小接近 maxSize 的 Split
    while(true) {
      //遍历 nodeToBlocks 映射，一次处理一个节点上的所有块。
      for (Iterator<Map.Entry<String, Set<OneBlockInfo>>> iter = nodeToBlocks
          .entrySet().iterator(); iter.hasNext();) {
        Map.Entry<String, Set<OneBlockInfo>> one = iter.next();
        //获取当前正在处理的节点的主机名。
        String node = one.getKey();
        
        // Skip the node if it has previously been marked as completed.
        //如果该节点之前已被标记为完成（即该节点上的所有块都已被分配），则跳过该节点，继续处理下一个节点。
        if (completedNodes.contains(node)) {
          continue;
        }

        Set<OneBlockInfo> blocksInCurrentNode = one.getValue();

        // for each block, copy it into validBlocks. Delete it from
        // blockToNodes so that the same block does not appear in
        // two different splits.
        Iterator<OneBlockInfo> oneBlockIter = blocksInCurrentNode.iterator();
        //如果该节点之前已被标记为完成（即该节点上的所有块都已被分配），则跳过该节点，继续处理下一个节点。
        while (oneBlockIter.hasNext()) {
          OneBlockInfo oneblock = oneBlockIter.next();
          
          // Remove all blocks which may already have been assigned to other
          // splits.
          if(!blockToNodes.containsKey(oneblock)) {
            oneBlockIter.remove();
            continue;
          }
          // 将当前块添加到正在构建的 Split (即 validBlocks) 中。
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          // if the accumulated split size exceeds the maximum, then
          // create this split.
          // Split 创建点（达到最大值）
          if (maxSize != 0 && curSplitSize >= maxSize) {
            // create an input split and add it to the splits array
            // 如果设置了 maxSize 且当前累计大小达到或超过 maxSize： 1. 调用 addCreatedSplit 创建一个节点本地 Split
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            curSplitSize = 0;

            splitsPerNode.add(node);

            // Remove entries from blocksInNode so that we don't walk these
            // again.
            blocksInCurrentNode.removeAll(validBlocks);
            validBlocks.clear();

            // Done creating a single split for this node. Move on to the next
            // node so that splits are distributed across nodes.
            break;
          }

        }

        //处理剩余块： 如果内部循环结束后 validBlocks 中还有块（即未达到 maxSize）
        if (validBlocks.size() != 0) {
          // This implies that the last few blocks (or all in case maxSize=0)
          // were not part of a split. The node is complete.
          
          // if there were any blocks left over and their combined size is
          // larger than minSplitNode, then combine them into one split.
          // Otherwise add them back to the unprocessed pool. It is likely
          // that they will be combined with other blocks from the
          // same rack later on.
          // This condition also kicks in when max split size is not set. All
          // blocks on a node will be grouped together into a single split.
          //Split 创建点（达到最小节点大小）： 如果满足以下所有条件：
          // 1. 设置了 minSizeNode 且累计大小达到或超过它。
          // 2. 当前节点尚未创建任何 Split (splitsPerNode.count(node) == 0)。
          // 则创建这个 Split，以保证该节点有机会并行执行 Map Task。
          if (minSizeNode != 0 && curSplitSize >= minSizeNode
              && splitsPerNode.count(node) == 0) {
            // haven't created any split on this machine. so its ok to add a
            // smaller one for parallelism. Otherwise group it in the rack for
            // balanced size create an input split and add it to the splits
            // array
            addCreatedSplit(splits, Collections.singleton(node), validBlocks);
            totalLength -= curSplitSize;
            splitsPerNode.add(node);
            // Remove entries from blocksInNode so that we don't walk this again.
            blocksInCurrentNode.removeAll(validBlocks);
            // The node is done. This was the last set of blocks for this node.
          } else {
            // Put the unplaced blocks back into the pool for later rack-allocation.
            //归还未使用的块： 否则，将 validBlocks 中的块重新添加回 blockToNodes（归还到未分配池中）。
            // 这些块将留待之后的机架本地合并阶段处理。
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
          }
          validBlocks.clear();
          curSplitSize = 0;
          completedNodes.add(node);
        } else { // No in-flight blocks.
          if (blocksInCurrentNode.size() == 0) {
            // Node is done. All blocks were fit into node-local splits.
            //将当前节点标记为完成，因为它要么创建了 Split，要么将剩余块归还了。
            completedNodes.add(node);
          } // else Run through the node again.
        }
      }

      // Check if node-local assignments are complete.
      if (completedNodes.size() == totalNodes || totalLength == 0) {
        // All nodes have been walked over and marked as completed or all blocks
        // have been assigned. The rest should be handled via rackLock assignment.
        LOG.debug("Terminated node allocation with : CompletedNodes: {}, size left: {}",
            completedNodes.size(), totalLength);
        break;
      }
    }

    // if blocks in a rack are below the specified minimum size, then keep them
    // in 'overflow'. After the processing of all racks is complete, these 
    // overflow blocks will be combined into splits.
    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    Set<String> racks = new HashSet<String>();
    //阶段三：机架本地合并和溢出处理 (Rack-Local and Overflow Splitting)
    //这一阶段处理在节点本地分配中未能分配的剩余块，尝试在机架级别进行合并，以维护机架本地性。
    // Process all racks over and over again until there is no more work to do.
    while (blockToNodes.size() > 0) {

      // Create one split for this rack before moving over to the next rack. 
      // Come back to this rack after creating a single split for each of the 
      // remaining racks.
      // Process one rack location at a time, Combine all possible blocks that
      // reside on this rack as one split. (constrained by minimum and maximum
      // split size).

      // iterate over all racks 
      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = 
           rackToBlocks.entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        racks.add(one.getKey());
        List<OneBlockInfo> blocks = one.getValue();

        // for each block, copy it into validBlocks. Delete it from 
        // blockToNodes so that the same block does not appear in 
        // two different splits.
        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;
      
            // if the accumulated split size exceeds the maximum, then 
            // create this split.
            if (maxSize != 0 && curSplitSize >= maxSize) {
              // create an input split and add it to the splits array
              addCreatedSplit(splits, getHosts(racks), validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        // if we created a split, then just go to the next rack
        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            // if there is a minimum size specified, then create a single split
            // otherwise, store these blocks into overflow data structure
            addCreatedSplit(splits, getHosts(racks), validBlocks);
          } else {
            // There were a few blocks in this rack that 
        	// remained to be processed. Keep them in 'overflow' block list. 
        	// These will be combined later.
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    // Process all overflow blocks
    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      // This might cause an exiting rack location to be re-added,
      // but it should be ok.
      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.racks[i]);
      }

      // if the accumulated split size exceeds the maximum, then 
      // create this split.
      if (maxSize != 0 && curSplitSize >= maxSize) {
        // create an input split and add it to the splits array
        addCreatedSplit(splits, getHosts(racks), validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    // Process any remaining blocks, if any.
    if (!validBlocks.isEmpty()) {
      addCreatedSplit(splits, getHosts(racks), validBlocks);
    }
  }

  /**
   * Create a single split from the list of blocks specified in validBlocks
   * Add this new split into splitList.
   */
  private void addCreatedSplit(List<InputSplit> splitList, 
                               Collection<String> locations, 
                               ArrayList<OneBlockInfo> validBlocks) {
    // create an input split
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath; 
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }
     // add this split to the list that is returned
    CombineFileSplit thissplit = new CombineFileSplit(fl, offset, 
                                   length, locations.toArray(new String[0]));
    splitList.add(thissplit); 
  }

  /**
   * This is not implemented yet. 
   */
  public abstract RecordReader<K, V> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException;

  /**
   * information about one file from the File System
   */
  // 用于在 Driver 端收集单个文件信息和数据块本地性信息的辅助类
  @VisibleForTesting
  static class OneFileInfo {
    // 文件大小
    //存储当前文件（OneFileInfo 所代表的文件）的总字节大小。
    private long fileSize;               // size of the file
    // 存储该文件所有数据块的数组。每个元素 OneBlockInfo 包含一个数据块的路径、偏移量、长度以及所在的主机和机架信息
    private OneBlockInfo[] blocks;       // all blocks in this file

    OneFileInfo(FileStatus stat, Configuration conf,
                boolean isSplitable,
                HashMap<String, List<OneBlockInfo>> rackToBlocks,
                HashMap<OneBlockInfo, String[]> blockToNodes,
                HashMap<String, Set<OneBlockInfo>> nodeToBlocks,
                HashMap<String, Set<String>> rackToNodes,
                long maxSize)
                throws IOException {
      this.fileSize = 0;

      // get block locations from file system
      // 获取块位置
      // 尝试从 FileStatus（如果它是 LocatedFileStatus 的子类，如 HDFS 文件状态）或通过调用文件系统 (FileSystem) API，
      // 获取该文件的所有 BlockLocation 数组，包含每个数据块的偏移、长度和副本位置。
      BlockLocation[] locations;
      if (stat instanceof LocatedFileStatus) {
        locations = ((LocatedFileStatus) stat).getBlockLocations();
      } else {
        FileSystem fs = stat.getPath().getFileSystem(conf);
        locations = fs.getFileBlockLocations(stat, 0, stat.getLen());
      }
      // create a list of all block and their locations
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {

        if(locations.length == 0 && !stat.isDirectory()) {
          locations = new BlockLocation[] { new BlockLocation() };
        }
        //根据文件是否可切分 (isSplitable) 来决定如何处理文件块。不可切分文件（如某些压缩文件）将被视为一个完整的块
        if (!isSplitable) {
          // if the file is not splitable, just create the one block with
          // full file length
          if (LOG.isDebugEnabled()) {
            LOG.debug("File is not splittable so no parallelization "
                + "is possible: " + stat.getPath());
          }
          // 如果不可切分，则创建一个包含文件完整长度的 OneBlockInfo 数组，并使用第一个块位置的信息作为该文件的位置。
          blocks = new OneBlockInfo[1];
          fileSize = stat.getLen();
          blocks[0] = new OneBlockInfo(stat.getPath(), 0, fileSize,
              locations[0].getHosts(), locations[0].getTopologyPaths());
        } else {
          //果可切分，则遍历 locations 数组，对每个 Hadoop 块进行二次切分。
          // 如果当前块长度大于 maxSize，则将其切分成多个不大于 maxSize 的逻辑块（OneBlockInfo），并使用启发式方法避免产生过小的 Split。
          ArrayList<OneBlockInfo> blocksList = new ArrayList<OneBlockInfo>(
              locations.length);
          for (int i = 0; i < locations.length; i++) {
            fileSize += locations[i].getLength();

            // each split can be a maximum of maxSize
            long left = locations[i].getLength();
            long myOffset = locations[i].getOffset();
            long myLength = 0;
            do {
              if (maxSize == 0) {
                myLength = left;
              } else {
                if (left > maxSize && left < 2 * maxSize) {
                  // if remainder is between max and 2*max - then
                  // instead of creating splits of size max, left-max we
                  // create splits of size left/2 and left/2. This is
                  // a heuristic to avoid creating really really small
                  // splits.
                  myLength = left / 2;
                } else {
                  myLength = Math.min(maxSize, left);
                }
              }
              OneBlockInfo oneblock = new OneBlockInfo(stat.getPath(),
                  myOffset, myLength, locations[i].getHosts(),
                  locations[i].getTopologyPaths());
              left -= myLength;
              myOffset += myLength;

              blocksList.add(oneblock);
            } while (left > 0);
          }
          blocks = blocksList.toArray(new OneBlockInfo[blocksList.size()]);
        }
        // 填充本地性映射
        populateBlockInfo(blocks, rackToBlocks, blockToNodes, 
                          nodeToBlocks, rackToNodes);
      }
    }
    //遍历文件中的所有 OneBlockInfo，并将这些块信息添加到全局映射中，以便后续的 CombineFileSplit 算法能够根据本地性进行分组。
    @VisibleForTesting
    static void populateBlockInfo(OneBlockInfo[] blocks,
                          Map<String, List<OneBlockInfo>> rackToBlocks,
                          Map<OneBlockInfo, String[]> blockToNodes,
                          Map<String, Set<OneBlockInfo>> nodeToBlocks,
                          Map<String, Set<String>> rackToNodes) {
      for (OneBlockInfo oneblock : blocks) {
        // add this block to the block --> node locations map
        blockToNodes.put(oneblock, oneblock.hosts);

        // For blocks that do not have host/rack information,
        // assign to default  rack.
        String[] racks = null;
        if (oneblock.hosts.length == 0) {
          racks = new String[]{NetworkTopology.DEFAULT_RACK};
        } else {
          racks = oneblock.racks;
        }

        // add this block to the rack --> block map
        for (int j = 0; j < racks.length; j++) {
          String rack = racks[j];
          List<OneBlockInfo> blklist = rackToBlocks.get(rack);
          if (blklist == null) {
            blklist = new ArrayList<OneBlockInfo>();
            rackToBlocks.put(rack, blklist);
          }
          blklist.add(oneblock);
          if (!racks[j].equals(NetworkTopology.DEFAULT_RACK)) {
            // Add this host to rackToNodes map
            addHostToRack(rackToNodes, racks[j], oneblock.hosts[j]);
          }
        }

        // add this block to the node --> block map
        for (int j = 0; j < oneblock.hosts.length; j++) {
          String node = oneblock.hosts[j];
          Set<OneBlockInfo> blklist = nodeToBlocks.get(node);
          if (blklist == null) {
            blklist = new LinkedHashSet<OneBlockInfo>();
            nodeToBlocks.put(node, blklist);
          }
          blklist.add(oneblock);
        }
      }
    }

    long getLength() {
      return fileSize;
    }

    OneBlockInfo[] getBlocks() {
      return blocks;
    }
  }

  /**
   * information about one block from the File System
   */
  @VisibleForTesting
  static class OneBlockInfo {
    Path onepath;                // name of this file
    long offset;                 // offset in file
    long length;                 // length of this block
    String[] hosts;              // nodes on which this block resides
    String[] racks;              // network topology of hosts

    OneBlockInfo(Path path, long offset, long len, 
                 String[] hosts, String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length ||
              topologyPaths.length == 0);

      // if the file system does not have any rack information, then
      // use dummy rack location.
      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i], 
                              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      // The topology paths have the host name included as the last 
      // component. Strip it.
      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  protected BlockLocation[] getFileBlockLocations(
    FileSystem fs, FileStatus stat) throws IOException {
    if (stat instanceof LocatedFileStatus) {
      return ((LocatedFileStatus) stat).getBlockLocations();
    }
    return fs.getFileBlockLocations(stat, 0, stat.getLen());
  }

  private static void addHostToRack(Map<String, Set<String>> rackToNodes,
                                    String rack, String host) {
    Set<String> hosts = rackToNodes.get(rack);
    if (hosts == null) {
      hosts = new HashSet<String>();
      rackToNodes.put(rack, hosts);
    }
    hosts.add(host);
  }
  
  private Set<String> getHosts(Set<String> racks) {
    Set<String> hosts = new HashSet<String>();
    for (String rack : racks) {
      if (rackToNodes.containsKey(rack)) {
        hosts.addAll(rackToNodes.get(rack));
      }
    }
    return hosts;
  }
  
  /**
   * Accept a path only if any one of filters given in the
   * constructor do. 
   */
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f: filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }
}
