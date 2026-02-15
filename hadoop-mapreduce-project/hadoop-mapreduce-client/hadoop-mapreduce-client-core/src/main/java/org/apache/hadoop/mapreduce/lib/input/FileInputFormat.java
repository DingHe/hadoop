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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.LocatedFileStatusFetcher;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.FileUtil.maybeIgnoreMissingDirectory;

/**
 * A base class for file-based {@link InputFormat}s.
 *
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobContext)}.
 *
 * Implementations of <code>FileInputFormat</code> can also override the
 * {@link #isSplitable(JobContext, Path)} method to prevent input files
 * from being split-up in certain situations. Implementations that may
 * deal with non-splittable files <i>must</i> override this method, since
 * the default implementation assumes splitting is always possible.
 */
// FileInputFormat 是所有基于文件的数据输入格式（如 TextInputFormat、SequenceFileInputFormat）的基类。
// 它的主要作用是管理文件系统中的**输入路径、文件过滤和数据分块（InputSplit）**逻辑。
// 核心功能概括：
// 输入路径管理： 提供设置和获取 MapReduce 作业输入路径的静态方法。
// 文件列表和过滤： 负责递归地遍历输入目录、应用内置的（例如忽略隐藏文件）和用户自定义的路径过滤器，生成最终要处理的文件列表（FileStatus）。
// 数据分块（Split）逻辑： 实现了将文件列表转换为 Map Task 可以处理的逻辑分块（InputSplit，通常是 FileSplit）的通用算法。这个算法考虑了文件的可拆分性、HDFS 块大小、以及用户设置的最小/最大分块大小限制。
// 数据本地性： 在生成 Split 时，获取文件的块位置信息，为 Map Task 调度提供数据本地性提示。
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {
  // 用于存储 MapReduce 作业的输入路径列表（逗号分隔）
  public static final String INPUT_DIR = 
    "mapreduce.input.fileinputformat.inputdir";
  // 用于设置单个 InputSplit 的最大字节大小。
  public static final String SPLIT_MAXSIZE = 
    "mapreduce.input.fileinputformat.split.maxsize";
  // 用于设置单个 InputSplit 的最小字节大小
  public static final String SPLIT_MINSIZE = 
    "mapreduce.input.fileinputformat.split.minsize";
  // 用于指定用户自定义的 PathFilter 类，以过滤输入路径中的文件
  public static final String PATHFILTER_CLASS = 
    "mapreduce.input.pathFilter.class";
  // 用于存储最终确定要处理的输入文件总数（通常在 getSplits 运行后设置）
  public static final String NUM_INPUT_FILES =
    "mapreduce.input.fileinputformat.numinputfiles";
  // 用于设置是否递归地遍历输入目录下的所有子目录来查找文件
  public static final String INPUT_DIR_RECURSIVE =
    "mapreduce.input.fileinputformat.input.dir.recursive";
  public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS =
    "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs";
  // 设置在 listStatus 方法中并行获取文件状态信息时使用的线程数，以加速大规模文件列表的生成
  public static final String LIST_STATUS_NUM_THREADS =
      "mapreduce.input.fileinputformat.list-status.num-threads";
  public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;

  private static final Logger LOG =
      LoggerFactory.getLogger(FileInputFormat.class);
  // 分块计算中的容差因子（默认为 1.1，即 10%）。用于确保剩余文件字节数接近 Split 大小时，仍作为一个 Split 处理，而不是创建一个过小的 Split
  private static final double SPLIT_SLOP = 1.1;   // 10% slop
  
  @Deprecated
  public enum Counter {
    BYTES_READ
  }
  // 内置的路径过滤器。用于忽略以 _ 或 . 开头的文件或目录（如 HDFS 的 _SUCCESS 文件、.crc 文件等）
  private static final PathFilter hiddenFileFilter = new PathFilter(){
      public boolean accept(Path p){
        String name = p.getName(); 
        return !name.startsWith("_") && !name.startsWith("."); 
      }
    }; 

  /**
   * Proxy PathFilter that accepts a path only if all filters given in the
   * constructor do. Used by the listPaths() to apply the built-in
   * hiddenFileFilter together with a user provided one (if any).
   */
  // 多重路径过滤器。 实现了 PathFilter 接口。它接收一个 PathFilter 列表，只有当所有内部过滤器都接受（accept 返回 true）给定的路径时，它才接受该路径
  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }
  
  /**
   * @param job
   *          the job to modify
   * @param inputDirRecursive
   */
  // 设置作业是否应递归扫描输入目录
  public static void setInputDirRecursive(Job job,
      boolean inputDirRecursive) {
    job.getConfiguration().setBoolean(INPUT_DIR_RECURSIVE,
        inputDirRecursive);
  }
 
  /**
   * @param job
   *          the job to look at.
   * @return should the files to be read recursively?
   */
  public static boolean getInputDirRecursive(JobContext job) {
    return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE,
        false);
  }

  /**
   * Get the lower bound on split size imposed by the format.
   * @return the number of bytes of the minimal split for this format
   */
  // 获取该具体 InputFormat 实例施加的最小 Split 大小约束。默认返回 1。子类可以重写此方法
  protected long getFormatMinSplitSize() {
    return 1;
  }

  /**
   * Is the given filename splittable? Usually, true, but if the file is
   * stream compressed, it will not be.
   *
   * The default implementation in <code>FileInputFormat</code> always returns
   * true. Implementations that may deal with non-splittable files <i>must</i>
   * override this method.
   *
   * <code>FileInputFormat</code> implementations can override this and return
   * <code>false</code> to ensure that individual input files are never split-up
   * so that {@link Mapper}s process entire files.
   * 
   * @param context the job context
   * @param filename the file name to check
   * @return is this file splitable?
   */
  // 检查给定文件是否可以被拆分成多个 Split。默认返回 true。对于不可拆分的文件格式（如 gzip 压缩文件），子类必须重写此方法返回 false
  protected boolean isSplitable(JobContext context, Path filename) {
    return true;
  }

  /**
   * Set a PathFilter to be applied to the input paths for the map-reduce job.
   * @param job the job to modify
   * @param filter the PathFilter class use for filtering the input paths.
   */
  // 设置用于过滤输入文件的用户自定义 PathFilter 类
  public static void setInputPathFilter(Job job,
                                        Class<? extends PathFilter> filter) {
    job.getConfiguration().setClass(PATHFILTER_CLASS, filter, 
                                    PathFilter.class);
  }

  /**
   * Set the minimum input split size
   * @param job the job to modify
   * @param size the minimum size
   */
  // 设置 Map Task 处理的 InputSplit 的最小字节大小
  public static void setMinInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MINSIZE, size);
  }

  /**
   * Get the minimum split size
   * @param job the job
   * @return the minimum number of bytes that can be in a split
   */
  // 获取配置的最小 Split 大小
  public static long getMinSplitSize(JobContext job) {
    return job.getConfiguration().getLong(SPLIT_MINSIZE, 1L);
  }

  /**
   * Set the maximum split size
   * @param job the job to modify
   * @param size the maximum split size
   */
  // 设置 Map Task 处理的 InputSplit 的最大字节大小
  public static void setMaxInputSplitSize(Job job,
                                          long size) {
    job.getConfiguration().setLong(SPLIT_MAXSIZE, size);
  }

  /**
   * Get the maximum split size.
   * @param context the job to look at.
   * @return the maximum number of bytes a split can include
   */
  // 获取配置的最大 Split 大小
  public static long getMaxSplitSize(JobContext context) {
    return context.getConfiguration().getLong(SPLIT_MAXSIZE, 
                                              Long.MAX_VALUE);
  }

  /**
   * Get a PathFilter instance of the filter set for the input paths.
   *
   * @return the PathFilter instance set for the job, NULL if none has been set.
   */
  // 实例化并返回用户为作业设置的 PathFilter 实例（如果已设置）
  public static PathFilter getInputPathFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null,
        PathFilter.class);
    return (filterClass != null) ?
        (PathFilter) ReflectionUtils.newInstance(filterClass, conf) : null;
  }

  /**
   * List input directories.
   * Subclasses may override to, e.g., select only files matching a regular
   * expression. 
   *
   * If security is enabled, this method collects
   * delegation tokens from the input paths and adds them to the job's
   * credentials.
   * @param job the job to list input paths for and attach tokens to.
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  // 核心文件发现逻辑
  protected List<FileStatus> listStatus(JobContext job
                                        ) throws IOException {
    // 获取所有输入路径并检查其有效性
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    
    // get tokens for all the required FileSystems..
    // 获取安全凭证
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, 
                                        job.getConfiguration());

    // Whether we need to recursive look into the directory structure
    boolean recursive = getInputDirRecursive(job);

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    //应用过滤器： 结合内置的 hiddenFileFilter 和用户自定义的 PathFilter 形成 MultiPathFilter
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    
    List<FileStatus> result = null;
    //遍历文件： 根据配置的线程数（LIST_STATUS_NUM_THREADS）选择单线程或多线程方式，
    // 递归或非递归地遍历路径，生成符合条件的文件状态列表 (FileStatus 或 LocatedFileStatus)
    int numThreads = job.getConfiguration().getInt(LIST_STATUS_NUM_THREADS,
        DEFAULT_LIST_STATUS_NUM_THREADS);
    StopWatch sw = new StopWatch().start();
    if (numThreads == 1) {
      result = singleThreadedListStatus(job, dirs, inputFilter, recursive);
    } else {
      Iterable<FileStatus> locatedFiles = null;
      try {
        LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
            job.getConfiguration(), dirs, recursive, inputFilter, true);
        locatedFiles = locatedFileStatusFetcher.getFileStatuses();
      } catch (InterruptedException e) {
        throw (IOException)
            new InterruptedIOException(
                "Interrupted while getting file statuses")
                .initCause(e);
      }
      result = Lists.newArrayList(locatedFiles);
    }
    
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to get FileStatuses: "
          + sw.now(TimeUnit.MILLISECONDS));
    }
    LOG.info("Total input files to process : " + result.size());
    return result;
  }

  private List<FileStatus> singleThreadedListStatus(JobContext job, Path[] dirs,
      PathFilter inputFilter, boolean recursive) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    List<IOException> errors = new ArrayList<IOException>();
    for (int i=0; i < dirs.length; ++i) {
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration()); 
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat: matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter =
                fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(),
                      inputFilter);
                } else {
                  result.add(shrinkStatus(stat));
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return result;
  }
  
  /**
   * Add files in the input path recursively into the results.
   * @param result
   *          The List to store all files.
   * @param fs
   *          The FileSystem.
   * @param path
   *          The input path.
   * @param inputFilter
   *          The input filter that can be used to filter files/dirs. 
   * @throws IOException
   */
  protected void addInputPathRecursively(List<FileStatus> result,
      FileSystem fs, Path path, PathFilter inputFilter) 
      throws IOException {
    // FNFE exceptions are caught whether raised in the list call,
    // or in the hasNext() or next() calls, where async reporting
    // may take place.
    try {
      RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
      while (iter.hasNext()) {
        LocatedFileStatus stat = iter.next();
        if (inputFilter.accept(stat.getPath())) {
          if (stat.isDirectory()) {
            addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
          } else {
            result.add(shrinkStatus(stat));
          }
        }
      }
    } catch (FileNotFoundException e) {
      // unless the store is capabile of list inconsistencies, rethrow.
      // because this is recursive, the caller method may also end up catching
      // and rethrowing, which is slighly inefficient but harmless.
      maybeIgnoreMissingDirectory(fs, path, e);
    }
  }

  /**
   * The HdfsBlockLocation includes a LocatedBlock which contains messages
   * for issuing more detailed queries to datanodes about a block, but these
   * messages are useless during job submission currently. This method tries
   * to exclude the LocatedBlock from HdfsBlockLocation by creating a new
   * BlockLocation from original, reshaping the LocatedFileStatus,
   * allowing {@link #listStatus(JobContext)} to scan more files with less
   * memory footprint.
   * @see BlockLocation
   * @see org.apache.hadoop.fs.HdfsBlockLocation
   * @param origStat The fat FileStatus.
   * @return The FileStatus that has been shrunk.
   */
  public static FileStatus shrinkStatus(FileStatus origStat) {
    if (origStat.isDirectory() || origStat.getLen() == 0 ||
        !(origStat instanceof LocatedFileStatus)) {
      return origStat;
    } else {
      BlockLocation[] blockLocations =
          ((LocatedFileStatus)origStat).getBlockLocations();
      BlockLocation[] locs = new BlockLocation[blockLocations.length];
      int i = 0;
      for (BlockLocation location : blockLocations) {
        locs[i++] = new BlockLocation(location);
      }
      LocatedFileStatus newStat = new LocatedFileStatus(origStat, locs);
      return newStat;
    }
  }

  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts) {
    return new FileSplit(file, start, length, hosts);
  }
  
  /**
   * A factory that makes the split for this class. It can be overridden
   * by sub-classes to make sub-types
   */
  protected FileSplit makeSplit(Path file, long start, long length, 
                                String[] hosts, String[] inMemoryHosts) {
    return new FileSplit(file, start, length, hosts, inMemoryHosts);
  }

  /** 
   * Generate the list of files and make them into FileSplits.
   * @param job the job context
   * @throws IOException
   */
  // 核心分块生成逻辑
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    // 调用 listStatus 获取所有符合条件的文件列表
    List<FileStatus> files = listStatus(job);
    //是否递归遍历文件目录
    boolean ignoreDirs = !getInputDirRecursive(job)
      && job.getConfiguration().getBoolean(INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS, false);
    //遍历每个文件
    for (FileStatus file: files) {
      if (ignoreDirs && file.isDirectory()) {
        continue;
      }
      Path path = file.getPath();
      //取到文件的长度
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        //如果文件可拆分（isSplitable 为 true）
        if (isSplitable(job, path)) {
          long blockSize = file.getBlockSize();
          // 计算 Split 大小（computeSplitSize），该大小是 HDFS 块大小、minSize 和 maxSize 的中值。
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);
          //循环地将文件分割成多个 FileSplit，直到文件结束
          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                       blkLocations[blkIndex].getHosts(),
                       blkLocations[blkIndex].getCachedHosts()));
          }
        } else { // not splitable
          if (LOG.isDebugEnabled()) {
            // Log only if the file is big enough to be splitted
            if (length > Math.min(file.getBlockSize(), minSize)) {
              LOG.debug("File is not splittable so no parallelization "
                  + "is possible: " + file.getPath());
            }
          }
          //如果文件不可拆分：整个文件被作为一个单独的 FileSplit
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                      blkLocations[0].getCachedHosts()));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }
  //计算文件分片的大小
  protected long computeSplitSize(long blockSize, long minSize,
                                  long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }

  /**
   * Sets the given comma separated paths as the list of inputs 
   * for the map-reduce job.
   * 
   * @param job the job
   * @param commaSeparatedPaths Comma separated paths to be set as 
   *        the list of inputs for the map-reduce job.
   */
  // 设置输入路径列表，路径之间用逗号分隔
  public static void setInputPaths(Job job, 
                                   String commaSeparatedPaths
                                   ) throws IOException {
    setInputPaths(job, StringUtils.stringToPath(
                        getPathStrings(commaSeparatedPaths)));
  }

  /**
   * Add the given comma separated paths to the list of inputs for
   *  the map-reduce job.
   * 
   * @param job The job to modify
   * @param commaSeparatedPaths Comma separated paths to be added to
   *        the list of inputs for the map-reduce job.
   */
  // 将额外的输入路径列表添加到现有配置中
  public static void addInputPaths(Job job, 
                                   String commaSeparatedPaths
                                   ) throws IOException {
    for (String str : getPathStrings(commaSeparatedPaths)) {
      addInputPath(job, new Path(str));
    }
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   * 
   * @param job The job to modify 
   * @param inputPaths the {@link Path}s of the input directories/files 
   * for the map-reduce job.
   */
  // 使用 Path 数组设置输入路径，并将其转换为全限定路径存储在配置中
  public static void setInputPaths(Job job, 
                                   Path... inputPaths) throws IOException {
    Configuration conf = job.getConfiguration();
    Path path = inputPaths[0].getFileSystem(conf).makeQualified(inputPaths[0]);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = inputPaths[i].getFileSystem(conf).makeQualified(inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set(INPUT_DIR, str.toString());
  }

  /**
   * Add a {@link Path} to the list of inputs for the map-reduce job.
   * 
   * @param job The {@link Job} to modify
   * @param path {@link Path} to be added to the list of inputs for 
   *            the map-reduce job.
   */
  // 向现有输入路径列表添加单个 Path
  public static void addInputPath(Job job, 
                                  Path path) throws IOException {
    Configuration conf = job.getConfiguration();
    path = path.getFileSystem(conf).makeQualified(path);
    String dirStr = StringUtils.escapeString(path.toString());
    String dirs = conf.get(INPUT_DIR);
    conf.set(INPUT_DIR, dirs == null ? dirStr : dirs + "," + dirStr);
  }
  
  // This method escapes commas in the glob pattern of the given paths.
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
          }
          break;
        }
        default:
          continue; // nothing special to do for this character
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
    
    return pathStrings.toArray(new String[0]);
  }
  
  /**
   * Get the list of input {@link Path}s for the map-reduce job.
   * 
   * @param context The job
   * @return the list of input {@link Path}s for the map-reduce job.
   */
  // 从配置中读取并返回所有输入路径的 Path 数组。
  public static Path[] getInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(INPUT_DIR, "");
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

}
