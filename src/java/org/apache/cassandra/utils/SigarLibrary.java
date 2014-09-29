/*
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
package org.apache.cassandra.utils;

import org.hyperic.sigar.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class SigarLibrary
{
    private Logger logger = LoggerFactory.getLogger(SigarLibrary.class);

    private Sigar sigar;
    private FileSystemMap mounts = null;
    private boolean sigarInitialized = Boolean.FALSE;
    private long INFINITY = -1;
    private long EXPECTED_MIN_NOFILE = 10000l; // number of files that can be opened
    /* We haven't found a way to get the MEMLOCK information from SIGAR
    private long EXPECTED_MEMLOCK = INFINITY;  // amount of memory that can be locked
    */
    private long EXPECTED_NPROC = 32768l; // number of processes
    private long EXPECTED_AS = INFINITY; // address space

    private enum Result
    {
        UNK, TRUE, FALSE
    }

    public SigarLibrary()
    {
    }

    // don't throw exception here, because this is not an error condition
    // we will just not use SIGAR if it is not found
    public boolean init()
    {
        logger.info("Initializing SIGAR library");
        try
        {
            sigar = new Sigar();
            mounts = sigar.getFileSystemMap();
            sigarInitialized = true;
        }
        catch (SigarException e)
        {
            logger.info("Could not initialize SIGAR library {} ", e.getMessage());
        }
        catch (UnsatisfiedLinkError linkError)
        {
            logger.info("Could not initialize SIGAR library {} ", linkError.getMessage());
        }
        return sigarInitialized;
    }

    /**
     *
     * @return true or false indicating if sigar was successfully initialized
     */
    public boolean isSigarInitialized() {
        return sigarInitialized;
    }

    /*
    public boolean isFileSystemTypeRemote(final String directory) throws SigarException
    {
        if (sigarInitialized)
        {
            FileSystem mntFileSystem = mounts.getMountPoint(directory);
            return mntFileSystem.getType() == FileSystem.TYPE_NETWORK;
        }
        else
        {
            logger.info("SIGAR not initialized");
            throw new SigarException("SIGAR not initialized");
        }
    }

    public boolean hasAcceptablediskLatency(String directory) throws SigarException
    {
        if (sigarInitialized)
        {
            FileSystem mntFileSystem = mounts.getMountPoint(directory);
            FileSystemUsage  fileSystemUsage= sigar.getFileSystemUsage(mntFileSystem.getDirName());
            double diskQueueLen = fileSystemUsage.getDiskQueue();
            logger.info("Disk Service time {} ", fileSystemUsage.getDiskServiceTime());
            logger.info("Disk Read bytes {} ", fileSystemUsage.getDiskReadBytes());
            logger.info("Disk Write bytes {} ", fileSystemUsage.getDiskWriteBytes());
            logger.info("Disk Writes {} ", fileSystemUsage.getDiskWrites());
            logger.info("Disk Reads {} ", fileSystemUsage.getDiskReads());
            logger.info("Disk queue size {} ", diskQueueLen );
            return false;
        }
        else
        {
            logger.info("SIGAR not initialized");
            throw new SigarException("SIGAR not initialized");
        }

    }
       */

    private Result hasAcceptableNProc()
    {
        try
        {
            long  fileMax= sigar.getResourceLimit().getProcessesMax();
            if (fileMax >= EXPECTED_NPROC || fileMax == INFINITY)
            {
                return Result.TRUE;
            }
            else
            {
                return Result.FALSE;
            }
        }
        catch (SigarException sigarException)
        {
            return Result.UNK;
        }

    }

    private Result hasAcceptableNoFile()
    {
        try
        {
            long  fileMax= sigar.getResourceLimit().getOpenFilesMax();
            if (fileMax >= EXPECTED_MIN_NOFILE || fileMax == INFINITY)
            {
                return Result.TRUE;
            }
            else
            {
                return Result.FALSE;
            }
        }
        catch (SigarException sigarException)
        {
            return Result.UNK;
        }

    }

    private Result hasAcceptableAs()
    {
        try
        {
            long  fileMax= sigar.getResourceLimit().getVirtualMemoryMax();
            if (fileMax == EXPECTED_AS)
            {
                return Result.TRUE;
            }
            else
            {
                return Result.FALSE;
            }
        }
        catch (SigarException sigarException)
        {
            return Result.UNK;
        }

    }

    private Result isSwapEnabled()
    {
        try
        {
            Swap swap =sigar.getSwap();
            long swapSize= swap.getTotal();
            if (swapSize>0l)
            {
                return Result.FALSE;
            }
            else
            {
                return Result.TRUE;
            }
        }
        catch (SigarException sigarException)
        {
            return Result.UNK;
        }
    }

    /**
     * Logs a warning if are certain that ulimit values are not
     * as suggested in the documentation or if swap is enabled.
     */
    public void warnIfRunningInDegradedMode(){
        if (sigarInitialized)
        {
            Result swapEnabled = isSwapEnabled();
            Result acceptableAs = hasAcceptableAs();
            Result acceptableNoFile = hasAcceptableNoFile();
            Result acceptableNProc = hasAcceptableNProc();
            if (
                   (   swapEnabled == Result.TRUE ||
                       acceptableAs == Result.FALSE ||
                       acceptableNoFile == Result.FALSE ||
                       acceptableNProc == Result.FALSE
                   )
              )
                {
                    logger.warn("Cassandra server running in degraded mode. Is swap disabled ? : {}  Address space adequate ? : {} " +
                        " nofile adequate ? : {} nproc adequate ? : {} ",swapEnabled, acceptableAs,
                        acceptableNoFile, acceptableNProc );
                }
        }
        else
        {
            logger.info("Sigar could not be initialized, test for checking degraded mode omitted.");
        }
    }
}
