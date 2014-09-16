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
    private long EXPECTED_MIN_FILE_COUNT = 10000;

    public SigarLibrary()
    {
        //System.setProperty("org.hyperic.sigar.path", "-");
    }

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

    public boolean hasAcceptableMaxFiles() throws SigarException
    {
        if (sigarInitialized)
        {
            long  fileMax= sigar.getResourceLimit().getOpenFilesMax();
            if (fileMax >= EXPECTED_MIN_FILE_COUNT)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            logger.info("SIGAR not initialized");
            throw new SigarException("SIGAR not initialized");
        }
    }

    public boolean isSwapEnabled() throws SigarException
    {
        if (sigarInitialized)
        {
            Swap swap =sigar.getSwap();
            long swapSize= swap.getTotal();
            if(swapSize>0l)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        else
        {
            logger.info("SIGAR not initialized");
            throw new SigarException("SIGAR not initialized");
        }
    }
}
