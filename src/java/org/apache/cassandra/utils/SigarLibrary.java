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

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;


public class SigarLibrary
{
    private Logger logger = LoggerFactory.getLogger(SigarLibrary.class);

    private Sigar sigar;
    private FileSystemMap mounts = null;
    private boolean sigarInitialized = Boolean.FALSE;

    public SigarLibrary()
    {
        System.setProperty("org.hyperic.sigar.path", "-");
    }

    public boolean init()
    {
        logger.info("Initializing SIGAR library");
        try
           {
                System.loadLibrary("sigar");
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
        if (sigar != null)
        {
            logger.info("SIGAR library loaded from: " + sigar.getNativeLibrary().getAbsolutePath());
        }
        return sigarInitialized;
    }

    public boolean isFileSystemTypeRemote(final String directory)
    {
        if (sigarInitialized)
        {
            FileSystem mntFileSystem = mounts.getMountPoint(directory);
            return mntFileSystem.getType() == FileSystem.TYPE_NETWORK;
        }
        else
        {
            logger.info("SIGAR not initialized");
            return false;
        }
    }
}
