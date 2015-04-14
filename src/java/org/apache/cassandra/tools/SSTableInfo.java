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
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.hexToBytes;

/**
 * Collects sstable info
 */
public class SSTableInfo
{  
    private static final Options options = new Options();
    private static CommandLine cmd;

    /**
     * Checks if PrintStream error and throw exception
     *
     * @param out The PrintStream to be check
     */
    private static void checkStream(PrintStream out) throws IOException
    {
        if (out.checkError())
            throw new IOException("Error writing output stream");
    }   

    // This is necessary to accommodate the test suite since you cannot open a Reader more
    // than once from within the same process.
    static void export(SSTableReader reader, PrintStream outs) throws IOException
    {
        SSTableIdentityIterator row;
        SSTableScanner scanner = reader.getScanner();
        try
        {
            int i = 0;
            int deletedRows = 0;
            int deletedColumns = 0;
            int rangeTombstones = 0;
            
            while (scanner.hasNext())
            {
                row = (SSTableIdentityIterator) scanner.next();
                if (row.getColumnFamily().deletionInfo().isLive())
                {
                	while (row.hasNext())
                    {                                               
                        OnDiskAtom atom = row.next();
                        if (atom instanceof Column)
                        {
                            if (atom instanceof DeletedColumn)
                            {
                            	deletedColumns++;
                            }                            
                        }
                        else if(atom instanceof RangeTombstone)
                        {
                        	rangeTombstones++;
                        }
                    }
                }
                else
                {
                	deletedRows++;
                }
                checkStream(outs);

                i++;
            }
            outs.println(String.format("Row tombstones: %d", deletedRows));
            outs.println(String.format("Column tombstones: %d", deletedColumns));
            outs.println(String.format("Range tombstones: %d", rangeTombstones));
            outs.flush();
        }
        finally
        {
            scanner.close();
        }
    }

    /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param desc     the descriptor of the sstable to read from
     * @param outs     PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @throws IOException on failure to read/write input/output
     */
    public static void export(Descriptor desc, PrintStream outs) throws IOException
    {
        export(SSTableReader.open(desc), outs);
    }

    /**
     * Given arguments specifying an SSTable, writes some metrics to output
     *
     * @param args command lines arguments
     * @throws IOException            on failure to open/read/write files or output streams
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        String usage = String.format("Usage: %s <sstable> %n", SSTableInfo.class.getName());

        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }


        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }

        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        DatabaseDescriptor.loadSchemas(false);
        Descriptor descriptor = Descriptor.fromFilename(ssTableFileName);

        // Start by validating keyspace name
        if (Schema.instance.getKSMetaData(descriptor.ksname) == null)
        {
            System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!",
                                             ssTableFileName, descriptor.ksname));
            System.exit(1);
        }
        Keyspace keyspace = Keyspace.open(descriptor.ksname);

        // Make it work for indexes too - find parent cf if necessary
        String baseName = descriptor.cfname;
        if (descriptor.cfname.contains("."))
        {
            String[] parts = descriptor.cfname.split("\\.", 2);
            baseName = parts[0];
        }

        // IllegalArgumentException will be thrown here if ks/cf pair does not exist
        try
        {
            keyspace.getColumnFamilyStore(baseName);
        }
        catch (IllegalArgumentException e)
        {
            System.err.println(String.format("The provided column family is not part of this cassandra keyspace: keyspace = %s, column family = %s",
                                             descriptor.ksname, descriptor.cfname));
            System.exit(1);
        }

        try
        {
            export(descriptor, System.out);
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }
}
