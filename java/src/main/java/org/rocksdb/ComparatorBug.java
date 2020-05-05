package org.rocksdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ComparatorBug {

    static {
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) {
        try {
            for (int i = 0; i < 100; i++) {
                System.out.println("Attempt: " + i);
                comparatorBug();
            }
        } catch (RocksDBException | IOException e) {
            e.printStackTrace();
        }
    }

    private static void comparatorBug() throws RocksDBException, IOException {
        try (/*ComparatorOptions comparatorOpts = new ComparatorOptions();*/
             VersionedComparator comparator = new VersionedComparator(new ComparatorOptions());
             ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
                 .setComparator(comparator)) {

            byte[] columnFamilyName = RocksDB.DEFAULT_COLUMN_FAMILY;
            ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(columnFamilyName, columnFamilyOptions);

            List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(columnFamilyDescriptor);

            final Path path = Paths.get("/tmp/test");
//            if (Files.exists(path)) {
//                deleteDir(path);
//            }

            try (DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
                 RocksDB rocksdb = RocksDB.open(dbOptions, path.toString(), columnFamilyDescriptors, columnFamilyHandles)) {

                try {
                    ColumnFamilyHandle cf1 = columnFamilyHandles.get(0);

                    rocksdb.put(cf1, ("justanotherrandomkey").getBytes(), "value".getBytes());

                    System.out.println(new String(rocksdb.get(cf1, ("justanotherrandomkey").getBytes())));

                } finally {
                    for (ColumnFamilyHandle cf : columnFamilyHandles) {
                        cf.close();
                    }
                }
            }
        }
    }


    public static class VersionedComparator extends AbstractComparator {
        private final int versionSize = 8;

        protected VersionedComparator(ComparatorOptions copt) {
            super(copt);
        }

        @Override
        public String name() {
            return "test";
        }

        @Override
        public int compare(ByteBuffer a, ByteBuffer b) {
            return compareToWithOffsetAndLength(a, a.remaining() - versionSize, versionSize, b, b.remaining() - versionSize, versionSize);
        }

        private int compareToWithOffsetAndLength(ByteBuffer a, int aOffset, int aLength, ByteBuffer b, int bOffset, int bLength) {
            int minLength = Math.min(aLength, bLength);

            for (int i = 0; i < minLength; i++) {
                int aByte = a.get(i + aOffset) & 0xFF;
                int bByte = b.get(i + bOffset) & 0xFF;
                if (aByte != bByte) {
                    return aByte - bByte;
                }
            }
            return aLength - bLength;
        }
    }


    private static void deleteDir(final Path path) throws IOException {
        Files.walkFileTree(path, new DeleteDirVisitor());
    }

    private static class DeleteDirVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            if (exc != null) {
                throw exc;
            }

            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
        }
    }
}
