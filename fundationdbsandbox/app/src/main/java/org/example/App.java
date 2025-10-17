package org.example;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Logger;

public class App {
    private static final Logger logger = Logger.getLogger(App.class.getName());
    private static final String FDB_CLUSTER_DEFAULT_LOCATION = "/etc/foundationdb/fdb.cluster";

    public static void main(String[] args) {
        logger.info("Starting FoundationDB application...");

        final FDB fdb = FDB.selectAPIVersion(730);
        final String connectionString = System.getenv("FDB_CONNECTION_STRING");
        try (Database db = (connectionString != null && !connectionString.isBlank()) ? fdb.open(connectionString) : fdb.open(getClusterFile())) {

            logger.info("Performing simple write to FoundationDB");
            performSimpleWrite(db, "3230", "Metropola");

            logger.info("Performing simple read from FoundationDB");
            final byte[] value = performSimpleRead(db, "3230");
            logger.info("Read value: " + new String(value));


            logger.info("Trying out insert and read with subspace...");
            final Subspace userSubspace = new Subspace(Tuple.from("users"));

            // --- INSERT USER ---
            db.run(tr -> {
                final Tuple userData = Tuple.from("Stric Bobo", 3230, "Software Engineer");
                tr.set(userSubspace.pack(Tuple.from("bobo")), userData.pack());
                logger.info("Inserted user: bobo -> " + userData);
                return null;
            });

            // --- READ USER ---
            db.read(tr -> {
                final byte[] raw = tr.get(userSubspace.pack(Tuple.from("bobo"))).join();
                if (raw != null) {
                    final Tuple userData = Tuple.fromBytes(raw);
                    logger.info("Read user:");
                    logger.info("  Name: " + userData.getString(0));
                    logger.info("  Origin: " + userData.getLong(1));
                    logger.info("  Job: " + userData.getString(2));
                } else {
                    logger.warning("User not found");
                }
                return null;
            });


            logger.info("Trying out atomic counter with subspace... (the more we run the app, the higher the counter should be!");
            final Subspace counterSubspace = new Subspace(Tuple.from("atomicCounter"));
            db.run(tr -> {
                final byte[] key = counterSubspace.pack(Tuple.from("visits"));
                final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(1L);
                tr.mutate(MutationType.ADD, key, buffer.array());
                logger.info("Incremented 'visits' counter atomically");
                return null;
            });

            db.read(tr -> {
                final byte[] counterValue = tr.get(counterSubspace.pack(Tuple.from("visits"))).join();
                long count = 0L;
                if (counterValue != null && counterValue.length == 8) {
                    count = ByteBuffer.wrap(counterValue).order(ByteOrder.LITTLE_ENDIAN).getLong();
                }
                logger.info("Current 'visits' count: " + count);
                return null;
            });
        }

        logger.info("Application completed successfully");
    }

    public static String getClusterFile() {
        String clusterFile = System.getenv("FDB_CLUSTER_FILE");
        if (clusterFile == null || clusterFile.isBlank()) {
            clusterFile = FDB_CLUSTER_DEFAULT_LOCATION;
        }
        return clusterFile;
    }

    public static void performSimpleWrite(Database db, String key, String value) {
        db.run((Transaction tr) -> {
            tr.set(key.getBytes(), value.getBytes());
            return null;
        });
    }

    public static byte[] performSimpleRead(Database db, String key) {
        return db.run((Transaction tr) -> tr.get(key.getBytes()).join());
    }
}
