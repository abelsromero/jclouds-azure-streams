package org.abelsromero.jclouds;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.PutOptions;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

public class App {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("YYYYMMdd-hhmmssMMM");

    public static void main(String[] args) throws IOException {
        new App().doStuff();
    }

    private void doStuff() throws IOException {

        final BlobStorageConfig config = buildConfiguration();

        final BlobStoreContext context = new BlobConnector(config.getAccountName(), config.getAccountKey())
            .openBlobStoreContext((long) (1 * 1024 * 1024));

        final String containerName = config.getContainerName();

        testFile(context, containerName, "myfile_100MB.bin");
        testFile(context, containerName, "sample.pdf");
        testFile(context, containerName, "myfile_32MB.bin");
        testFile(context, containerName, "myfile_32_5MB.bin");
    }

    private void testFile(BlobStoreContext context, String containerName, String filename) throws IOException {

        final File file = new File(this.getClass().getClassLoader().getResource(filename).getFile());
        final long fileSize = file.length();
        final InputStream is = this.getClass().getClassLoader().getResourceAsStream(filename);

        putContent(context, containerName, file);
        putContent(context, containerName, is, filename, fileSize);
        putContent(context, containerName, new FileInputStream(file), filename, fileSize);

        byte[] buffer = new byte[(int) fileSize];
        IOUtils.readFully(new FileInputStream(file), buffer);
        putContent(context, containerName, new ByteArrayInputStream(buffer), filename, fileSize);

        System.out.println("----------------------------------------------");
    }

    private void putContent(BlobStoreContext context, String containerName, File payload) {

        final BlobStore blobStore = context.getBlobStore();

        final String name = payload.getName() + "-" + DATE_FORMAT.format(new Date()) + "-" + UUID.randomUUID();
        final Blob blob = blobStore
            .blobBuilder(name)
            .payload(payload)
            .contentLength(payload.length())
            .contentType("application/octet-stream")
            .build();

        System.out.println("Uploading " + name);
        blobStore.putBlob(containerName, blob, PutOptions.Builder.multipart());
    }

    private void putContent(BlobStoreContext context, String containerName, InputStream payload, String filename, long contentLength) {
        final BlobStore blobStore = context.getBlobStore();

        final String name = filename + "-" + DATE_FORMAT.format(new Date()) + "-" + UUID.randomUUID();
        final Blob blob = blobStore
            .blobBuilder(name)
            .payload(payload)
            .contentLength(contentLength)
            .contentType("application/octet-stream")
            .build();

        System.out.println("Uploading " + name);
        try {
            blobStore.putBlob(containerName, blob, PutOptions.Builder.multipart(false));
        } catch (Exception e) {
            System.err.println("Failed uploading file of size " + (contentLength / (1024 * 1024)) + "MB");
        }
    }

    private BlobStorageConfig buildConfiguration() throws IOException {
        final Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));

        return new BlobStorageConfig(
            properties.getProperty("storage.account.name"),
            properties.getProperty("storage.account.key"),
            properties.getProperty("storage.container.name")
        );
    }

    private void timed(Consumer<Void> block) {
        long init = System.currentTimeMillis();
        block.accept(null);
        System.out.println("Time: " + (System.currentTimeMillis() - init));
    }

}
