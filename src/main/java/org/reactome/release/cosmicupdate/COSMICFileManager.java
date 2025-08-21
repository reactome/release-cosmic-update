package org.reactome.release.cosmicupdate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactome.release.common.dataretrieval.cosmic.COSMICFileRetriever;
import org.reactome.util.general.GUnzipCallable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class COSMICFileManager {
    private static final Logger logger = LogManager.getLogger();

    private final Config config;

    public COSMICFileManager(Config config) {
        this.config = config;
    }

    public void unzipFiles() throws InterruptedException {
        ExecutorService execService = Executors.newCachedThreadPool();
        // The files are large, and it could be slow to unzip them sequentially, so we will unzip them in parallel.
        execService.invokeAll(getGUnzipCallables());
        execService.shutdown();
    }

    /**
     * Clean up the uncompressed data files. Don't remove the zipped files, because if you need to run this code again,
     * you'll be stuck waiting for the files to download again. Try to avoid removing the zipped files until
     * you're *sure* this step has run successfully.
     */
    public void cleanupFiles() {
        List<String> filesToDelete = getConfig().getAllCOSMICLocalFilePaths();
        for (String file : filesToDelete) {
            deleteFile(file);
        }
    }

    /**
     * Download the data files from COSMIC if older than the specified duration
     */
    public void downloadFiles(Duration fileAge) {
        for (Config.FileConfig fileConfig : getConfig().getFileConfigs() ) {
            downloadFile(fileConfig, fileAge);
        }
    }

    private void downloadFile(Config.FileConfig config, Duration fileAge) {
        try {
            COSMICFileRetriever retriever = new COSMICFileRetriever();
            retriever.setDataURL(new URI(config.getUrl()));
            retriever.setFetchDestination(config.getDestination());
            retriever.setMaxAge(fileAge);
            retriever.setUserName(getConfig().getCosmicUsername());
            retriever.setPassword(getConfig().getCosmicPassword());

            logger.info("Downloading {} to {}", retriever.getDataURL(), config.getDestination());
            retriever.fetchData();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid URL for " + config.getDescription(), e);
        } catch (Exception e) {
            throw new RuntimeException("Error downloading " + config.getDescription(), e);
        }
    }

    /**
     * Deletes a file and logs a message about the deletion.
     * @param filePath Path of the file to delete
     */
    private void deleteFile(String filePath) {
        try {
            Files.deleteIfExists(Paths.get(filePath));
            logger.info("{} was deleted.", filePath);
        } catch (IOException e) {
            logger.warn("IOException caught while cleaning up the data file: \"" + filePath + "\". " +
                "You may need to manually remove any remaining files.", e);
        }
    }

    private List<GUnzipCallable> getGUnzipCallables() {
        return getConfig().getAllCOSMICLocalFilePaths().stream()
            .map(this::getGUnzipCallable)
            .collect(Collectors.toList());
    }

    private GUnzipCallable getGUnzipCallable(String filePathAsString) {
        return new GUnzipCallable(getGZippedFilePath(filePathAsString), getGUnzippedFilePath(filePathAsString));
    }

    private Path getGZippedFilePath(String filePathAsString) {
        return Paths.get(getGUnzippedFilePath(filePathAsString) + ".gz");
    }

    private Path getGUnzippedFilePath(String filePathAsString) {
        return Paths.get(filePathAsString);
    }

    private Config getConfig() {
        return this.config;
    }
}
