package org.reactome.release.cosmicupdate;

import org.gk.persistence.MySQLAdaptor;
import org.reactome.util.general.DBUtils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Config {
    private final Properties configProps;
    private MySQLAdaptor dba;

    public Config(String configFilePath) {
        validateConfigFilePath(configFilePath);
        this.configProps = loadConfigProperties(configFilePath);
    }

    public String getCosmicUsername() {
        return throwIfNullOrBlank("cosmic.user", getConfigProperties().getProperty("cosmic.user"));
    }

    public String getCosmicPassword() {
        return throwIfNullOrBlank("cosmic.password", getConfigProperties().getProperty("cosmic.password"));
    }

    public long getPersonId() {
        return Long.parseLong(getConfigProperties().getProperty("personId"));
    }

    public boolean isTestMode() {
        return Boolean.parseBoolean(getConfigProperties().getProperty("testMode", "false"));
    }

    public MySQLAdaptor getDBA() throws SQLException {
        if (this.dba == null) {
            this.dba = DBUtils.getCuratorDbAdaptor(getConfigProperties());
        }

        return this.dba;
    }

    public List<FileConfig> getFileConfigs() {
        return Arrays.asList(
            new FileConfig(
                getMutantExportRemoteFilePath(),
                addGzipExtension(getMutantExportLocalFilePath()),
                "Mutant Export"
            ),
            new FileConfig(
                getMutationTrackingRemoteFilePath(),
                addGzipExtension(getMutationTrackingLocalFilePath()),
                "Mutation Tracking"
            ),
            new FileConfig(
                getFusionExportRemoteFilePath(),
                addGzipExtension(getFusionExportLocalFilePath()),
                "Fusion Export"
            )
        );
    }

    public List<String> getAllCOSMICLocalFilePaths() {
        return Arrays.asList(
            getMutantExportLocalFilePath(),
            getFusionExportLocalFilePath(),
            getMutationTrackingLocalFilePath()
        );
    }

    public String getMutantExportLocalFilePath() {
        return getConfigProperties().getProperty("pathToMutantExportFile", "./CosmicMutantExport.tsv");

    }

    public String getFusionExportLocalFilePath() {
        return getConfigProperties().getProperty("pathToFusionExportFile", "./CosmicFusionExport.tsv");
    }

    public String getMutationTrackingLocalFilePath() {
        return getConfigProperties().getProperty("pathToMutationTrackingFile", "./CosmicMutationTracking.tsv");
    }

    private String getMutantExportRemoteFilePath() {
        return throwIfNullOrBlank(
            "urlToMutantExportFile", getConfigProperties().getProperty("urlToMutantExportFile")
        );
    }

    private String getFusionExportRemoteFilePath() {
        return throwIfNullOrBlank(
            "urlToFusionExportFile", getConfigProperties().getProperty("urlToFusionExportFile")
        );
    }

    private String getMutationTrackingRemoteFilePath() {
        return throwIfNullOrBlank(
            "urlToMutationTrackingFile", getConfigProperties().getProperty("urlToMutationTrackingFile")
        );
    }

    private String throwIfNullOrBlank(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Config value " + name + " cannot be null or blank!");
        }
        return value;
    }

    private void validateConfigFilePath(String configFilePath) {
        if (configFilePath == null || configFilePath.isBlank()) {
            throw new IllegalArgumentException("Config file path cannot be null or blank!");
        }
    }

    private Properties loadConfigProperties(String configFilePath) {
        Properties configProps = new Properties();

        try(Reader configReader = new FileReader(configFilePath)) {
            configProps.load(configReader);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load config file", e);
        }
        return configProps;
    }

    private Properties getConfigProperties() {
        return this.configProps;
    }

    private String addGzipExtension(String filePath) {
        return filePath + ".gz";
    }

    public static class FileConfig {
        private final String url;
        private final String destination;
        private final String description;

        public FileConfig(String url, String destination, String description) {
            this.url = url;
            this.destination = destination;
            this.description = description;
        }

        public String getUrl() { return url; }
        public String getDestination() { return destination; }
        public String getDescription() { return description; }
    }
}
