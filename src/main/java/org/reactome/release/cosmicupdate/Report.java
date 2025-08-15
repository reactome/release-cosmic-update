package org.reactome.release.cosmicupdate;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class Report {
    private static final String REPORTS_DIRECTORY_PATH = "reports";
    
    private final String dateSuffix;
    
    private CSVPrinter nonEWASPrinter;
    private CSVPrinter identifiersWithNoReferrerPrinter;
    private CSVPrinter identifierUpdatePrinter;

    public Report() {
        this.dateSuffix = setDateSuffix();
        initReports();
    }

    public void printIdentifierWithNoReferrerRecord(String identifier) {
        try {
            identifiersWithNoReferrerPrinter.printRecord(identifier);
            identifiersWithNoReferrerPrinter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to print identifier with no referrer record", e);
        }
    }

    public void printNonEWASRecord(String identifier, String instanceDisplayNameAndDbId) {
        try {
            nonEWASPrinter.printRecord(identifier, instanceDisplayNameAndDbId);
            nonEWASPrinter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to print non-EWAS record", e);
        }
    }

    public void printIdentifierUpdateRecord(Object[] identifierUpdateReportLineValues) {
        try {
            identifierUpdatePrinter.printRecord(identifierUpdateReportLineValues);
            identifierUpdatePrinter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to print identifier update record", e);
        }
    }

    private void initReports() {
        try {
            createReportDirectoryIfNotExists();
            
            nonEWASPrinter = new CSVPrinter(
                new FileWriter(getNonEWASReportFileName()),
                CSVFormat.DEFAULT.withHeader("COSMIC identifier", "non-EWAS entity")
            );

            identifiersWithNoReferrerPrinter = new CSVPrinter(
                new FileWriter(getIdentifiersWithNoReferrerReportFileName()),
                CSVFormat.DEFAULT.withHeader("COSMIC identifier")
            );
            identifierUpdatePrinter = new CSVPrinter(
                new FileWriter(getIdentifierUpdateReportFileName()),
                CSVFormat.DEFAULT.withHeader(getIdentifierUpdateReportHeader())
            );
        } catch (IOException e) {
            throw new RuntimeException("Unable to initialize reports", e);
        }
    }

    private void createReportDirectoryIfNotExists() throws IOException {
        Files.createDirectories(Paths.get(REPORTS_DIRECTORY_PATH));
    }

    private String getNonEWASReportFileName() {
        return REPORTS_DIRECTORY_PATH + "/nonEWASObjectsWithCOSMICIdentifiers_" + getDateSuffix() + ".csv";
    }
    
    private String getIdentifiersWithNoReferrerReportFileName() {
        return REPORTS_DIRECTORY_PATH + "/COSMICIdentifiersNoReferrers_" + getDateSuffix() + ".csv";
    }

    private String getIdentifierUpdateReportFileName() {
        return REPORTS_DIRECTORY_PATH + "/COSMIC-identifiers-report_" + getDateSuffix() + ".csv";
    }

    private static String[] getIdentifierUpdateReportHeader() {
        return Arrays.asList(
                "DB_ID",
                "Identifier",
                "Suggested Prefix",
                "Valid (according to COSMIC files)?",
                "COSV identifier",
                "Mutation IDs",
                "COSMIC Search URL"
        ).toArray(new String[0]);
    }

    private String setDateSuffix() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_kkmmss");
        return formatter.format(LocalDateTime.now());
    }

    private String getDateSuffix() {
        return this.dateSuffix;
    }
}
