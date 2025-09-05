package org.sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.gradle.api.Project;

import com.github.jk1.license.License;
import com.github.jk1.license.LicenseFileData;
import com.github.jk1.license.LicenseFileDetails;
import com.github.jk1.license.LicenseReportExtension;
import com.github.jk1.license.ManifestData;
import com.github.jk1.license.ModuleData;
import com.github.jk1.license.PomData;
import com.github.jk1.license.ProjectData;
import com.github.jk1.license.render.ReportRenderer;

public class CustomRenderer implements ReportRenderer {

    private static List<String> GITHUB_LICENSE_FILE_NAMES = List.of("LICENSE", "LICENSE.md", "LICENSE.txt", "NOTICE");
    private static final List<String> GITHUB_BRANCH_NAMES = List.of("main", "master");

    private static record LicenseData(
            String header,
            String content) {
    }

    private final String txtFileName;
    private final String csvFileName;

    private final Map<String, LicenseData> licenseCache = new HashMap<>();
    private final AtomicInteger dependencyCounter = new AtomicInteger();
    private final HttpClient client = HttpClient
            .newBuilder()
            .followRedirects(Redirect.NORMAL)
            .build();
    private final String customLicenseUrlsFile;

    private ProjectData projectData;
    private Properties customLicenseUrls;
    private String absoluteOutputDir;

    public CustomRenderer(String txtFileName, String csvFileName) {
        this(txtFileName, csvFileName, "custom-license-urls.properties");
    }

    public CustomRenderer(String txtFileName, String csvFileName, String customLicenseUrlsFile) {
        this.txtFileName = txtFileName;
        this.csvFileName = csvFileName;
        this.customLicenseUrlsFile = customLicenseUrlsFile;
    }

    public void render(ProjectData projectData) {
        try {
            doRendering(projectData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to render license report", e);
        }
    }

    /**
     * Performs the license rendering process for project dependencies.
     * 
     * @param projectData the project data containing dependency information to be
     *                    rendered
     * @throws IOException if an error occurs while reading the license names file
     *                     or writing to the output file
     */
    private void doRendering(ProjectData projectData) throws IOException {
        this.projectData = projectData;
        this.customLicenseUrls = new Properties();
        Project project = projectData.getProject();
        try (FileInputStream fis = new FileInputStream(new File(project.getParent().getProjectDir(),
                "config/license-config/" + customLicenseUrlsFile))) {
            this.customLicenseUrls.load(fis);
        }

        LicenseReportExtension licenseReport = project.getExtensions()
                .getByType(LicenseReportExtension.class);
        this.absoluteOutputDir = licenseReport.getAbsoluteOutputDir();

        // try (BufferedWriter stream = new BufferedWriter(new FileWriter(new
        // File(txtFileName)))) {
        // for (ModuleData moduleData : projectData.getAllDependencies()) {
        // printDependencyLicenses(stream, moduleData);
        // }
        // }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(csvFileName)))) {
            printCsvHeader(writer);
            for (ModuleData moduleData : projectData.getAllDependencies()) {
                printCsvDependencyLicenses(writer, moduleData);
            }
        }
    }

    private void printCsvHeader(BufferedWriter writer) throws IOException {
        println(writer,
                "Component Name;Artifact Name;Description;Version Currently Integrated;Link;Change Log;License Type;License Link");
    }

    private void printCsvDependencyLicenses(BufferedWriter writer, ModuleData moduleData) throws IOException {
        String libraryName = libraryName(moduleData);
        String artifactName = moduleData.getName();
        String description = description(moduleData);
        String version = moduleData.getVersion();
        String projectLink = projectUrl(moduleData);
        String changeLog = "-";
        String licenseType = moduleData.getPoms().stream()
                .flatMap(pom -> pom.getLicenses().stream()).findFirst()
                .map(License::getName)
                .orElse("N/A");
        String licenseLink = moduleData.getPoms().stream()
                .flatMap(pom -> pom.getLicenses().stream()).findFirst()
                .map(License::getUrl)
                .orElse("N/A");
        println(writer, "%s;%s;%s;%s;%s;%s;%s;%s",
                libraryName, artifactName, description, version, projectLink, changeLog, licenseType, licenseLink);
    }

    /**
     * Writes the information about a dependency module and its associated licenses
     * to the output stream.
     * 
     * This method formats and writes:
     * - A numbered header with a separator line
     * - The library name
     * - The library version
     * - The group and artifact name
     * - Associated license information for the dependency
     *
     * @param writer     the {@link BufferedWriter} to write the output to
     * @param moduleData the {@link ModuleData object containing dependency
     * information
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    private void printDependencyLicenses(BufferedWriter writer, ModuleData moduleData) throws IOException {
        // Print the Header of the dependency
        println(writer, "%d. ########################################################################",
                dependencyCounter.incrementAndGet());
        String libraryName = libraryName(moduleData);
        println(writer, "Library: %s", libraryName);
        println(writer, "Group: %s, Name: %s", moduleData.getGroup(), moduleData.getName());
        println(writer, "Version: %s", moduleData.getVersion());

        println(writer);

        // Print the associated licenses of the dependency
        printDependencyLicense(writer, libraryName, moduleData);
    }

    private void println(BufferedWriter writer, String msg, Object... args) throws IOException {
        writer.write(msg.formatted(args));
        writer.newLine();
    }

    private void println(BufferedWriter writer, String msg) throws IOException {
        writer.write(msg);
        writer.newLine();
    }

    private void println(BufferedWriter writer, char c, int times) throws IOException {
        writer.write(String.valueOf(c).repeat(Math.max(0, times)));
        writer.newLine();
    }

    private void println(BufferedWriter writer) throws IOException {
        writer.newLine();
    }

    private String libraryName(ModuleData moduleData) {
        return moduleData.getPoms().stream()
                .map(PomData::getName)
                .filter(n -> n != null && !n.isBlank())
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        "No project name found for module " + moduleData.getName() + " in POM files!"));
    }

    private String description(ModuleData moduleData) {
        return moduleData.getPoms().stream()
                .map(PomData::getDescription)
                .filter(n -> n != null && !n.isBlank())
                .findFirst()
                .orElseGet(() -> moduleData.getManifests().stream()
                        .map(ManifestData::getDescription)
                        .filter(n -> n != null && !n.isBlank())
                        .findFirst()
                        .orElse("N/A"));
    }

    /**
     * Retrieves and writes the license information for a dependency to the provided
     * output stream.
     * <P>
     * 
     * This method follows a sequential fallback strategy to locate license
     * information:
     * <ol>
     * <li>Searches for license files directly within the dependency package</li>
     * <li>Checks for manually configured license URLs in the configuration</li>
     * <li>Attempts to retrieve license from the project's GitHub repository</li>
     * <li>Falls back to license URLs declared in the dependency's POM files</li>
     * </ol>
     *
     * @param writer      the {@link BufferedWriter} to write the license
     *                    information to
     * @param libraryName the name of the library/dependency for which to find
     *                    license information
     * @param moduleData  the {@link ModuleData} object containing dependency
     *                    information
     * 
     * @throws IOException      if an error occurs while writing to the output
     *                          stream
     * @throws RuntimeException if no license information can be found through any
     *                          method
     */
    private void printDependencyLicense(BufferedWriter writer, String libraryName, ModuleData moduleData)
            throws IOException {
        /*
         * Step 1: First attempt - Check for license files in the package itself.
         * This is the most reliable source as the license is directly bundled
         * with the dependency.
         */
        if (printFromPackage(writer, moduleData)) {
            return;
        }

        /*
         * Step 2: Second attempt - Check for custom license repository URLs in
         * configuration.
         * This allows manual override for dependencies with non-standard license
         * locations.
         */
        String customLicenseUrl = customLicenseUrls.getProperty(libraryName);
        if (customLicenseUrl != null) {
            printFromCustomUrl(writer, customLicenseUrl);
            return;
        }

        /*
         * Step 3: Third attempt - Check GitHub repository if the project is hosted
         * there.
         * Many open source projects host their code and license files on GitHub,
         * so we try to fetch license files from standard locations in the repository.
         */
        if (printFromGitHub(writer, moduleData)) {
            return;
        }

        /*
         * Step 4: Final attempt - Use license URLs declared in POM files.
         * If all else fails, try to retrieve the license from URLs specified
         * in the dependency's metadata files. This is the least preferred method
         * as these URLs may sometimes be incorrect or unavailable.
         */
        if (printFromPom(writer, moduleData)) {
            return;
        }

        throw new RuntimeException("No license information available for " + libraryName + "!");
    }

    /**
     * Processes and prints license and notice files from a module package.
     * 
     * This method searches through the license files associated with the provided
     * module data and prints the content of any file whose name starts with
     * "LICENSE" or "NOTICE" (case-insensitive). The license content is printed to
     * the specified output stream with an appropriate header.
     * 
     * @param writer     the {@link BufferedWriter} to write the license information
     *                   to
     * @param moduleData the {@link ModuleData} object containing dependency
     *                   information
     * @return {@code true} if at least one license was printed, {@code false}
     *         otherwise
     * @throws RuntimeException if an error occurs while reading a license file
     */
    private boolean printFromPackage(BufferedWriter writer, ModuleData moduleData) {
        boolean licensePrinted = false;
        for (LicenseFileData licenseFileData : moduleData.getLicenseFiles()) {
            for (LicenseFileDetails fileDetail : licenseFileData.getFileDetails()) {
                Path path = Paths.get(this.absoluteOutputDir, fileDetail.getFile());

                String fileName = path.getFileName().toString();
                String upperFileName = fileName.toUpperCase();

                // Only process license and notice files
                if (upperFileName.startsWith("LICENSE") || upperFileName.startsWith("NOTICE")) {
                    try (var lines = Files.lines(path)) {
                        String content = lines.collect(Collectors.joining("\n"));
                        String header = "from the " + fileName + " file included in the distribution";
                        printLicense(writer, new LicenseData(header, content));
                        licensePrinted = true;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to read embedded license file: " + path, e);
                    }
                }
            }
        }
        return licensePrinted;
    }

    /**
     * Writes the formatted license information to the specified writer.
     * The method formats the license by:
     * <ul>
     * <li>Adding a header line with "**" prefix</li>
     * <li>Adding a separator line of dashes</li>
     * <li>Writing the license content</li>
     * <li>Adding two blank lines after the license</li>
     * </ul>
     *
     * @param writer  the {@link BufferedWriter} to write the license information to
     * @param license the {@link LicenseData} object containing the license header
     *                and content
     * @throws IOException if an I/O error occurs during writing
     */
    private void printLicense(BufferedWriter writer, LicenseData license) throws IOException {
        String header = license.header();
        println(writer, "** %s", header);
        println(writer, '-', header.length() + 3);
        println(writer, license.content());
        println(writer);
        println(writer);
    }

    /**
     * Attempts to retrieve and print a license from a custom URL.
     * 
     * @param writer     the {@link BufferedWriter} to write the license information
     *                   to
     * @param licenseUrl the URL from which to retrieve the license
     * @return {@code true} if license was successfully retrieved and printed,
     *         {@code false} otherwise
     * @throws IOException if an error occurs during reading from the URL or writing
     *                     to the output
     */
    private boolean printFromCustomUrl(BufferedWriter writer, String licenseUrl) throws IOException {
        Optional<LicenseData> license = retrieveFromUrl(licenseUrl, licenseUrl);
        if (!license.isEmpty()) {
            printLicense(writer, license.get());
            return true;
        }
        return false;
    }

    /**
     * Attempts to retrieve license files from a GitHub repository by checking
     * multiple branches and common license file names.
     * <p>
     *
     * This method iterates over a list of common branch names (e.g., "main",
     * "master") and a list of typical license file names (e.g., "LICENSE",
     * "NOTICE"), attempting to fetch the license file from each combination. If
     * a license file is found and successfully retrieved, it is printed and the
     * method returns {@code true}. If no license file is found after all attempts,
     * the method returns {@code false}.
     *
     * @param stream     the {@link BufferedWriter} to write license information to
     * @param moduleData the {@link ModuleData} object containing dependency
     *                   information
     * @return {@code true} if a license file was found and printed; {@code false}
     *         otherwise
     */
    private boolean printFromGitHub(BufferedWriter stream, ModuleData moduleData) throws IOException {
        String projectUrl = projectUrl(moduleData);
        if (isFromGitHub(projectUrl)) {
            boolean licensePrinted = false;

            // Try each common branch name
            for (String branch : GITHUB_BRANCH_NAMES) {
                // Try each common license file name
                for (String licenseFileName : GITHUB_LICENSE_FILE_NAMES) {
                    String githubBlobUrl = String.format("%s/blob/%s/%s", projectUrl, branch, licenseFileName);
                    String rawLicenseUrlTry = String.format("%s?raw=true", githubBlobUrl);
                    Optional<LicenseData> license = retrieveFromUrl(rawLicenseUrlTry, githubBlobUrl);
                    if (license.isPresent()) {
                        printLicense(stream, license.get());
                        licensePrinted = true;
                    }
                }
                // If we found at least one license, we stop looking further branches.
                if (licensePrinted) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Extracts the project URL from the provided module data.
     *
     * @param moduleData the {@link ModuleData} object containing dependency
     *                   information
     * @return the first non-blank project URL found, or "N/A" if none is found
     */
    private String projectUrl(ModuleData moduleData) {
        return moduleData.getPoms().stream()
                .map(PomData::getProjectUrl)
                .filter(u -> u != null && !u.isBlank())
                .findFirst()
                .orElseGet(() -> moduleData.getManifests().stream()
                        .map(ManifestData::getUrl)
                        .filter(u -> u != null && !u.isBlank())
                        .findFirst()
                        .orElse("N/A"));
    }

    /**
     * Writes license information extracted from POM files to the specified writer.
     * <p>
     * 
     * This method processes all licenses found in the POM files associated with the
     * given module. For each license, it retrieves the license data from the URL
     * specified in the license metadata. If the license URL has been processed
     * before, the cached license data is used instead of making a new request.
     *
     * @param writer     the {@link BufferedWriter} to write the license information
     *                   to
     * @param moduleData the {@link ModuleData} object containing dependency
     *                   information
     * @return {@code true} if at least one license was printed, {@code false}
     *         otherwise
     * @throws IOException      if an I/O error occurs while writing to the writer
     * @throws RuntimeException if a license URL in the POM metadata is invalid
     */
    private boolean printFromPom(BufferedWriter writer, ModuleData moduleData) throws IOException {
        List<License> licenses = moduleData.getPoms().stream()
                .flatMap(pom -> pom.getLicenses().stream()).toList();

        boolean printed = false;
        for (License license : licenses) {
            LicenseData licenseData = licenseCache.computeIfAbsent(license.getUrl(), url -> {
                System.out.println("Feeding cache for " + url);
                // We assume that the URL in the POM metadata points to a valid license file.
                // If it is not the case, we interrupt the process throwing an exception.
                return retrieveFromUrl(url, url)
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Found invalid license URL in the POM metadata of the module "
                                                + moduleData.getName() + ": " + url));
            });
            printLicense(writer, licenseData);
            printed = true;
        }

        return printed;
    }

    /**
     * Attempts to retrieve license data from the specified URL.
     * 
     * @param licenseUrl the URL from which to retrieve the license content.
     * @param formalUrl  the formal URL to be included in the license data
     *                   description.
     * @return an {@link Optional} containing {@link LicenseData} if the license was
     *         successfully retrieved, or an empty {@link Optional} if the request
     *         was unsuccessful (e.g., non-200 response code).
     * @throws RuntimeException if there is an error during the HTTP request.
     */
    private Optional<LicenseData> retrieveFromUrl(String licenseUrl, String formalUrl) {
        try {
            HttpResponse<String> response = client.send(HttpRequest
                    .newBuilder(URI.create(licenseUrl))
                    .build(), BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return Optional.of(
                        new LicenseData("from the license contents available at " + formalUrl, response.body()));
            }
        } catch (IllegalArgumentException | IOException | InterruptedException e) {
            throw new RuntimeException("Error while trying to retrieve license from " + licenseUrl, e);
        }
        return Optional.empty();
    }

    /**
     * Determines if a URL points to a GitHub repository.
     * 
     * @param projectUrl the URL to check
     * @return {@code true} if the URL is from GitHub (starts with http://github.com
     *         or https://github.com), {@code false} otherwise
     */
    private static boolean isFromGitHub(String projectUrl) {
        return projectUrl.startsWith("https://github.com") || projectUrl.startsWith("http://github.com");
    }

}
