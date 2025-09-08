package org.sample;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
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

public class KafkaConnectorLicenseRenderer implements ReportRenderer {

    private interface InternalRenderer {

        /**
         * Renders the license information to the provided writer.
         *
         * @param writer the {@link BufferedWriter} to write the license
         *               information to
         * @throws IOException if an I/O error occurs while writing to the output
         */
        default void render(BufferedWriter writer) throws IOException {
            printHeader(writer);
            for (ModuleData moduleData : getProjectData().getAllDependencies()) {
                printModuleHeader(writer, moduleData);
                printDependencyLicenses(writer, moduleData);
            }
        }

        /**
         * Retrieves the project data associated with this license renderer.
         * 
         * @return the {@link ProjectData} object containing project information
         */
        ProjectData getProjectData();

        /**
         * Prints the header section of the license to the specified writer.
         * This default implementation does nothing and can be overridden by
         * implementing classes to provide specific header formatting.
         *
         * @param writer the {@link BufferedWriter} to write the license
         *               information to
         * @throws IOException if an I/O error occurs while writing to the output
         */
        default void printHeader(BufferedWriter writer) throws IOException {

        }

        /**
         * Prints the header information for a module to the specified writer.
         * This default implementation does nothing and can be overridden by
         * implementing classes to provide specific header formatting.
         *
         * @param writer     the {@link BufferedWriter} to write the license
         *                   information to
         * @param moduleData the {@link ModuleData} object containing dependency
         *                   information
         * @throws IOException if an I/O error occurs while writing to the output
         */
        default void printModuleHeader(BufferedWriter writer, ModuleData moduleData) throws IOException {

        }

        /**
         * Writes license information for all dependencies of a module to the specified
         * writer.
         * 
         * This method formats and outputs the license details for each dependency in
         * the specified module data.
         * 
         * @param writer     the {@link BufferedWriter} to write the license
         *                   information to
         * @param moduleData the {@link ModuleData} object containing dependency
         *                   information
         * @throws IOException If an I/O error occurs while writing to the output
         */
        void printDependencyLicenses(BufferedWriter writer, ModuleData moduleData) throws IOException;

        default void println(BufferedWriter writer, String msg, Object... args) throws IOException {
            writer.write(msg.formatted(args));
            writer.newLine();
        }

        default void println(BufferedWriter writer, String msg) throws IOException {
            writer.write(msg);
            writer.newLine();
        }

        default void println(BufferedWriter writer, char c, int times) throws IOException {
            writer.write(String.valueOf(c).repeat(Math.max(0, times)));
            writer.newLine();
        }

        default void println(BufferedWriter writer) throws IOException {
            writer.newLine();
        }

        /**
         * Extracts the project URL from the provided module data.
         *
         * @param moduleData the {@link ModuleData} object containing dependency
         *                   information
         * @return the first non-blank project URL found, or "N/A" if none is found
         */
        default String projectUrl(ModuleData moduleData) {
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
         * Extracts the library name from the provided module data.
         *
         * @param moduleData the {@link ModuleData} object containing POM information
         * @return the first non-null and non-blank name found in the POM data
         * @throws RuntimeException If no valid name can be found in any of the POM
         *                          files
         */
        default String libraryName(ModuleData moduleData) {
            return moduleData.getPoms().stream()
                    .map(PomData::getName)
                    .filter(n -> n != null && !n.isBlank())
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "No project name found for module " + moduleData.getName() + " in POM files!"));
        }

        /**
         * Extracts a formatted description from the provided module data.
         * 
         * @param moduleData the {@link ModuleData} object containing POM information
         * @return a formatted description string, truncated and cleaned of newlines and
         *         extra spaces
         */
        default String description(ModuleData moduleData) {
            String fullDescription = moduleData.getPoms().stream()
                    .map(PomData::getDescription)
                    .filter(n -> n != null && !n.isBlank())
                    .findFirst()
                    .orElseGet(() -> moduleData.getManifests().stream()
                            .map(ManifestData::getDescription)
                            .filter(n -> n != null && !n.isBlank())
                            .findFirst()
                            .orElse("N/A"));
            String truncatedDescription = fullDescription.length() <= 100
                    ? fullDescription
                    : fullDescription.substring(0, 100) + "...";
            return truncatedDescription.replaceAll("\\r?\\n", "").replaceAll("\\s+", " ");
        }

    }

    /**
     * The filename containing custom license URLs mappings.
     */
    private final String customLicenseUrlsFileName;

    /**
     * The name of the output text file containing license information to be
     * rendered.
     */
    private final String txtFileName;

    /**
     * The name of the output CSV file containing license information.
     */
    private final String csvFileName;

    public KafkaConnectorLicenseRenderer(String txtFileName, String csvFileName) {
        this(txtFileName, csvFileName, "custom-license-urls.properties");
    }

    public KafkaConnectorLicenseRenderer(String txtFileName, String csvFileName, String customLicenseUrlsFileName) {
        this.txtFileName = txtFileName;
        this.csvFileName = csvFileName;
        this.customLicenseUrlsFileName = customLicenseUrlsFileName;
    }

    @Override
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
        Project project = projectData.getProject();
        LicenseReportExtension licenseReport = project.getExtensions()
                .getByType(LicenseReportExtension.class);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(txtFileName)))) {
            File customLicenseUrlsFile = new File(project.getParent().getProjectDir(),
                    "config/license-config/" + customLicenseUrlsFileName);
            new TxtRenderer(licenseReport.getAbsoluteOutputDir(), customLicenseUrlsFile, projectData).render(writer);
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(csvFileName)))) {
            new CsvRenderer(projectData).render(writer);
        }
    }

    /**
     * A text-based renderer that processes and outputs license information for
     * project dependencies.
     * <p>
     * This renderer implements the {@link InternalRenderer} interface to generate a
     * text representation
     * of all licenses associated with project dependencies. For each dependency, it
     * presents:
     * <ul>
     * <li>Basic dependency information (name, group, artifact, version)</li>
     * <li>The full text of associated license files</li>
     * </ul>
     */
    static class TxtRenderer implements InternalRenderer {

        private static List<String> GITHUB_LICENSE_FILE_NAMES = List.of("LICENSE", "LICENSE.md", "LICENSE.txt",
                "NOTICE");
        private static final List<String> GITHUB_BRANCH_NAMES = List.of("main", "master");

        private static record LicenseData(
                String header,
                String content) {
        }

        /**
         * The directory path where embedded licenses are stored.
         * This path is used by the license renderer to locate and process
         * license files that need to be embedded in the Kafka connector.
         */
        private final String embeddedLicensesDir;

        /**
         * Stores mappings between dependency identifiers and their custom license URLs.
         * This Properties object is used when standard license detection needs to be
         * overridden with specific URLs for certain dependencies.
         */
        private final Properties customLicenseUrls = new Properties();

        /**
         * Counter used to track the number of dependencies processed.
         */
        private final AtomicInteger dependencyCounter = new AtomicInteger();

        /**
         * Cache for storing license data to avoid repeated HTTP requests for the same
         * license URL.
         * Maps license URLs to their corresponding {@link LicenseData} objects.
         * This cache improves performance by preventing redundant network requests and
         * license parsing.
         */
        private final Map<String, LicenseData> licenseCache = new HashMap<>();

        /**
         * HTTP client used for retrieving license content from URLs.
         * This client is configured to automatically follow redirects
         * and is used to fetch license data that gets stored in the licenseCache
         * to avoid redundant network requests for the same license URL.
         */
        private final HttpClient client = HttpClient
                .newBuilder()
                .followRedirects(Redirect.NORMAL)
                .build();

        /**
         * The project data containing information needed for license rendering.
         */
        private final ProjectData projectData;

        TxtRenderer(String embeddedLicensesDir, File customLicenseUrlsFile, ProjectData projectData)
                throws IOException {
            this.embeddedLicensesDir = embeddedLicensesDir;
            this.projectData = projectData;
            try (FileInputStream fis = new FileInputStream(customLicenseUrlsFile)) {
                this.customLicenseUrls.load(fis);
            }
        }

        @Override
        public ProjectData getProjectData() {
            return projectData;
        }

        /**
         * Prints a header line to the license document.
         * This method creates a separator line with '#' characters and adds a section
         * number by incrementing the dependency counter.
         *
         * @param writer the {@link BufferedWriter} to write the license information to
         * @throws IOException if an I/O error occurs while writing to the output
         */
        @Override
        public void printModuleHeader(BufferedWriter writer, ModuleData moduleData) throws IOException {
            println(writer, "%d. ########################################################################",
                    dependencyCounter.incrementAndGet());

            String libraryName = libraryName(moduleData);
            println(writer, "Library: %s", libraryName);
            println(writer, "Group: %s, Name: %s", moduleData.getGroup(), moduleData.getName());
            println(writer, "Version: %s", moduleData.getVersion());

            println(writer);
        }

        /**
         * Renders license information for a module dependency by attempting multiple
         * strategies in sequence.
         * The method follows a prioritized approach to finding and displaying license
         * information:
         * 
         * <ol>
         * <li>First attempts to extract license information from the dependency package
         * itself</li>
         * <li>Then checks for custom license URLs defined in the configuration</li>
         * <li>Next tries to retrieve license information from GitHub repositories</li>
         * <li>Finally attempts to use license URLs declared in POM files</li>
         * </ol>
         *
         * If all strategies fail, the method throws a {@link RuntimeException}
         * indicating that no license information could be found for the dependency.
         * 
         * @param writer     the {@link BufferedWriter} to write the license
         *                   information to
         * @param moduleData the {@link ModuleData} object containing dependency
         *                   information
         * @throws IOException      if an I/O error occurs while writing to the output
         * @throws RuntimeException if no license information can be found for the
         *                          dependency
         */
        @Override
        public void printDependencyLicenses(BufferedWriter writer, ModuleData moduleData)
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
            String libraryName = libraryName(moduleData);
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
                    Path path = Paths.get(embeddedLicensesDir, fileDetail.getFile());

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
         * @throws IOException if an I/O error occurs while writing to the output
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
         * Determines if a URL points to a GitHub repository.
         * 
         * @param projectUrl the URL to check
         * @return {@code true} if the URL is from GitHub (starts with http://github.com
         *         or https://github.com), {@code false} otherwise
         */
        private static boolean isFromGitHub(String projectUrl) {
            return projectUrl.startsWith("https://github.com") || projectUrl.startsWith("http://github.com");
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
         * @throws IOException      if an I/O error occurs while writing to the output
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
    }

    /**
     * A renderer implementation that outputs dependency license information in CSV
     * format.
     * This renderer creates a semicolon-separated CSV file containing detailed
     * information about project dependencies and their associated licenses.
     * <p>
     * The generated CSV includes the following columns:
     * <ul>
     * <li>Component Name - The name of the library/component</li>
     * <li>Artifact Name - The formal artifact identifier</li>
     * <li>Description - Brief description of the dependency</li>
     * <li>Version Currently Integrated - The version being used</li>
     * <li>Link - URL to the project</li>
     * <li>Change Log - Change log information (defaults to "-")</li>
     * <li>License Type - Type of license used by the dependency</li>
     * <li>License Link - URL to the license text</li>
     * </ul>
     */
    class CsvRenderer implements InternalRenderer {

        /**
         * The project data containing information needed for license rendering.
         */
        private final ProjectData projectData;

        CsvRenderer(ProjectData projectData) {
            this.projectData = projectData;
        }

        @Override
        public ProjectData getProjectData() {
            return projectData;
        }

        /**
         * Writes the CSV header line to the specified writer.
         * The header includes columns for component information, version details, and
         * license data.
         *
         * @param writer the {@link BufferedWriter} to write the output to
         * @throws IOException if an I/O error occurs while writing to the output
         */
        @Override
        public void printHeader(BufferedWriter writer) throws IOException {
            println(writer,
                    "Component Name;Artifact Name;Description;Version Currently Integrated;Link;Change Log;License Type;License Link");
        }

        /**
         * Writes license information for a dependency module to the specified writer in
         * a CSV format.
         * 
         * This method extracts metadata from the specified module, including library
         * name, artifact name, description, version, project URL, license type, and
         * license URL.
         * 
         * @param writer     the {@link BufferedWriter} to write the license
         *                   information to
         * @param moduleData the {@link ModuleData} object containing dependency
         *                   information
         * @throws IOException if an I/O error occurs while writing to the output
         */
        @Override
        public void printDependencyLicenses(BufferedWriter writer, ModuleData moduleData) throws IOException {
            String libraryName = libraryName(moduleData);
            String artifactName = moduleData.getName();
            String description = description(moduleData);
            String version = moduleData.getVersion();
            String projectLink = projectUrl(moduleData);
            String licenseType = moduleData.getPoms().stream()
                    .flatMap(pom -> pom.getLicenses().stream()).findFirst()
                    .map(License::getName)
                    .orElse("N/A");
            String licenseLink = moduleData.getPoms().stream()
                    .flatMap(pom -> pom.getLicenses().stream()).findFirst()
                    .map(License::getUrl)
                    .orElse("N/A");
            println(writer, "%s;%s;%s;%s;%s;%s;%s",
                    libraryName, artifactName, description, version, projectLink, licenseType, licenseLink);
        }

    }
}
