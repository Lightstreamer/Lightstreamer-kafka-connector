package org.sample;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

    private final String fileName;

    private final Map<String, LicenseData> licenseCache = new HashMap<>();
    private final AtomicInteger dependencyCounter = new AtomicInteger();
    private final HttpClient client = HttpClient
            .newBuilder()
            .followRedirects(Redirect.NORMAL)
            .build();
    private final String licenseNamesFile;

    private ProjectData projectData;
    private Properties licenseNames;
    private String absoluteOutputDir;

    public CustomRenderer() {
        this("THIRD-PARTY-LIBRARIES.txt", "license-names.properties");
    }

    public CustomRenderer(String fileName, String licenseNamesFile) {
        this.fileName = fileName;
        this.licenseNamesFile = licenseNamesFile;
    }

    public void render(ProjectData projectData) {
        try {
            doRendering(projectData);
        } catch (IOException e) {
            throw new RuntimeException("Failed to render license report", e);
        }
    }

    /**
     * Performs rendering of license information for project dependencies.
     * 
     * @param projectData the project data containing information about dependencies
     *                    and licenses
     * @throws Exception if an error occurs during file operations (reading
     *                   license names or writing output)
     */
    private void doRendering(ProjectData projectData) throws IOException {
        this.projectData = projectData;
        this.licenseNames = new Properties();
        Project project = projectData.getProject();
        try (FileInputStream fis = new FileInputStream(new File(project.getParent().getProjectDir(),
                "config/license-config/" + licenseNamesFile))) {
            this.licenseNames.load(fis);
        }

        LicenseReportExtension licenseReport = project.getExtensions()
                .getByType(LicenseReportExtension.class);
        this.absoluteOutputDir = licenseReport.getAbsoluteOutputDir();
        try (PrintStream stream = new PrintStream(new File(absoluteOutputDir, fileName))) {
            for (ModuleData moduleData : projectData.getAllDependencies()) {
                printDependencyLicenses(stream, moduleData);
            }
        }
    }

    private void printDependencyLicenses(PrintStream stream, ModuleData moduleData) {
        // Print the Header of the dependency
        println(stream, "%d. ########################################################################",
                dependencyCounter.incrementAndGet());
        String libraryName = libraryName(moduleData);
        println(stream, "Library: %s", libraryName);
        println(stream, "Version: %s", moduleData.getVersion());
        println(stream);

        // Print the associated licenses of the dependency
        printLicenses(stream, libraryName, moduleData);
    }

    private void printLicense(PrintStream stream, LicenseData license) {
        String header = license.header();
        println(stream, "** %s", header);
        println(stream, '-', header.length() + 3);
        println(stream, license.content());
        println(stream);
        println(stream);
    }

    private void println(PrintStream stream, String msg, Object... args) {
        stream.println(msg.formatted(args));
    }

    private void println(PrintStream stream, String msg) {
        stream.println(msg);
    }

    private void println(PrintStream stream, char c, int times) {
        stream.println(String.valueOf(c).repeat(Math.max(0, times)));
    }

    private void println(PrintStream stream) {
        stream.println();
    }

    private String libraryName(ModuleData moduleData) {
        return moduleData.getPoms().stream()
                .map(PomData::getName)
                .filter(n -> n != null && !n.isBlank())
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                        "No project name found for module " + moduleData.getName() + " in POM files!"));
    }

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
     * Retrieves and prints license information for a dependency using a prioritized
     * multi-step approach.
     * 
     * This method attempts to find license text in the following order:
     * 1. Directly from files within the dependency package itself
     * 2. From custom license URLs specified in configuration
     * 3. From standard locations in the project's GitHub repository
     * 4. From license URLs declared in POM metadata files
     * 
     * The process stops at the first successful license retrieval. If all methods
     * fail,
     * a {@link RuntimeException} is thrown.
     * 
     * @param stream      the {@link PrintStream} to which license text should be
     *                    written
     * @param libraryName the name of the library/dependency
     * @param moduleData  metadata about the module containing license information
     *                    and artifact details
     * @throws RuntimeException If no license information can be found through any
     *                          method
     */
    private void printLicenses(PrintStream stream, String libraryName, ModuleData moduleData) {
        /*
         * Step 1: First attempt - Check for license files in the package itself.
         * This is the most reliable source as the license is directly bundled
         * with the dependency.
         */
        if (printFromPackage(stream, moduleData)) {
            return;
        }

        /*
         * Step 2: Second attempt - Check for custom license repository URLs in
         * configuration.
         * This allows manual override for dependencies with non-standard license
         * locations.
         */
        String customLicenseUrl = licenseNames.getProperty(libraryName);
        if (customLicenseUrl != null) {
            printFromCustomUrl(stream, customLicenseUrl);
            return;
        }

        /*
         * Step 3: Third attempt - Check GitHub repository if the project is hosted
         * there.
         * Many open source projects host their code and license files on GitHub,
         * so we try to fetch license files from standard locations in the repository.
         */
        if (printFromGitHub(stream, moduleData)) {
            return;
        }

        /*
         * Step 4: Final attempt - Use license URLs declared in POM files.
         * If all else fails, try to retrieve the license from URLs specified
         * in the dependency's metadata files. This is the least preferred method
         * as these URLs may sometimes be incorrect or unavailable.
         */
        if (printFromPom(stream, moduleData)) {
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
     * @param stream     the output stream to which license content will be printed
     * @param moduleData the module data containing license information to process
     * @return {@code true} if at least one license was printed, {@code false}
     *         otherwise
     * @throws RuntimeException if an error occurs while reading a license file
     */
    private boolean printFromPackage(PrintStream stream, ModuleData moduleData) {
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
                        printLicense(stream, new LicenseData(header, content));
                        licensePrinted = true;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to read embedded license file: " + path, e);
                    }
                }
            }
        }
        return licensePrinted;
    }

    private boolean printFromCustomUrl(PrintStream stream, String githubBlobUrl) {
        String rawLicenseTryUrl = String.format("%s?raw=true", githubBlobUrl);
        Optional<LicenseData> license = retrieveFromUrl(rawLicenseTryUrl, githubBlobUrl);
        if (!license.isEmpty()) {
            printLicense(stream, license.get());
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
     * @param stream     the {@link PrintStream} to output license information
     * @param moduleData the module data containing project URL information
     * @return {@code true} if a license file was found and printed; {@code false}
     *         otherwise
     */
    private boolean printFromGitHub(PrintStream stream, ModuleData moduleData) {
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
     * Retrieves license information for a given module by extracting license URLs
     * from its POM files and prints them.
     *
     * @param stream     the {@link PrintStream} to output license information
     * @param moduleData the module data containing POM files with license
     *                   information
     * @return {@code true} if any licenses were found and printed, {@code false}
     *         otherwise
     * @throws RuntimeException if a license cannot be found at the specified URL or
     *                          if an error occurs during the retrieval process.
     *                          This method may throw unchecked exceptions if
     *                          license retrieval fails.
     */
    /**
     * Retrieves license information from POM metadata and processes it.
     * <p>
     * This method extracts license information from the provided module data,
     * retrieves the actual license content from the specified URLs, and prints
     * the license information to the output stream. It caches license data to
     * avoid redundant retrievals.
     *
     * @param stream     the print stream to which license information will be
     *                   output
     * @param moduleData the module data containing POM information with licenses
     * @return {@code true} if at least one license was found and processed;
     *         {@code false} otherwise
     * @throws RuntimeException if a license URL in the POM metadata is invalid or
     *                          cannot be accessed
     */
    private boolean printFromPom(PrintStream stream, ModuleData moduleData) {
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
            printLicense(stream, licenseData);
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

