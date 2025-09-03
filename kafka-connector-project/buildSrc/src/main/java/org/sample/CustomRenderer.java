package org.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private static record License(
            String header,
            String content) {
    }

    private final String fileName;

    private final Map<String, License> licenseCache = new HashMap<>();
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
        this.projectData = projectData;
        try {
            this.licenseNames = new Properties();
            this.licenseNames
                    .load(new FileInputStream(new File(projectData.getProject().getParent().getProjectDir(),
                            "config/license-config/" + licenseNamesFile)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load license names", e);
        }
        LicenseReportExtension licenseReport = projectData.getProject().getExtensions()
                .getByType(LicenseReportExtension.class);
        this.absoluteOutputDir = licenseReport.getAbsoluteOutputDir();
        try (PrintStream stream = new PrintStream(new File(absoluteOutputDir, fileName))) {
            printDependencies(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void printDependencies(PrintStream stream) throws Exception {
        Set<ModuleData> dependencies = projectData.getAllDependencies();
        for (ModuleData moduleData : dependencies) {
            printDependency(stream, moduleData);
        }
    }

    private void printDependency(PrintStream stream, ModuleData moduleData) throws Exception {
        // Print the Header of the dependency
        println(stream, "%d. ########################################################################",
                dependencyCounter.incrementAndGet());
        String projectName = projectName(moduleData);
        println(stream, "Library: %s", projectName);
        println(stream, "Version: %s", moduleData.getVersion());
        println(stream);

        List<License> licenses = projectLicenses(stream, projectName, moduleData);
        if (licenses.isEmpty()) {
            throw new RuntimeException("No license information available!");
        }
        for (License licenseInfo : licenses) {
            String header = licenseInfo.header();
            println(stream, "** %s", header);
            println(stream, '-', header.length() + 3);
            println(stream, licenseInfo.content());
            println(stream);
            println(stream);
        }
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

    private String projectName(ModuleData moduleData) {
        String name = moduleData.getPoms().stream()
                .map(PomData::getName)
                .filter(n -> n != null && !n.isBlank())
                .findFirst()
                .orElseGet(() -> moduleData.getManifests().stream()
                        .map(ManifestData::getName)
                        .filter(n -> n != null && !n.isBlank())
                        .findFirst()
                        .orElse("N/A"));
        return name;
    }

    private String projectDescription(Set<PomData> poms, Set<ManifestData> manifests) {
        String description = poms.stream()
                .map(PomData::getDescription)
                .filter(d -> d != null && !d.isBlank())
                .findFirst()
                .orElseGet(() -> manifests.stream()
                        .map(ManifestData::getDescription)
                        .filter(d -> d != null && !d.isBlank())
                        .findFirst()
                        .orElse("N/A"));
        return description;
    }

    private String projectUrl(ModuleData moduleData) {
        Set<PomData> poms = moduleData.getPoms();
        Set<ManifestData> manifests = moduleData.getManifests();
        String url = poms.stream()
                .map(PomData::getProjectUrl)
                .filter(u -> u != null && !u.isBlank())
                .findFirst()
                .orElseGet(() -> manifests.stream()
                        .map(ManifestData::getUrl)
                        .filter(u -> u != null && !u.isBlank())
                        .findFirst()
                        .orElse("N/A"));
        return url;
    }

    private List<License> projectLicenses(PrintStream stream, String projectName, ModuleData moduleData)
            throws Exception {
        /*
         * Step 1: First attempt - Check for license files in the package itself.
         * This is the most reliable source as the license is directly bundled
         * with the dependency.
         */
        List<License> licenses = retrieveFromPackage(moduleData);
        if (!licenses.isEmpty()) {
            return licenses;
        }

        /*
         * Step 2: Second attempt - Check for custom license repository URLs in
         * configuration.
         * This allows manual override for dependencies with non-standard license
         * locations
         * or when other methods fail to retrieve the license.
         */
        String property = licenseNames.getProperty(projectName);
        if (property != null) {
            return retrieveFromCustomUrl(property);
        }

        /*
         * Step 3: Third attempt - Check GitHub repository if the project is hosted
         * there.
         * Many open source projects host their code and license files on GitHub,
         * so we try to fetch license files from standard locations in the repository.
         */
        String projectUrl = projectUrl(moduleData);
        if (isFromGitHub(projectUrl)) {
            licenses = retrieveFromGitHub(projectUrl);
            if (!licenses.isEmpty()) {
                return licenses;
            }
        }

        /*
         * Step 4: Final attempt - Use license URLs declared in POM/Manifest files.
         * If all else fails, try to retrieve the license from URLs specified
         * in the dependency's metadata files. This is the least preferred method
         * as these URLs may sometimes be incorrect or unavailable.
         */
        return retrieveFromLicenseUrl(moduleData);
    }

    /**
     * Retrieves license information for a given module by extracting license URLs
     * from its POM/Manifest files.
     * <p>
     *
     * @param moduleData The module data containing POM/Manifest files with license
     *                   information
     * @return A list of {@link License} objects representing the licenses used
     *         by the module
     * @throws RuntimeException If a license cannot be found at the specified URL or
     *                          if an error
     *                          occurs during the retrieval process
     */
    private List<License> retrieveFromLicenseUrl(ModuleData moduleData) {
        return moduleData.getPoms().stream()
                .flatMap(pom -> pom.getLicenses().stream())
                .filter(license -> license.getUrl() != null && !license.getUrl().isBlank())
                .map(license -> licenseCache.computeIfAbsent(license.getUrl(), url -> {
                    System.out.println("Feeding cache for " + url);
                    try {
                        return retrieveFromUrl(url, url)
                                .orElseThrow(() -> new RuntimeException("License not found"));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })).toList();
    }

    private List<License> retrieveFromPackage(ModuleData moduleData) throws IOException {
        List<License> licenseInfos = new ArrayList<>();
        Set<LicenseFileData> licenses = moduleData.getLicenseFiles();
        for (LicenseFileData licenseFileData : licenses) {
            Collection<LicenseFileDetails> fileDetails = licenseFileData.getFileDetails();
            for (LicenseFileDetails fileDetail : fileDetails) {
                Path path = Paths.get(this.absoluteOutputDir, fileDetail.getFile());
                String simpleFileName = path.toFile().getName();
                if (simpleFileName.toUpperCase().startsWith("LICENSE")
                        || simpleFileName.toUpperCase().startsWith("NOTICE")) {
                    List<String> lines = Files.readAllLines(path);
                    String content = lines.stream().collect(Collectors.joining("\n"));
                    String header = "from the " + simpleFileName + " file included in the distribution";
                    licenseInfos.add(new License(header, content));
                }
            }
        }
        return Collections.unmodifiableList(licenseInfos);
    }

    private List<License> retrieveFromGitHub(String projectUrl)
            throws URISyntaxException, IOException, InterruptedException {
        List<License> licenseInfos = new ArrayList<>();
        String baseRawProjectUrl = projectUrl.replace("github.com", "raw.githubusercontent.com") + "/refs/heads";

        for (String licenseFileName : GITHUB_LICENSE_FILE_NAMES) {
            for (String branch : GITHUB_BRANCH_NAMES) {
                String licenseUrl = String.format("%s/%s/%s", baseRawProjectUrl, branch, licenseFileName);
                String gitHubResourceUrl = projectUrl + "/blob/" + branch + "/" + licenseFileName;
                retrieveFromUrl(licenseUrl, gitHubResourceUrl).ifPresent(licenseInfos::add);
            }
        }
        return Collections.unmodifiableList(licenseInfos);
    }

    private List<License> retrieveFromCustomUrl(String customUrls)
            throws URISyntaxException, IOException, InterruptedException {
        String gitHubResourceUrl = customUrls.split(",")[0];
        String customRepoUrl = customUrls.split(",")[1];
        License licenseInfo = retrieveFromUrl(customRepoUrl, gitHubResourceUrl)
                .orElseThrow(
                        () -> new RuntimeException("License not found from custom repository URL: " + customRepoUrl));
        return List.of(licenseInfo);
    }

    private Optional<License> retrieveFromUrl(String licenseUrl, String gitHubResourceUrl)
            throws URISyntaxException, IOException, InterruptedException {
        HttpRequest request = HttpRequest
                .newBuilder(new URI(licenseUrl))
                .build();
        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            return Optional.of(
                    new License("from the license contents available at " + gitHubResourceUrl, response.body()));
        }
        return Optional.empty();
    }

    private static boolean isFromGitHub(String projectUrl) {
        return projectUrl.startsWith("https://github.com") || projectUrl.startsWith("http://github.com");
    }

}
