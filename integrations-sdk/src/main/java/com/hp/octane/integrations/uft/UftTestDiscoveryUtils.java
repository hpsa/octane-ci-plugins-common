/*
 *     Copyright 2017 EntIT Software LLC, a Micro Focus company, L.P.
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package com.hp.octane.integrations.uft;

import com.hp.octane.integrations.uft.items.*;
import com.hp.octane.integrations.utils.SdkConstants;
import com.hp.octane.integrations.utils.SdkStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.poifs.filesystem.*;
import org.apache.poi.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UftTestDiscoveryUtils {
    private static final Logger logger = LogManager.getLogger(UftTestDiscoveryUtils.class);

    private static final String STFileExtention = ".st";//api test
    private static final String API_ACTIONS_FILE = "actions.xml";//api test

    private static final String QTPFileExtention = ".tsp";//gui test
    private static final String XLSXExtention = ".xlsx";//excel file
    private static final String XLSExtention = ".xls";//excel file

    private static final Set<String> SKIP_FOLDERS = Stream.of("_discovery_results").collect(Collectors.toSet());

    public static UftTestDiscoveryResult doFullDiscovery(File root) {
        UftTestDiscoveryResult result = new UftTestDiscoveryResult();
        scanFileSystemRecursively(root, root, result);
        result.setFullScan(true);
        return result;
    }

    private static void scanFileSystemRecursively(File root, File dirPath, UftTestDiscoveryResult discoveryResult) {
        if (dirPath.isDirectory() && SKIP_FOLDERS.contains(dirPath.getName())) {
            return;
        }

        File[] paths = dirPath.isDirectory() ? dirPath.listFiles() : new File[]{dirPath};

        //if it test folder - create new test, else drill down to subFolders
        UftTestType testType = paths != null ? isUftTestFolder(paths) : UftTestType.None;
        if (!testType.isNone()) {
            AutomatedTest test = createAutomatedTest(root, dirPath, testType);
            discoveryResult.getAllTests().add(test);
        } else if (paths != null) {
            for (File path : paths) {
                if (path.isDirectory()) {
                    scanFileSystemRecursively(root, path, discoveryResult);
                } else if (isUftDataTableFile(path.getName())) {
                    ScmResourceFile dataTable = createDataTable(root, path);
                    discoveryResult.getAllScmResourceFiles().add(dataTable);
                }
            }
        }
    }

    public static ScmResourceFile createDataTable(File root, File path) {
        ScmResourceFile resourceFile = new ScmResourceFile();
        resourceFile.setName(path.getName());
        resourceFile.setRelativePath(getRelativePath(root, path));
        resourceFile.setOctaneStatus(OctaneStatus.NEW);

        return resourceFile;

    }

    public static boolean isUftDataTableFile(String path) {
        String loweredPath = path.toLowerCase();
        return loweredPath.endsWith(XLSXExtention) || loweredPath.endsWith(XLSExtention);
    }

    public static UftTestType isUftTestFolder(File[] paths) {
        for (File path : paths) {
            if (path.getName().endsWith(STFileExtention)) {
                return UftTestType.API;
            }
            if (path.getName().endsWith(QTPFileExtention)) {
                return UftTestType.GUI;
            }
        }

        return UftTestType.None;
    }

    public static boolean isTestMainFilePath(String path) {
        return !getUftTestType(path).isNone();
    }

    public static UftTestType getUftTestType(String testMainFilePath) {
        String lowerPath = testMainFilePath.toLowerCase();
        if (lowerPath.endsWith(QTPFileExtention)) {
            return UftTestType.GUI;
        }

        if (lowerPath.endsWith(STFileExtention) || lowerPath.endsWith(API_ACTIONS_FILE)) {
            return UftTestType.API;
        }

        return UftTestType.None;
    }

    public static File getTestFolderForTestMainFile(String path) {
        if (isTestMainFilePath(path)) {
            File file = new File(path);
            File parent = file.getParentFile();
            return parent;
        }
        return null;
    }

    public static AutomatedTest createAutomatedTest(File root, File dirPath, UftTestType testType) {
        AutomatedTest test = new AutomatedTest();
        test.setName(dirPath.getName());

        String relativePath = getRelativePath(root, dirPath);
        String packageName = relativePath.length() != dirPath.getName().length() ? relativePath.substring(0, relativePath.length() - dirPath.getName().length() - 1) : "";
        test.setPackage(packageName);
        test.setExecutable(true);
        test.setUftTestType(testType);
        String description = getTestDescription(dirPath, testType);
        description = convertToHtmlFormatIfRequired(description);
        test.setDescription(description);
        test.setOctaneStatus(OctaneStatus.NEW);


        return test;
    }

    public static String convertToHtmlFormatIfRequired(String description) {
        if (description == null || !description.contains("\n")) {
            return description;
        }
        //aaa\nbbb = >   <html><body><p>aaa</p><p>bbb</p></body></html>
        String[] lines = description.split("\n");
        StringBuilder sb = new StringBuilder(description.length() + lines.length * 10 + 30);
        sb.append("<html><body>");
        for (String line : lines) {
            sb.append("<p>");
            sb.append(line);
            sb.append("</p>");
            sb.append("\n");
        }
        sb.append("</body></html>");
        return sb.toString();
    }

    private static String getRelativePath(File root, File path) {
        String testPath = path.getPath();
        String rootPath = root.getPath();
        String relativePath = testPath.replace(rootPath, "");
        relativePath = SdkStringUtils.strip(relativePath, SdkConstants.FileSystem.WINDOWS_PATH_SPLITTER + SdkConstants.FileSystem.LINUX_PATH_SPLITTER);
        //we want all paths will be in windows style, because tests are run in windows, therefore we replace all linux splitters (/) by windows one (\)
        //http://stackoverflow.com/questions/23869613/how-to-replace-one-or-more-in-string-with-just
        relativePath = relativePath.replaceAll(SdkConstants.FileSystem.LINUX_PATH_SPLITTER, SdkConstants.FileSystem.WINDOWS_PATH_SPLITTER + SdkConstants.FileSystem.WINDOWS_PATH_SPLITTER);//str.replaceAll("/", "\\\\");
        return relativePath;
    }

    /**
     * Extract test description from UFT GUI test.
     * Note : UFT API test doesn't contain description
     *
     * @param dirPath  path of UFT test
     * @param testType GUI or API
     * @return test description
     */
    public static String getTestDescription(File dirPath, UftTestType testType) {
        if (!dirPath.exists() || testType.isNone()) {
            return null;
        }

        try {
            String description;
            if (UftTestType.GUI.equals(testType)) {
                description = getTestDescriptionFromGuiTest(dirPath);
            } else {
                description = getTestDescriptionFromAPITest(dirPath);
            }
            if (description != null) {
                description = description.trim();
            }
            return description;
        } catch (IOException | ParserConfigurationException | SAXException e) {
            return null;
        }
    }

    private static String getTestDescriptionFromAPITest(File dirPath) throws ParserConfigurationException, IOException, SAXException {

        //Actions.xml
        //<Actions>
        //<Action internalName="MainAction" userDefinedName="APITest1" description="radi end test description" />
        //</Actions>

        File actionsFile = new File(dirPath, "Actions.xml");
        if (!actionsFile.exists()) {
            return null;
        }

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(actionsFile);

        NodeList actions = doc.getElementsByTagName("Action");
        for (int temp = 0; temp < actions.getLength(); temp++) {
            Node nNode = actions.item(temp);
            NamedNodeMap attributes = nNode.getAttributes();
            Node internalNameAttr = attributes.getNamedItem("internalName");
            if (internalNameAttr != null && "MainAction".equals(internalNameAttr.getNodeValue())) {
                return attributes.getNamedItem("description").getNodeValue();
            }
        }
        return null;
    }

    private static String getTestDescriptionFromGuiTest(File dirPath) throws IOException, ParserConfigurationException, SAXException {

        File tspTestFile = new File(dirPath, "Test.tsp");
        if (!tspTestFile.exists()) {
            return null;
        }

        InputStream is = new FileInputStream(tspTestFile);
        String xmlContent = extractXmlContentFromTspFile(is);

        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(new InputSource(new StringReader(xmlContent)));
        String desc = document.getElementsByTagName("Description").item(0).getTextContent();
        return desc;
    }


    private static String extractXmlContentFromTspFile(InputStream stream) throws IOException {
        POIFSFileSystem poiFS = new POIFSFileSystem(stream);
        DirectoryNode root = poiFS.getRoot();
        String xmlData = "";

        for (Entry entry : root) {
            String name = entry.getName();
            if ("ComponentInfo".equals(name)) {
                if (entry instanceof DirectoryEntry) {
                    System.out.println(entry);
                } else if (entry instanceof DocumentEntry) {
                    byte[] content = new byte[((DocumentEntry) entry).getSize()];
                    int readBytes = poiFS.createDocumentInputStream("ComponentInfo").read(content);
                    if (readBytes < content.length) {
                        //  [YG] probably should handle this case and continue to read
                        logger.warn("expected to read " + content.length + " bytes, but read and stopped after " + readBytes);
                    }
                    String fromUnicodeLE = StringUtil.getFromUnicodeLE(content);
                    xmlData = fromUnicodeLE.substring(fromUnicodeLE.indexOf('<')).replaceAll("\u0000", "");
                }
            }
        }
        return xmlData;
    }
}
