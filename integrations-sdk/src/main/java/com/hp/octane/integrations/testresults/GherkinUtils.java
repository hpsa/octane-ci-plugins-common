package com.hp.octane.integrations.testresults;

import com.hp.octane.integrations.dto.DTOFactory;
import com.hp.octane.integrations.dto.tests.TestRunResult;
import com.hp.octane.integrations.utils.SdkConstants;
import org.apache.poi.util.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class GherkinUtils {

    public static void aggregateGherkinFilesToMqmResultFile(Collection<File> gherkinFiles, File mqmFile, String jobId, String buildId) throws Exception {
        List<XmlWritableTestResult> result = parseFiles(gherkinFiles);
        writeXmlFile(mqmFile, jobId, buildId, result);
    }

    public static List<XmlWritableTestResult> parseFiles(Collection<File> gherkinFiles) throws ParserConfigurationException, SAXException, IOException {
        List<XmlWritableTestResult> result = new ArrayList<>();
        for (File file : gherkinFiles) {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(file);

            doc.getDocumentElement().normalize();

            validateXMLVersion(doc);

            //Go over the features
            NodeList featureNodes = doc.getElementsByTagName("feature");
            for (int f = 0; f < featureNodes.getLength(); f++) {
                Element featureElement = (Element) featureNodes.item(f);
                FeatureInfo featureInfo = new FeatureInfo(featureElement);
                result.add(new GherkinXmlWritableTestResult(featureInfo.getName(), featureElement, featureInfo.getDuration(), featureInfo.getStatus()));
            }

        }
        return result;
    }

    /**
     * Find all files according to template
     * @param folder folder to search
     * @param fileTemplate for example GherkinResults_%s
     * @param counterStart     initial file index
     * @return list of found files
     */

    public static List<File> findGherkinFilesByTemplateWithCounter(String folder, String fileTemplate, int counterStart) {
        List<File> result = new ArrayList<>();
        int i = counterStart;
        while (true) {
            File file = new File(folder, String.format(fileTemplate, i++));
            if (file.exists()) {
                result.add(file);
            } else {
                break;
            }
        }

        return result;
    }


    private static void validateXMLVersion(Document doc) {
        String XML_VERSION = "1";
        NodeList featuresNodes = doc.getElementsByTagName("features");
        if (featuresNodes.getLength() > 0) {
            String versionAttr = ((Element) featuresNodes.item(0)).getAttribute("version");
            if (versionAttr == null || versionAttr.isEmpty() || versionAttr.compareTo(XML_VERSION) != 0) {
                throw new IllegalArgumentException("\n********************************************************\n" +
                        "Incompatible xml version received from the Octane formatter.\n" +
                        "expected version = " + XML_VERSION + " actual version = " + versionAttr + ".\n" +
                        "You may need to update the octane formatter version to the correct version in order to work with this plugin\n" +
                        "********************************************************");
            }
        } else {
            throw new IllegalArgumentException("The file does not contain Octane Gherkin results. Configuration error?");
        }
    }

    private static class FeatureInfo {
        private String name;
        private List<String> scenarioNames = new ArrayList<>();
        private TestRunResult status = TestRunResult.PASSED;
        private boolean statusDetermined = false;
        private long duration = 0;

        public FeatureInfo(Element featureElement) {
            name = featureElement.getAttribute("name");
            NodeList backgroundNodes = featureElement.getElementsByTagName("background");
            Element backgroundElement = backgroundNodes.getLength() > 0 ? (Element) backgroundNodes.item(0) : null;
            NodeList backgroundSteps = backgroundElement != null ? backgroundElement.getElementsByTagName("step") : null;

            //Go over the scenarios
            NodeList scenarioNodes = featureElement.getElementsByTagName("scenario");
            for (int s = 0; s < scenarioNodes.getLength(); s++) {
                Element scenarioElement = (Element) scenarioNodes.item(s);
                ScenarioInfo scenarioInfo = new ScenarioInfo(scenarioElement, backgroundSteps);
                String scenarioName = scenarioInfo.getName();
                scenarioNames.add(scenarioName);

                duration += scenarioInfo.getDuration();
                if (!statusDetermined && TestRunResult.SKIPPED.equals(scenarioInfo.getStatus())) {
                    status = TestRunResult.SKIPPED;
                    statusDetermined = true;
                } else if (!statusDetermined && TestRunResult.FAILED.equals(scenarioInfo.getStatus())) {
                    status = TestRunResult.FAILED;
                    statusDetermined = true;
                }
            }
        }

        public String getName() {
            return name;
        }

        public TestRunResult getStatus() {
            return status;
        }

        public long getDuration() {
            return duration;
        }

        private static class ScenarioInfo {
            private List<String> stepNames = new ArrayList<String>();
            private long duration = 0;
            private TestRunResult status = TestRunResult.PASSED;
            private boolean statusDetermined = false;
            private String name;

            public ScenarioInfo(Element scenarioElement, NodeList backgroundSteps) {
                name = getScenarioName(scenarioElement);

                List<Element> stepElements = getStepElements(backgroundSteps, scenarioElement);
                for (Element stepElement : stepElements) {
                    addStep(stepElement);
                }

                scenarioElement.setAttribute("status", status.value());

                //for surefire report
                stepNames.add(name);
                stepNames.add("Scenario: " + name);
            }

            public long getDuration() {
                return duration;
            }

            public TestRunResult getStatus() {
                return status;
            }

            public String getName() {
                return name;
            }

            private void addStep(Element stepElement) {
                String stepName = stepElement.getAttribute("name");
                stepNames.add(stepName);

                String durationStr = stepElement.getAttribute("duration");
                long stepDuration = durationStr != "" ? Long.parseLong(durationStr) : 0;
                duration += stepDuration;

                String stepStatus = stepElement.getAttribute("status");
                if (!statusDetermined && ("pending".equals(stepStatus) || "skipped".equals(stepStatus))) {
                    status = TestRunResult.SKIPPED;
                    statusDetermined = true;
                } else if (!statusDetermined && "failed".equals(stepStatus)) {
                    status = TestRunResult.FAILED;
                    statusDetermined = true;
                }
            }

            private List<Element> getStepElements(NodeList backgroundSteps, Element scenarioElement) {
                List<Element> stepElements = new ArrayList<Element>();
                if (backgroundSteps != null) {
                    for (int bs = 0; bs < backgroundSteps.getLength(); bs++) {
                        Element stepElement = (Element) backgroundSteps.item(bs);
                        stepElements.add(stepElement);
                    }
                }
                NodeList stepNodes = scenarioElement.getElementsByTagName("step");
                for (int sn = 0; sn < stepNodes.getLength(); sn++) {
                    Element stepElement = (Element) stepNodes.item(sn);
                    stepElements.add(stepElement);
                }

                return stepElements;
            }

            private String getScenarioName(Element scenarioElement) {
                String scenarioName = scenarioElement.getAttribute("name");
                if (scenarioElement.hasAttribute("outlineIndex")) {
                    String outlineIndexStr = scenarioElement.getAttribute("outlineIndex");
                    if (outlineIndexStr != null && !outlineIndexStr.isEmpty()) {
                        Integer outlineIndex = Integer.valueOf(scenarioElement.getAttribute("outlineIndex"));
                        if (outlineIndex > 1) {
                            //we add the index only from 2 and upwards seeing as that is the naming convention in junit xml.
                            String delimiter = " ";
                            if (!scenarioName.contains(" ")) {
                                //we need to use the same logic as used in the junit report
                                delimiter = "_";
                            }
                            scenarioName = scenarioName + delimiter + scenarioElement.getAttribute("outlineIndex");
                        }
                    }
                }
                return scenarioName;
            }
        }
    }

    private static void writeXmlFile(File mqmFile, String planName, String buildNumber, List<XmlWritableTestResult> gherkinXmlWritableTestResults) throws IOException, XMLStreamException {
        FileOutputStream outputStream = new FileOutputStream(mqmFile);
        try {
            XMLStreamWriter writer = DTOFactory.getInstance().getXMLMapper().getFactory().getXMLOutputFactory().createXMLStreamWriter(outputStream, "UTF-8");
            if (!gherkinXmlWritableTestResults.isEmpty()) {
                writer.writeStartDocument("UTF-8", "1.0");
                writer.writeStartElement("test_result");
                writer.writeStartElement("build");
                writer.writeAttribute("server_id", SdkConstants.General.INSTANCE_ID_TO_BE_SET_IN_SDK);
                writer.writeAttribute("job_id", planName);
                writer.writeAttribute("build_id", buildNumber);
                writer.writeEndElement(); // build
                //  writeFields(resultFields);
                writer.writeStartElement("test_runs");

                for (XmlWritableTestResult g : gherkinXmlWritableTestResults) {
                    g.writeXmlElement(writer);
                }
                writer.writeEndElement(); // test_runs
                writer.writeEndElement(); // test_result
                writer.writeEndDocument();
            }
            writer.flush();
            writer.close();
        } finally {
            IOUtils.closeQuietly(outputStream);
        }

    }
}
