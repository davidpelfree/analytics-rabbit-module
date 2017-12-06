package demo.support;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.xmlpull.v1.XmlPullParser.*;

public class XpathBasedParser {

    private static PrintStream outFilePrinter;
    private static final Map<String, Class<? extends TypeParser>> xpathSplitters = new HashMap();
    private static TypeParser currentTypeParser = null;
    private static ByteArrayOutputStream outBuffer;
    private static Consumer<byte[]> processor; // TODO not thread safe! Fix.

    public static void parseFromSystemIn(Consumer<byte[]> processor) throws Exception {
        XpathBasedParser.processor = processor;

        xpathSplitters.put("/items/item", ItemParser.class);

        XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
        factory.setNamespaceAware(true);
        XmlPullParser xpp = factory.newPullParser();
        xpp.setInput(System.in, null);
        process(xpp, "", xpp.getEventType(), xpathSplitters);

    }

    public static void process(XmlPullParser xpp, String containerXpath, int eventType, Map<String, Class<? extends TypeParser>> xpathSplitters) throws XmlPullParserException, IOException {
        loop:
        while (true) {
            switch (eventType) {
                case START_TAG:
                    final String name = xpp.getName();
                    final String xpath = containerXpath + "/" + name;
                    final boolean hasAttributes = xpp.getAttributeCount() > 0;
                    final boolean isEmpty = xpp.isEmptyElementTag();
                    startTag(xpath, hasAttributes, isEmpty);
                    printAttributes(xpath, xpp, isEmpty);
                    process(xpp, xpath, xpp.next(), xpathSplitters);
                    break;
                case END_TAG:
                    endTag(containerXpath);
                case END_DOCUMENT:
                    break loop;
                case TEXT:
                    final String text = xpp.getText().trim().replace('\n', ' ').replace('\r', ' ').replace('\t', ' ');
                    if (text.length() > 0) {
                        text(containerXpath, text);
                    }
                    break;
            }
            eventType = xpp.next();
        }
    }

    private static void printAttributes(String xpath, XmlPullParser xpp, boolean isTagEmpty) {
        int attributeCount = xpp.getAttributeCount();
        if (attributeCount > 0) {
            boolean isFirst = true;
            for (int i = 0; i < attributeCount; i++) {
                final boolean isLast = i == attributeCount - 1;
                final String key = xpp.getAttributeName(i);
                final String value = xpp.getAttributeValue(i);
                attribute(xpath, key, value, isFirst, isLast, isTagEmpty);
                isFirst = false;
            }
        }
    }

    // =================================================================================================
    // XPath based events
    // =================================================================================================

    private static void startTag(String xpath, boolean hasAttributes, boolean isEmpty) {
        if (xpathSplitters.containsKey(xpath)) {
            startType(xpath, xpathSplitters.get(xpath));
        }
        if (currentTypeParser != null) currentTypeParser.startTag(xpath, hasAttributes, isEmpty);
    }

    private static void attribute(String xpath, String key, String value, boolean isFirst, boolean isLast, boolean isTagEmpty) {
        if (currentTypeParser != null) currentTypeParser.attribute(xpath, key, value, isFirst, isLast, isTagEmpty);
    }

    private static void text(String xpath, String text) {
        if (currentTypeParser != null) currentTypeParser.text(xpath, text);
    }

    private static void endTag(String xpath) {
        if (currentTypeParser != null) {
            final boolean completed = currentTypeParser.endTag(xpath);
            if (completed && currentTypeParser == null) endType(xpath);
        }
    }

    private static void startType(String xpath, Class<? extends TypeParser> typeParserClass) {
        if (typeParserClass == null) {
            currentTypeParser = null;
        } else {
            try {
                currentTypeParser = typeParserClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            currentTypeParser.setBaseXpath(xpath);
        }

    }

    private static void endType(String xpath) {
        outFilePrinter.println();
    }

    // =================================================================================================
    // Type parser
    // =================================================================================================


    interface TypeParser {
        void setBaseXpath(String xpath);

        void startTag(String xpath, boolean hasAttributes, boolean isEmpty);

        void attribute(String xpath, String key, String value, boolean isFirst, boolean isLast, boolean isTagEmpty);

        void text(String xpath, String text);

        /**
         * @return true if type parsing is completed
         */
        boolean endTag(String xpath);
    }

    // =================================================================================================

    static final class ItemParser implements TypeParser {
        private String baseXpath;
        private ByteArrayOutputStream startTagStream = new ByteArrayOutputStream();
        private PrintStream startTagBuffer = new PrintStream(startTagStream);
        private boolean isStartTagEmpty = false;

        @Override
        public void setBaseXpath(String xpath) {
            baseXpath = xpath;
        }

        @Override
        public void startTag(String xpath, boolean hasAttributes, boolean isEmpty) {
            final String elementName = xpath.substring(xpath.lastIndexOf("/") + 1);
            indent(xpath, startTagBuffer);
            startTagBuffer.print("<" + elementName);
            if (!hasAttributes) {
                isStartTagEmpty = isEmpty;
                closeTag(xpath, isEmpty);
            }
        }

        @Override
        public void attribute(String xpath, String attrKey, String attrValue, boolean isFirst, boolean isLast, boolean isTagEmpty) {
            startTagBuffer.print(" ");
            startTagBuffer.print(attrKey);
            startTagBuffer.print("=\"");
            startTagBuffer.print(attrValue);
            startTagBuffer.print("\"");
            if (isLast) {
                isStartTagEmpty = isTagEmpty;
                closeTag(xpath, isTagEmpty);
            }
        }

        private void startNewType() {
            outBuffer = new ByteArrayOutputStream();
            outFilePrinter = new PrintStream(outBuffer);
        }

        private void closeTag(String xpath, boolean isTagEmpty) {
            final boolean isRoot = xpath.equals(baseXpath);
            if (isRoot) {
                startNewType();
            }

            if (isTagEmpty) startTagBuffer.print("/");
            startTagBuffer.println(">");
            outFilePrinter.print(new String(startTagStream.toByteArray()));
            startTagStream = new ByteArrayOutputStream();
            startTagBuffer = new PrintStream(startTagStream);
        }

        @Override
        public void text(String xpath, String text) {
            indent(xpath, outFilePrinter);
            outFilePrinter.println(text);
        }

        private void indent(String xpath, PrintStream printer) {
            final int indentationLevel = xpath.substring(baseXpath.length()).split("/").length - 1;
            for (int i = 0; i < indentationLevel; i++) {
                printer.print("    ");
            }
        }

        @Override
        public boolean endTag(String xpath) {
            if (!isStartTagEmpty) {
                final String elementName = xpath.substring(xpath.lastIndexOf("/") + 1);
                indent(xpath, outFilePrinter);
                outFilePrinter.println("</" + elementName + ">");
            }
            isStartTagEmpty = false;
            if (xpath.equals(baseXpath)) {
                currentTypeParser = null;
                completeType();
                return true;
            }
            return false;
        }

        private void completeType() {
            if (outFilePrinter != null) {
                outFilePrinter.close();
                final byte[] content = outBuffer.toByteArray();
                processor.accept(content);
            }
        }
    }
}
