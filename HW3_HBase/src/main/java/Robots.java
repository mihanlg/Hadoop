import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Robots {
    public class BadRobotsFormatException extends Exception {}

    private List<String> disallowed = new ArrayList<>();

    public Robots() {}

    public Robots(String robots) throws BadRobotsFormatException {
        add(robots);
    }


    public void add(String robots) throws BadRobotsFormatException {
        if (robots.length() == 0)
            return;

        for (String line : robots.split("\n")) {
            if (line.matches("Disallow: .*")) {
                if (line.contains("info:robots")) {
                    disallowed.clear();
                    return;
                }
                //Skip "Disallow: "
                String rule = line.substring(10);
                disallowed.add(createRule(rule));
            } else {
                throw new BadRobotsFormatException();
            }
        }
    }

    public boolean isDisallowed(String fullURL) {
        String local = extractLocalPath(fullURL);
        for (String rule: disallowed) if (local.matches(rule)) return true;
        return false;
    }

    private static String extractLocalPath(String fullURL) {
        try {
            URL url = new URL(fullURL);
            StringBuilder local = new StringBuilder();
            String path = url.getPath();
            String query = url.getQuery();
            String ref = url.getRef();
            if (path != null) local.append(path);
            if (query != null) local.append("?").append(query);
            if (ref != null) local.append("#").append(ref);
            return local.length() > 0 ? local.toString() : fullURL;
        } catch (MalformedURLException error) {
            return fullURL;
        }
    }

    private String createRule(String rule) throws BadRobotsFormatException {
        if (!rule.startsWith("/") && !rule.startsWith("*")) {
            throw new BadRobotsFormatException();
        }
        if (!rule.endsWith("$")) {
            rule = rule + "*$";
        }
        rule = Pattern.compile("([(){}.?|^+])").matcher(rule).replaceAll("\\\\$0");
        rule = Pattern.compile("\\*").matcher(rule).replaceAll("(.*)");
        return rule;
    }
}
