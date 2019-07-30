import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {

    public static void main(String[] args){

        String[] base                       = new String[2];
        String baseAlias                    = "";
        String baseTable                    = "";

        //Select
        Map<String,String> selectTerms      = new HashMap<>();

        //Joins
        ArrayList<JoinModel> listOfTheJoins = new ArrayList<>();
        Map<String,JoinModel> dependenciesMap  = new HashMap<>();

        //Where
        ArrayList<String> whereCaluse       = new ArrayList<>();

        //Query
        String path  = "/Users/pirasath.vallipuram/Desktop/QUERY.sql";

        try {

            base = getSelectValues(path,selectTerms,base);
            baseAlias = base[1];
            baseTable = base[0];
            evaluateAllJoins(path,listOfTheJoins,whereCaluse,dependenciesMap);
            evaluateAllDependencies(listOfTheJoins,baseAlias,dependenciesMap);

            FileUtility.createFlyMigrationScript(listOfTheJoins,dependenciesMap,selectTerms,baseAlias,whereCaluse,baseTable);

            //System.out.println(selectTerms.size());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private static void evaluateAllDependencies(ArrayList<JoinModel> listOfTheJoins, String baseTable, Map<String, JoinModel> dependenciesMap) {

        for(int i =0; i<listOfTheJoins.size(); i++){

            JoinModel value = listOfTheJoins.get(i);
            String parent   = value.getParent();
            String table    = baseTable.trim();

            while(!parent.equals(table)){
                JoinModel joinModel = dependenciesMap.get(parent);
                value.getJoinDependencies().add(joinModel.getAlias());
                parent = joinModel.getParent();
            }
        }
    }

    private static void evaluateAllJoins(String path, ArrayList<JoinModel> listOfTheJoins, ArrayList<String> whereCaluse, Map<String, JoinModel> dependenciesMap) throws IOException {

        Pattern getJoinPattern    = Pattern.compile(".*JOIN(.*\\n)*.*$");

        String getJoinText        = getFileContent(path, getJoinPattern);
        String replaceCharacter   = getJoinText.replace("`", "\"");
        String replaceSingleQuote = replaceCharacter.replaceAll("'", "\"");


        String replaceLeftJoin    = replaceSingleQuote.replaceAll("LEFT JOIN", "LEFT OUTER JOIN");
        String replaceRightJoin   = replaceLeftJoin.replaceAll("RIGHT JOIN", "RIGHT OUTER JOIN");
        String replaceJoin        = replaceRightJoin.replaceAll("(?<!INNER\\s)(?<!RIGHT OUTER\\s)(?<!LEFT OUTER\\s)JOIN", "INNER JOIN");
        String removeNewLine      = replaceJoin.replaceAll("\\n"," ");
        String removeWhiteSpace   = removeNewLine.replaceAll("\\s\\s+"," ");
        String newlineWhere       = removeWhiteSpace.replaceAll("(\\bWHERE\\b)(?!.*\\1)", "\nWHERE");
        String newlineInnerJoin   = newlineWhere.replaceAll("INNER JOIN", "\nINNER JOIN");
        String newlineLeftJoin    = newlineInnerJoin.replaceAll("LEFT OUTER JOIN", "\nLEFT OUTER JOIN");
        String newlineRightJoin   = newlineLeftJoin.replaceAll("RIGHT OUTER JOIN", "\nRIGHT OUTER JOIN");

        String[] joinValues = Arrays.stream(newlineRightJoin.split("\n")).map(String::trim).toArray(String[]::new);

        boolean appendJoinBracket = false;
        long openBracket = 0;
        long closedBracket = 0;
        String joinWithSelect = "";
        int initialIndex = 0;

        for(int i=0; i<joinValues.length; i++){

            if(appendJoinBracket){
                openBracket += joinValues[i].chars().filter(num -> num == '(').count();
                closedBracket += joinValues[i].chars().filter(num -> num == ')').count();

                joinWithSelect += " " + joinValues[i];
                joinValues[i] = "";

                if(closedBracket == openBracket){
                    appendJoinBracket = false;
                    joinValues[initialIndex] = joinWithSelect;
                }
            }

            if((joinValues[i].contains("JOIN (") | joinValues[i].contains("JOIN(")) && !appendJoinBracket){
                appendJoinBracket = true;
                openBracket = joinValues[i].chars().filter(num -> num == '(').count();
                closedBracket = joinValues[i].chars().filter(num -> num == ')').count();

                joinWithSelect = joinValues[i];
                initialIndex = i;

                if(closedBracket == openBracket){
                    appendJoinBracket = false;
                }
            }

        }

        joinValues = Arrays.stream(joinValues).filter(x -> !x.isEmpty()).toArray(String[]::new);


        for(int i =0; i<joinValues.length; i++){

            if(joinValues[i].length() > 1 ) {

                String[] joinArray = joinValues[i].trim().split("JOIN\\s*\\(");
                if (joinArray.length == 1) {
                    joinArray = joinValues[i].trim().split("JOIN");
                }

                if (joinArray.length >= 2) {

                    String[] conditionArray = joinArray[1].trim().split("(\\bON\\b)(?!.*\\1)");
                    if(conditionArray.length == 1){
                        conditionArray = joinArray[1].trim().split("\\b(ON|on)\\b");
                    }

                    String[] tableArray = null;

                    if (conditionArray[0].contains("SELECT")) {
                        tableArray = conditionArray[0].trim().split("(\\sAS\\s)(?!.*\\1)");
                        tableArray[0] = "(" + tableArray[0];
                    } else {
                        tableArray = conditionArray[0].trim().split("\\s");
                    }

                    String joinType = joinArray[0] + "JOIN";
                    String condition = conditionArray[1];
                    String table = tableArray[0];
                    String alias = tableArray[1];


                    String parent = "";
                    JoinModel newEntry = null;
                        String[] parentArray =  condition.trim().split(" = ");

                        for (String value : parentArray) {
                            int index = value.indexOf('.');
                            if( index != -1) {
                                parent = value.substring(0, index).trim();
                            }
                            if (!alias.equals(parent)) {
                                break;
                            }
                        }
                    newEntry = new JoinModel(joinType, table, alias, condition, parent);
                    listOfTheJoins.add(newEntry);
                    dependenciesMap.put(alias, newEntry);

                    //System.out.println(joinType + " , " + condition + " , " + table + " , " + alias);
                }else{
                    //where clause
                    whereCaluse.add(joinValues[i]);
                    //System.out.println(joinValues[i]);
                }
            }
        }
    }

    private static String getFileContent(String path, Pattern getJoinPattern) throws IOException {
        List<String> content = Files.readAllLines(Paths.get(path));

        String contentOfFile = "";
        for (String line : content) {
            contentOfFile += line + "\n";
        }

        Matcher getJoin = getJoinPattern.matcher(contentOfFile.toUpperCase());
        String getJoinText = "";
        while (getJoin.find()) {
            getJoinText = getJoin.group();
        }
        return getJoinText;
    }

    private static String[] getSelectValues(String path, Map<String, String> selectTerms, String[] base) throws IOException {

        Pattern selectPattern   = Pattern.compile("SELECT(.*(\\n))*FROM(\\s)(.*)");

        String getSelectText    = getFileContent(path, selectPattern);

        String replaceCharacter = getSelectText.replace("`", "\"");
        String newLineSelect    = replaceCharacter.replaceAll("^SELECT.*\n","");
        String removeNewLine    = newLineSelect.replace("\n"," ");
        String removeSpace      = removeNewLine.replaceAll("\\s\\s+", " ");
        String parseableSelect  = removeSpace.replaceAll("\\\",","\",\n");
        String[] selectNotClean = Arrays.stream(parseableSelect.split("\n")).map(String::trim).toArray(String[]::new);
        List<String> myList     = new ArrayList( Arrays.asList(selectNotClean) );

        for(String select : myList){

            if(select.contains(" FROM ") && !select.contains("SELECT")){
                myList.remove(select);
                String[] values = select.split(" FROM ");
                if(values.length == 2){
                    myList.add(values[0]);
                    myList.add("FROM " + values[1]);
                    break;
                }
            }
        }

        String[] selectValues = new String[myList.size()];
        selectValues = myList.toArray(selectValues);

        int i = 0;
        String[] keyValuePair = new String[2];
        String[] baseTableAlias = null;
        for (String key : selectValues) {

            keyValuePair = key.split("(\\b(AS)\\b)(?!.*\\1)| (AS)");
            if (keyValuePair.length > 1) {
                selectTerms.put(keyValuePair[1].replaceAll("\"", "").replaceAll(",", ""), keyValuePair[0]);
            }

            if (!key.contains("SELECT")) {
                keyValuePair = key.split("(FROM)\\s");
                if (keyValuePair.length > 1) {
                    if (keyValuePair[1].contains(" AS ")) {
                        baseTableAlias = keyValuePair[1].split("(\\sAS\\s)");
                    } else {
                        baseTableAlias = keyValuePair[1].split("((\\s))");
                    }
                    selectTerms.put(baseTableAlias[1].replaceAll("\"", "").replaceAll(",", ""), baseTableAlias[0]);
                }
            }
        }
    return baseTableAlias;
    }
}