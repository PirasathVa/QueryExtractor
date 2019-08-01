import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtility {

    public static void createFlyMigrationScript(ArrayList<JoinModel> joinModelArrayList, Map<String, JoinModel> dependenciesMap, Map<String, String> selectMap, String baseAlias, ArrayList<String> whereCaluse, String baseTable){

       Map<String,String> selectDependencies = new HashMap<>();
       ArrayList<String> paramsList = new ArrayList<>();
        
       String report           = insertIntoCustomReport(baseAlias,whereCaluse,"custom-channel-unused-customer-purchase",paramsList,baseTable);
       String selects          = insertIntoCustomColumnDefinition(selectMap,baseAlias);
       String joins            = insertIntoCustomJoinDefinition(joinModelArrayList);
       String baseJoin         = insertIntoCustomReportJoinBaseTable(baseAlias);
       String dependenciesJoin = insertIntoCustomColumnJoinDefinitionSelects(joinModelArrayList,dependenciesMap,selectMap,selectDependencies,baseAlias);
       String customFilters    = insertIntoCustomFilterDefinitions(paramsList);
       String access           = insertAccess();

       String flywayScript     = report + selects + joins + baseJoin + dependenciesJoin + customFilters + access;
       String pattern          = "(\\b" + baseAlias.toLowerCase() + "\\b)";
       flywayScript            = flywayScript.replaceAll(pattern,baseTable.toLowerCase());
       System.out.println( flywayScript );
    }

    private static String insertAccess() {

        String access = "\nINSERT INTO access (uuid, report_uuid, owner_type, owner_partner, custom_report_definition_id) " +
                        "\nVALUES (UUID(), NULL, 'ROLE_CHANNEL_ADMIN', 'APPDIRECT',@report_number);\n";
        return access;
    }

    private static String insertIntoCustomFilterDefinitions(ArrayList<String> paramsList) {

        String customFilterBegin = "\nINSERT INTO custom_filter_definitions (id, name, custom_report_definition_id, filter, params) \nVALUES\n";
        String line = "";
        String params = "";

        for(int i =0; i < paramsList.size(); i++){

            params = getParameterValues(params, paramsList.get(i));
            String value  =  paramsList.get(i).replaceAll("':(\\w+)'","'':$1''");
            String valueManipulateDate  =  value.replaceAll("((?<!'):\\w+)","''$1''");
            params = removeDuplicateDep(params);
            
            line += "(id,'"+ params +"', @report_number,'"+ valueManipulateDate + "', '" + params + "'),\n";
            params = "";

        }
        line = line.substring(0,line.length()-2);

        return customFilterBegin + line.toLowerCase() +";\n";
    }

    private static String insertIntoCustomReport(String baseAlias, ArrayList<String> whereCaluse, String reportName, ArrayList<String> paramsList, String baseTable) {

        String reportNumberPlaceholder = "SELECT @report_number := id from custom_report_definitions where name = '" + reportName + "';\n";

        String s = whereCaluse.stream().map(Object::toString).collect(Collectors.joining("','"));
        String noWhere = s.replaceAll("WHERE","").trim();
        String noGroupBy = noWhere.replaceAll("GROUP BY.*\\w","").trim();

        String[] whereConditions = noGroupBy.split("AND");
        String filter = "";

        for(int i =0; i<whereConditions.length; i++){
            if(!whereConditions[i].contains(":")){
                filter += whereConditions[i].trim() + " AND ";
            }else{
                paramsList.add(whereConditions[i].trim());
            }
        }

        if(!filter.isEmpty()){
            filter = filter.substring(0,filter.length()-5);
        }

        String filterQuoted  =  filter.toLowerCase().replaceAll("((?!'\\s*')'[a-zA-Z\\s_-]*')","'$1'");

        String insertReport =
                "\nSELECT @report_number := id from custom_report_definitions where name = '"+ reportName + "';\n" +
                "\nINSERT INTO custom_report_definitions (`id`, `name`, `database`, `base_table`, `filter`) \n" +
                "SELECT * FROM ((SELECT ((select max(id) from custom_report_definitions)+1),'" + reportName + "','appdirect','" + baseTable.toLowerCase() + "','" + filterQuoted + "')) AS tmp\n" +
                "WHERE not exists(\n" +
                "   (Select * from custom_report_definitions where id =  @report_number)\n" +
                ");\n" +
                "\nSELECT @report_number := id from custom_report_definitions where name = '"+ reportName + "';\n";



        return insertReport;

    }

    private static String insertIntoCustomColumnJoinDefinitionSelects(ArrayList<JoinModel> joinModelArrayList, Map<String, JoinModel> dependenciesMap, Map<String, String> selectMap, Map<String, String> selectDependencies, String baseTable) {

        String name = "";
        Map<String,ArrayList<String>> dependencies = new HashMap<>();
        Map<String,String> matches = new HashMap<>();

        for(Map.Entry<String,String> entry : selectMap.entrySet()){

            Pattern pattern = Pattern.compile("(\\w*\\.)");
            Matcher matcher = pattern.matcher(entry.getValue());
            while (matcher.find())
            {
                if(!matches.containsKey(matcher.group())) {
                    matches.put(matcher.group().substring(0, matcher.group(1).length() - 1),"");
                }
            }


            List<String> l = new ArrayList<String>(matches.keySet());
            name = l.stream().map(Object::toString).collect(Collectors.joining(","));

            if(selectDependencies.containsKey(name)){
                String value = selectDependencies.get(name);
                value = value + "," + "'" + entry.getKey().trim().replaceAll(" ","_") + "'";
                selectDependencies.replace(name,value);
            }else {
                selectDependencies.put(name, "'" + entry.getKey().trim().replaceAll(" ","_") + "'");
            }

            matches.clear();
        }

        for(Map.Entry<String,String> entry : selectDependencies.entrySet()){

            if(!entry.getKey().isEmpty()) {
                if (dependencies.containsKey(entry.getKey())) {
                    String putQuotes = entry.getValue();
                    if (!dependencies.get(entry.getKey()).contains(putQuotes)) {
                        dependencies.get(entry.getKey()).add(putQuotes);
                    }
                } else {
                    ArrayList o = new ArrayList<>();
                    String putQuotes = entry.getValue();
                    dependencies.put(entry.getKey(), o);
                    dependencies.get(entry.getKey()).add(putQuotes);
                }
            }
        }



        String beginning = "";
        for(Map.Entry<String,ArrayList<String>> entry : dependencies.entrySet()) {

            String v = entry.getValue().stream().map(Object::toString).collect(Collectors.joining("','"));

            String k = "";
            String[] values = entry.getKey().split(",");
            for(String x : values){
                if (dependenciesMap.containsKey(x)) {
                    String dep = dependenciesMap.get(x).getJoinDependencies().stream().map(Object::toString).collect(Collectors.joining(","));
                    if (dep.isEmpty()) {
                        k = entry.getKey();
                    } else {
                        k = dep;
                    }
                }

                String[] appendReportNumber = k.split(",");
                k = "";
                for (int i = 0; i < appendReportNumber.length; i++) {
                    if (!appendReportNumber[i].isEmpty()) {
                        k += "'" + appendReportNumber[i] + "',";
                    }

                    if (i == (appendReportNumber.length - 1)) {
                        if (!k.contains(entry.getKey()) && !entry.getKey().equals(baseTable)) {
                            String test =
                                    Stream.of(entry.getKey().split(","))
                                    .map(s -> "'" + s + "'")
                                    .collect(Collectors.joining(","));
                            k = k + test.toLowerCase();

                            k = removeDuplicateDep(k)+"'";

                        }
                    }
                }
            }

            if(!k.isEmpty()){

            k = k.substring(0,k.length()-1);

                beginning += "\nINSERT INTO custom_column_join_definitions \n(\nselect\n  ccd.id, cjd.id\nfrom\n  custom_join_definitions cjd, custom_column_definitions ccd\nwhere\n  ccd.custom_report_definition_id = @report_number\n  " +
                        "and ccd.name in (" + v.toLowerCase() + ") " +
                        "\n  and cjd.alias in (" + k.toLowerCase() + ")\n);\n";
            }
        }

        return beginning;
    }

    private static String removeDuplicateDep(String duplicates) {


        String[] findUnique = duplicates.toLowerCase().split(",");
        Map<String,String> findUniqueMap = new HashMap<>();

        for(String val : findUnique) {
            findUniqueMap.put(val,"");
        }

        List<String> l = new ArrayList<String>(findUniqueMap.keySet());
        String removedDuplicates = l.stream().map(Object::toString).collect(Collectors.joining(","));

        return removedDuplicates ;
    }

    private static String insertIntoCustomReportJoinBaseTable(String baseTable) {

        //replace report number with parameter
        return "\nINSERT INTO custom_report_join_definitions \n(\n select @report_number, id\n from custom_join_definitions\n where alias in  ('" + " REPLACE WILL VALUES REQUIRED BY WHERE CLAUSE " + "')\n);\n";

    }

    private static String insertIntoCustomJoinDefinition(ArrayList<JoinModel> joinModelArrayList) {

        String beginning = "\nINSERT INTO custom_join_definitions (join_type, alias, `table`, clause, params)\n" +
                "VALUES" ;

        String params = "";
        String tableParams = "";
        int i = 0;
        String content = "";
        for( JoinModel entry : joinModelArrayList) {

            if( i == (joinModelArrayList.size() -1)) {

                params = getParameterValues(params, entry.getJoinClause().trim().toLowerCase());
                tableParams = getParameterValuesForTable(tableParams,entry.getTable().toLowerCase());

                params = getParameter(params, tableParams);

                String value = getTableValue(entry);

                String joinCondition = entry.getJoinClause().trim().toLowerCase().replaceAll("(:\\w+)","''$1''");

                content += "\n( '" + entry.getJoinType() + "', '"+ entry.getAlias().toLowerCase() + "', '" + value + "', '" + joinCondition + "', " + params + ");";
                params = "";
                tableParams="";
            }else{

                params = getParameterValues(params, entry.getJoinClause().trim().toLowerCase());
                tableParams = getParameterValuesForTable(tableParams,entry.getTable().toLowerCase());

                params = getParameter(params, tableParams);

                String value = getTableValue(entry);

                String joinCondition = entry.getJoinClause().trim().toLowerCase().replaceAll("(:\\w+)","''$1''");
                ++i;
                content += "\n( '" + entry.getJoinType() + "', '"+ entry.getAlias().toLowerCase() + "', '" + value + "', '" + joinCondition + "', " + params + "),";
                params = "";
                tableParams="";
            }
        }
        content = beginning + content + "\n";
        return content;
    }

    private static String getTableValue(JoinModel entry) {
        String doubleQuoteWords = entry.getTable().toLowerCase().replaceAll("((?!'\\s*')'[a-zA-Z\\s_-]*')", "'$1'");
        String additionalDoubleQuote = doubleQuoteWords.replaceAll("(((?<!\\w)''(?!\\w))|(' '))", "'$1'");
        String valueTable  =  additionalDoubleQuote.replaceAll("':(\\w+)'","'':$1''");
        String valueManipulateDate  =  valueTable.replaceAll("((?<!'):\\w+)","''$1''");

        return valueManipulateDate;
    }

    private static String getParameter(String params, String tableParams) {
        if (!tableParams.isEmpty() && params.equals("NULL")) {
            params = tableParams;
        } else if (!tableParams.isEmpty() && !params.equals("NULL")) {
            params = tableParams + "," + params.substring(1);
        }
        if(!params.equals("NULL")) {
            params = removeDuplicateDep(params);
        }

        if(!params.equals("NULL")){
            params = "'"+ params +"'";
        }

        return params;
    }

    private static String getParameterValues(String params, String entry) {
        Pattern pattern = Pattern.compile(":(\\w+)");
        Matcher matcher = pattern.matcher(entry);

        while (matcher.find()) {
            String value = matcher.group().toString();
            if(!value.isEmpty()) {
                params += value.substring(1) + ",";
            }
        }

        if(params.isEmpty()){
            params = "NULL";
        }else{
            params =  params.substring(0,params.length()-1);
        }
        return params;
    }

    private static String getParameterValuesForTable(String tableParams, String tableEntry) {
        Pattern pattern = Pattern.compile(":(\\w+)");
        Matcher matcher = pattern.matcher(tableEntry);

        while (matcher.find()) {
            String value = matcher.group().toString();
            if(!value.isEmpty()) {
                tableParams += value.substring(1) + ",";
            }
        }

        if(tableParams.isEmpty()){
            tableParams = "";
        }else{
            tableParams = tableParams.substring(0,tableParams.length()-1);
        }
        return tableParams;
    }



    public static String insertIntoCustomColumnDefinition(Map<String, String> selectList, String baseTable){

        String beginning = "\nINSERT INTO custom_column_definitions (name, custom_report_definition_id, `select`, group_name, default_column_label, default_position, default_order_by, params, field_type)\n" +
                "VALUES" ;

        int i = 0;
        String content = "";
        String params = "";
        for( Map.Entry< String,String> entry : selectList.entrySet()) {

            String doubleQuoteWords = entry.getValue().toLowerCase().replaceAll("((?!'\\s*')'[a-zA-Z\\s_-]*')","'$1'");
            String additionalDoubleQuote = doubleQuoteWords.replaceAll("(((?<!\\w)''(?!\\w))|(' '))","'$1'");

            params = getParameterValues(params, entry.getValue().trim().toLowerCase());

            if(!params.equals("NULL")){
                params = "'" + params + "'";
            }

            if(!entry.getKey().equals(baseTable)) {
                    content += "\n( '" + entry.getKey().trim().replaceAll("\\s", "_").toLowerCase() + "' , " + "@report_number" + " , '" + additionalDoubleQuote + "' , '" + "GROUP_NAME" + "' , '" + entry.getKey().trim().toLowerCase() + "' , " + ++i + ", NULL, " + params +  ", 'STRING'),";
            }
            params = "";
        }
        content = content.substring(0,content.length()-1)+";";
        content = beginning + content + "\n";
        return content;
    }

}
