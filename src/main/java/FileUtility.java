import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileUtility {

    public static void createFlyMigrationScript(ArrayList<JoinModel> joinModelArrayList, Map<String, JoinModel> dependenciesMap, Map<String, String> selectMap, String baseTable, ArrayList<String> whereCaluse){

       Map<String,String> selectDependencies = new HashMap<>();
       ArrayList<String> paramsList = new ArrayList<>();
        
       String report           = insertIntoCustomReport(baseTable,whereCaluse,"REPORT_NAME",paramsList);
       String selects          = insertIntoCustomColumnDefinition(selectMap,baseTable);
       String joins            = insertIntoCustomJoinDefinition(joinModelArrayList);
       String baseJoin         = insertIntoCustomReportJoinBaseTable(baseTable);
       String dependenciesJoin = insertIntoCustomColumnJoinDefinitionSelects(joinModelArrayList,dependenciesMap,selectMap,selectDependencies,baseTable);
       String customFilters    = insertIntoCustomFilterDefinitions(paramsList);


       System.out.println( report + selects + joins + baseJoin + dependenciesJoin + customFilters);
    }

    private static String insertIntoCustomFilterDefinitions(ArrayList<String> paramsList) {

        String customFilterBegin = "\nINSERT INTO custom_filter_definitions (id, name, custom_report_definition_id, filter, params) \nVALUES\n";
        String line = "";

        for(int i =0; i < paramsList.size(); i++){

            line += "(id, name, @report_number,"+ paramsList.get(i) + ", param),\n";

        }
        line = line.substring(0,line.length()-2);

        return customFilterBegin + line.toLowerCase() +";";


    }

    private static String insertIntoCustomReport(String baseTable, ArrayList<String> whereCaluse, String reportName, ArrayList<String> paramsList) {

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

        String insertReport =
                "\nSELECT @report_number := id from custom_report_definitions where name = '"+ reportName + "';\n" +
                "\nINSERT INTO custom_report_definitions (`id`, `name`, `database`, `base_table`, `filter`) \n" +
                "SELECT * FROM ((SELECT ((select max(id) from custom_report_definitions)+1),'" + reportName + "','appdirect','" + baseTable.toLowerCase() + "','" + filter + "')) AS tmp\n" +
                "WHERE not exists(\n" +
                "   (Select * from custom_report_definitions where id =  @report_number)\n" +
                ");\n" +
                "\nSELECT @report_number := id from custom_report_definitions where name = '"+ reportName + "';\n";

        String beginning = "\nINSERT INTO custom_report_definitions (id, name, `database`, base_table, filter) \nVALUES (" +
                "@report_number, 'report_name', 'appdirect', '"+ baseTable.toLowerCase() +"','"+ noGroupBy.toLowerCase() +"');\n";

        return insertReport;

    }

    private static String insertIntoCustomColumnJoinDefinitionSelects(ArrayList<JoinModel> joinModelArrayList, Map<String, JoinModel> dependenciesMap, Map<String, String> selectMap, Map<String, String> selectDependencies, String baseTable) {

        String name = "";
        Map<String,ArrayList<String>> dependencies = new HashMap<>();
        for(Map.Entry<String,String> entry : selectMap.entrySet()){

            Pattern pattern = Pattern.compile("(\\w*\\.)");
            Matcher matcher = pattern.matcher(entry.getValue());
            if (matcher.find())
            {
                name = matcher.group(1).substring(0,matcher.group(1).length()-1);
                selectDependencies.put(entry.getKey(),name);

                if(dependencies.containsKey(name)) {
                    String putQuotes = "'" + entry.getKey().trim().replaceAll(" ","_") + "'";
                    dependencies.get(name).add(putQuotes);
                }else{
                    ArrayList o = new ArrayList<>();
                    String putQuotes = "'" + entry.getKey().trim().replaceAll(" ","_") + "'";
                    dependencies.put(name,o);
                    dependencies.get(name).add(putQuotes);
                }
            }
        }

        String beginning = "";
        for(Map.Entry<String,ArrayList<String>> entry : dependencies.entrySet()) {

            String v = entry.getValue().stream().map(Object::toString).collect(Collectors.joining(","));

            String k = "";
            if(dependenciesMap.containsKey(entry.getKey())){
               String dep = dependenciesMap.get(entry.getKey()).getJoinDependencies().stream().map(Object::toString).collect(Collectors.joining(","));
                if(dep.isEmpty()){
                    k = entry.getKey();
                }else{
                    k = dep;
                }
            }

            String[] appendReportNumber = k.split(",");

            k = "";
            for(int i =0; i<appendReportNumber.length; i++) {
                if (!appendReportNumber[i].isEmpty()) {
                    k += "'" + appendReportNumber[i] + "',";
                }

                if (i == (appendReportNumber.length - 1)) {
                    if (!k.contains("'"+entry.getKey()+"'") && !entry.getKey().equals(baseTable)) {
                        k = k + "'" + entry.getKey().toLowerCase() + "',";
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

    private static String insertIntoCustomReportJoinBaseTable(String baseTable) {

        //replace report number with parameter
        return "\nINSERT INTO custom_report_join_definitions \n(\n select @report_number, id\n from custom_join_definitions\n where alias in  ('" + baseTable.toLowerCase() + "')\n);\n";

    }

    private static String insertIntoCustomJoinDefinition(ArrayList<JoinModel> joinModelArrayList) {

        String beginning = "\nINSERT INTO custom_join_definitions (join_type, alias, `table`, clause, params)\n" +
                "VALUES" ;

        int i = 0;
        String content = "";
        for( JoinModel entry : joinModelArrayList) {

            if( i == (joinModelArrayList.size() -1)) {
                content += "\n( '" + entry.getJoinType() + "', '"+ entry.getAlias().toLowerCase() + "', '" + entry.getTable().toLowerCase() + "', '" + entry.getJoinClause().trim().toLowerCase() + "', NULL);";
            }else{
                ++i;
                content += "\n( '" + entry.getJoinType() + "', '"+ entry.getAlias().toLowerCase() + "', '" + entry.getTable().toLowerCase() + "', '" + entry.getJoinClause().trim().toLowerCase() + "', NULL),";
            }
        }
        content = beginning + content + "\n";
        return content;
    }


    public static String insertIntoCustomColumnDefinition(Map<String, String> selectList, String baseTable){

        String beginning = "\nINSERT INTO custom_column_definitions (name, custom_report_definition_id, `select`, group_name, default_column_label, default_position, default_order_by, params, field_type)\n" +
                "VALUES" ;

        int i = 0;
        String content = "";
        for( Map.Entry< String,String> entry : selectList.entrySet()) {

            if(!entry.getKey().equals(baseTable)) {
                    content += "\n( '" + entry.getKey().trim().replaceAll("\\s", "_").toLowerCase() + "' , " + "@report_number" + " , '" + entry.getValue().toLowerCase() + "' , '" + "GROUP_NAME" + "' , '" + entry.getKey().trim().toLowerCase() + "' , " + ++i + ", NULL, NULL" + ", 'STRING'),";
            }
        }
        content = content.substring(0,content.length()-1)+";";
        content = beginning + content + "\n";
        return content;
    }
    
    
    
    
    
}
