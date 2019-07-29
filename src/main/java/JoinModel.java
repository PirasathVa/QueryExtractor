import java.util.ArrayList;

public class JoinModel {

    private String joinType;

    private String table;

    private String alias ;

    private String parent;

    private String joinClause;

    private ArrayList<String> joinDependencies ;


    public JoinModel(String joinType, String table, String alias, String joinClause, String parent) {
        this.joinType = joinType;
        this.table = table;
        this.alias = alias;
        this.parent = parent;
        this.joinClause = joinClause;
        this.joinDependencies = new ArrayList<>();
    }


    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }
    public ArrayList<String> getJoinDependencies() {
        return joinDependencies;
    }

    public void setJoinDependencies(ArrayList<String> joinDependencies) {
        this.joinDependencies = joinDependencies;
    }

    public String getJoinType() {
        return joinType;
    }

    public String getTable() {
        return table;
    }

    public String getAlias() {
        return alias;
    }

    public String getJoinClause() {
        return joinClause;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setJoinClause(String joinClause) {
        this.joinClause = joinClause;
    }

}
