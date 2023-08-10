package tables;

import data.PredicatesData;
import db.IDBConnector;
import dto.Group;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GroupTable extends AbsTable{
  public GroupTable (IDBConnector idbConnector) {super ( "group_s", idbConnector);}
  public List<Group> getGroups(String[] columns, Map<PredicatesData, List<String>> predicates) throws SQLException {
    ResultSet resultSet = getData(columns, predicates, null, null);
    List<Group> groups = new ArrayList<>();
    while(resultSet.next()) {
      Group group = new Group(
          resultSet.getInt("id"),
          resultSet.getString("name"),
          resultSet.getInt("id_curator")
      );
      groups.add(group);
    }
    return groups;
  }
}