package tables;

import data.AggregationData;
import data.PredicatesData;
import db.IDBConnector;
import dto.Curator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CuratorTable extends AbsTable{
  public CuratorTable (IDBConnector idbConnector) {super ( "curator", idbConnector);}
  public List<Curator> getCurators(String[] columns, Map<PredicatesData, List<String>> predicates) throws SQLException {
    ResultSet resultSet = getData(columns, predicates, null, null);
    List<Curator> curators = new ArrayList<>();
    while(resultSet.next()) {
      Curator curator = new Curator(
          resultSet.getInt("id"),
          resultSet.getString("fio")
      );
      curators.add(curator);
    }
    return curators;
  }
}