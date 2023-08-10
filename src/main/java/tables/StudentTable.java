package tables;

import data.PredicatesData;
import db.IDBConnector;
import dto.Student;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StudentTable extends AbsTable{
  public StudentTable(IDBConnector idbConnector) {super ( "student", idbConnector);}

  public List<Student> getStudents(String[] columns, Map<PredicatesData, List<String>> predicates) throws SQLException {
    ResultSet resultSet = getData(columns, predicates, null, null);
    List<Student> students = new ArrayList<>();
    while(resultSet.next()) {
      Student student = new Student(
          resultSet.getInt("id"),
          resultSet.getString("fio"),
          resultSet.getString("sex"),
          resultSet.getInt("id_group")
      );
      students.add(student);
    }
    return students;
  }
}