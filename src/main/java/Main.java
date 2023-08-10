import data.AggregationData;
import data.PredicatesData;
import db.DBConnector;
import db.IDBConnector;
import dto.Group;
import dto.Student;
import tables.CuratorTable;
import tables.GroupTable;
import tables.StudentTable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Main {

  public static void main(String... args) {
    IDBConnector idbConnector = new DBConnector();
    try {
      String[] columns = new String[0];

      StudentTable studentTable = new StudentTable(idbConnector);

      List<String> columnsStudentTable = new ArrayList<>();
      columnsStudentTable.add("id int primary key");
      columnsStudentTable.add("fio varchar (50)");
      columnsStudentTable.add("sex varchar (1)");
      columnsStudentTable.add("id_group int");

      studentTable.create(columnsStudentTable);

      CuratorTable curatorTable = new CuratorTable(idbConnector);

      List<String> columnsCuratorTable = new ArrayList<>();
      columnsCuratorTable.add("id int primary key");
      columnsCuratorTable.add("fio varchar (50)");

      curatorTable.create(columnsCuratorTable);

      GroupTable groupTable = new GroupTable(idbConnector);

      List<String> columnsGroupTable = new ArrayList<>();
      columnsGroupTable.add("id int primary key");
      columnsGroupTable.add("name varchar (10)");
      columnsGroupTable.add("id_curator int");

      groupTable.create(columnsGroupTable);

      studentTable.insertData();

      System.out.println("Задание 1. Вывести на экран информацию о всех студентах включая название группы и имя куратора:");

      Map<String, List<String>> joinsTaskOne = new LinkedHashMap<>();
      List<String> joinPredicates2 = new ArrayList<>();
      joinPredicates2.add("student.id_group = group_s.id");
      List<String> joinPredicates3 = new ArrayList<>();
      joinPredicates3.add("curator.id = group_s.id_curator");

      joinsTaskOne.put("join group_s", joinPredicates2);
      joinsTaskOne.put("join curator", joinPredicates3);

      String[] columnsJoinsTaskOne = new String[6];
      columnsJoinsTaskOne[0] = "student.id";
      columnsJoinsTaskOne[1] = "student.fio";
      columnsJoinsTaskOne[2] = "student.sex";
      columnsJoinsTaskOne[3] = "student.id_group";
      columnsJoinsTaskOne[4] = "group_s.name";
      columnsJoinsTaskOne[5] = "curator.fio";

      ResultSet resultJoinTaskOne = studentTable.getData(columnsJoinsTaskOne, null, null, joinsTaskOne);
      System.out.println("Результат:");
      while (resultJoinTaskOne.next()) {
        System.out.println(columnsJoinsTaskOne[0] + " = " + resultJoinTaskOne.getString(1) + ", " + columnsJoinsTaskOne[1] + " = " + resultJoinTaskOne.getString(2) + ", " +
            columnsJoinsTaskOne[2] + " = " + resultJoinTaskOne.getString(3) + ", " + columnsJoinsTaskOne[3] + " = " + resultJoinTaskOne.getString(4) + ", " +
            columnsJoinsTaskOne[4] + " = " + resultJoinTaskOne.getString(5) + ", " + columnsJoinsTaskOne[5] + " = " + resultJoinTaskOne.getString(6));
      }

      System.out.println("Задание 2. Вывести на экран количество студентов:");
      AggregationData aggregate = AggregationData.COUNT;

      ResultSet result = studentTable.getData(columns, null, aggregate, null);
      result.next();
      System.out.println("Результат:");
      System.out.println(result.getInt(1));

      System.out.println("Задание 3. Вывести студенток:");
      Map<PredicatesData, List<String>> predicates = new HashMap<>();
      List<String> predicatesValues = new ArrayList<>();
      predicatesValues.add("sex=\"F\"");

      predicates.put(PredicatesData.AND, predicatesValues);
      List<Student> studentsTaskThree = studentTable.getStudents(columns, predicates);
      System.out.println("Результат:");
      for (Student student : studentsTaskThree) {
        System.out.println("id = " + student.getId() + ", " + "fio = " + student.getFIO() + ", " + "sex = " + student.getSex() + ", " + "groupID = " + student.getIdGroup());
      }
      System.out.println("Задание 4. Обновить данные по группе сменив куратора:");
      System.out.println("а) таблица до обновления данных:");
      List<Group> groups = groupTable.getGroups(columns, null);
      System.out.println("Результат:");
      for (Group group : groups) {
        System.out.println("id = " + group.getId() + ", " + "name = " + group.getName() + ", " + "idCurator = " + group.getIdCurator());
      }
      System.out.println("б) обновление данных:");
      List<String> updateValues = new ArrayList<>();
      updateValues.add("id_curator=\"4\"");

      Map<PredicatesData, List<String>> predicatesForUpdate = new HashMap<>();
      List<String> predicatesValuesForUpdate = new ArrayList<>();
      predicatesValuesForUpdate.add("id=\"3\"");
      predicatesForUpdate.put(PredicatesData.AND, predicatesValuesForUpdate);

      groupTable.updateData(updateValues, predicatesForUpdate);
      System.out.println("в) таблица после обновления данных:");
      groups = groupTable.getGroups(columns, null);
      System.out.println("Результат:");
      for (Group group : groups) {
        System.out.println("id = " + group.getId() + ", " + "name = " + group.getName() + ", " + "idCurator = " + group.getIdCurator());
      }

      System.out.println("Задание 5. Вывести список групп с их кураторами:");
      Map<String, List<String>> joinsTaskFive = new HashMap<>();
      List<String> joinPredicates = new ArrayList<>();
      joinPredicates.add("curator.id = group_s.id_curator");

      joinsTaskFive.put("join curator", joinPredicates);

      String[] columnsJoinsTaskTwo = new String[2];
      columnsJoinsTaskTwo[0] = "group_s.name";
      columnsJoinsTaskTwo[1] = "curator.fio";

      ResultSet resultJoinTaskFive = groupTable.getData(columnsJoinsTaskTwo, null, null, joinsTaskFive);
      System.out.println("Результат:");
      while (resultJoinTaskFive.next()) {
        System.out.println(columnsJoinsTaskTwo[0] + " = " + resultJoinTaskFive.getString(1) + ", " + columnsJoinsTaskTwo[1] + " = " + resultJoinTaskFive.getString(2));
      }

      System.out.println("Задание 6. Используя вложенные запросы вывести на экран студентов из определенной группы(поиск по имени группы):");
      StudentTable studentTable2 = new StudentTable(idbConnector);
      studentTable2.tableName = "(select * from student where id_group = 2) x";
      List<Student> studentsTaskSix = studentTable2.getStudents(columns, null);
      System.out.println("Результат:");
      for (Student student : studentsTaskSix) {
        System.out.println("id = " + student.getId() + ", " + "fio = " + student.getFIO() + ", " + "sex = " + student.getSex() + ", " + "groupID = " + student.getIdGroup());
      }

    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      idbConnector.executeRequest("drop table student");
      idbConnector.executeRequest("drop table curator");
      idbConnector.executeRequest("drop table group_s");
      idbConnector.close();
    }

  }
}